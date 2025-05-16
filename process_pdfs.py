#!/usr/bin/env python3
import os
import re
import shutil
import tempfile
import snowflake.connector
import pdfplumber
import pandas as pd
from io import BytesIO
from tqdm import tqdm
from snowflake.connector.pandas_tools import write_pandas

def extract_course_info_from_bytes(file_bytes, filename):
    """
    Extract course information from PDF file bytes.
    Parses metadata from the filename and extracts text from the PDF.
    Also extracts a RateMyProfessors ID (rmp_id) if present, e.g. a pattern like "RMP12345".
    """
    filename = filename.strip()
    
    # Extract professor name using $$...$$ delimiters
    prof_m = re.search(r'\$\$(.*?)\$\$', filename)
    professor = prof_m.group(1) if prof_m else "Unknown"

    # Extract semester (e.g. SP2025 or FL2025)
    sem_m = re.search(r'^(SP|FL)(\d{4})', filename)
    semester = f"{sem_m.group(1)} {sem_m.group(2)}" if sem_m else "Unknown"

    # Extract course code and section (e.g. patterns for L24 or E81)
    crs_m = re.search(r'(L24|E81)[._\s]+(\w+)[._\s]+(\d+)', filename)
    course_code, section = (crs_m.group(2), crs_m.group(3)) if crs_m else ("Unknown", "Unknown")
    
    # Extract rmp_id by looking for a pattern like "RMP12345" (allowing optional dash/underscore)
    rmp_m = re.search(r'RMP[-_]?(\d+)', filename, re.IGNORECASE)
    rmp_id = int(rmp_m.group(1)) if rmp_m else None

    # Extract text from the PDF using pdfplumber
    text = ""
    pdf_stream = BytesIO(file_bytes)
    try:
        with pdfplumber.open(pdf_stream) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + " "
    except Exception as e:
        print(f"Error extracting text from {filename}: {e}")
        text = ""

    # Extract the course name from the PDF text (for example, using a pattern like "Reports for ...- course_name (")
    name_m = re.search(r'Reports for .*?- (.*?)\(', text)
    course_name = name_m.group(1).strip() if name_m else "Unknown"

    # Extract overall rating (e.g. "Overall Rating: 5.00")
    rate_m = re.search(r'Overall Rating:\s*(\d+\.\d+)', text)
    overall_rating = float(rate_m.group(1)) if rate_m else None

    return {
        "filename": filename,
        "professor": professor,
        "semester": semester,
        "course_code": course_code,
        "section": section,
        "course_name": course_name,
        "overall_rating": overall_rating,
        "rmp_id": rmp_id
    }

def clean_and_store_data(df):
    """
    Clean the DataFrame: convert overall_rating to numeric and fill missing values.
    """
    df = df.copy()
    df['overall_rating'] = pd.to_numeric(df['overall_rating'], errors='coerce')
    df['semester'] = df['semester'].fillna('Unknown')
    df['professor'] = df['professor'].fillna('Unknown')
    df['course_code'] = df['course_code'].fillna('Unknown')
    df['section'] = df['section'].fillna('Unknown')
    df['course_name'] = df['course_name'].fillna('Unknown')
    return df

def fix_columns_for_snowflake(df):
    """
    Rename DataFrame columns to exactly match the intended Snowflake table.
    Mapping:
      filename        -> FILENAME
      professor       -> PROFESSOR
      semester        -> SEMESTER
      course_code     -> COURSE_CODE
      section         -> SECTION
      course_name     -> COURSE_NAME
      overall_rating  -> "Overall Rating"   (mixed case with a space)
      rmp_id          -> RMP_ID
    """
    mapping = {
        "filename": "FILENAME",
        "professor": "PROFESSOR",
        "semester": "SEMESTER",
        "course_code": "COURSE_CODE",
        "section": "SECTION",
        "course_name": "COURSE_NAME",
        "overall_rating": "Overall Rating",
        "rmp_id": "RMP_ID"
    }
    return df.rename(columns=mapping)

def ensure_processed_pdfs_table_exists(cursor):
    """
    Create (if not exists) the table in Snowflake that stores the enriched PDF data.
    The table now includes the RMP_ID column.
    """
    create_table_sql = """
    CREATE OR REPLACE TABLE AIRFLOW.APP_PROCESSED_PDFS (
        FILENAME STRING,
        PROFESSOR STRING,
        SEMESTER STRING,
        COURSE_CODE STRING,
        SECTION STRING,
        COURSE_NAME STRING,
        "Overall Rating" FLOAT,
        RMP_ID INTEGER
    )
    """
    cursor.execute(create_table_sql)

def store_results_in_snowflake(cursor, df):
    """
    Upload the cleaned DataFrame (with all columns) to Snowflake using write_pandas.
    """
    success, nchunks, nrows, _ = write_pandas(
        conn=cursor.connection,
        df=df,
        table_name='APP_PROCESSED_PDFS',
        schema='AIRFLOW',
        database='BISON_DB'
    )
    if success:
        print(f"Successfully wrote {nrows} rows in {nchunks} chunks to Snowflake.")
    else:
        print("Failed to write data to Snowflake.")

def process_pdfs_from_snowflake(stage_name="APP_STG"):
    """
    Connects to Snowflake, lists files in the specified stage, downloads and processes each PDF,
    cleans the resulting DataFrame, uploads it to Snowflake, and also writes a CSV for downstream tasks.
    """
    conn = snowflake.connector.connect(
        account="SFEDU02-IOB55870",
        user="BISON",
        password="voshim-bopcyt-3virBe",
        role="TRAINING_ROLE",
        warehouse="BISON_WH",
        database="BISON_DB",
        schema="AIRFLOW"
    )
    cursor = conn.cursor()

    try:
        ensure_processed_pdfs_table_exists(cursor)
        cursor.execute(f"LIST @{stage_name}")
        files = cursor.fetchall()

        if not files:
            print("No files found in stage")
            return pd.DataFrame()

        results = []
        print(f"Found {len(files)} files in stage {stage_name}")

        for row in tqdm(files, desc='Processing PDFs', unit='file'):
            file_path = row[0]
            filename = os.path.basename(file_path)
            if not filename.lower().endswith('.pdf'):
                continue

            tmp_dir = tempfile.mkdtemp()
            try:
                stage_file = f"@{stage_name}/{filename}"
                get_query = f"GET '{stage_file}' file://{tmp_dir}/"
                try:
                    cursor.execute(get_query)
                except Exception as e:
                    print(f"Failed GET for {filename}: {e}")
                    continue

                local_path = os.path.join(tmp_dir, filename)
                if not os.path.exists(local_path):
                    print(f"File not found locally: {local_path}")
                    continue

                try:
                    with open(local_path, 'rb') as f:
                        file_bytes = f.read()
                    info = extract_course_info_from_bytes(file_bytes, filename)
                    results.append(info)
                except Exception as e:
                    print(f"Failed processing {filename}: {e}")
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        if not results:
            print("No PDF files were successfully processed")
            return pd.DataFrame()

        df = pd.DataFrame(results)
        cleaned_df = clean_and_store_data(df)
        # Rename columns to match the Snowflake table exactly.
        cleaned_df = fix_columns_for_snowflake(cleaned_df)
        store_results_in_snowflake(cursor, cleaned_df)

        # Also export the enriched data to a CSV file for downstream tasks.
        output_file = "RMP_ENRICHED_DATA.csv"
        cleaned_df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")

        return cleaned_df

    except Exception as e:
        print(f"Error in process_pdfs_from_snowflake: {e}")
        return pd.DataFrame()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    df = process_pdfs_from_snowflake()
    if not df.empty:
        print("\nProcessed Data Sample:")
        print(df.head())


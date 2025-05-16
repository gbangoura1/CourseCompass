import os
import re
import shutil
import tempfile
import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def scrape_professor_rating(rmp_id):
    """
    Scrape professor rating information from RateMyProfessor.
    Args:
        rmp_id (str): RateMyProfessor ID of the professor.
    Returns:
        dict: Dictionary containing rating information.
    """
    if not rmp_id or pd.isnull(rmp_id):
        return None
    print(f"Scraping professor: {rmp_id}")
    url = f"https://www.ratemyprofessors.com/professor/{rmp_id}"
    
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) '
                       'Chrome/91.0.4472.124 Safari/537.36'),
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.ratemyprofessors.com/'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve page for professor ID {rmp_id}: {response.status_code}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        rating_info = {
            'rmp_id': rmp_id,
            'scraped_overall_rating': None,
            'would_take_again': None,
            'difficulty': None,
            'tags': []
        }
        
        rating_element = soup.find('div', class_=re.compile(r'RatingValue__Numerator-.*'))
        if rating_element:
            rating_info['scraped_overall_rating'] = rating_element.text.strip()
        
        feedback_numbers = soup.find_all('div', class_=re.compile(r'FeedbackItem__FeedbackNumber-.*'))
        if len(feedback_numbers) >= 2:
            wt_again = feedback_numbers[0].text.strip()
            difficulty = feedback_numbers[1].text.strip()
            if '%' in wt_again:
                wt_again = wt_again.replace('%', '')
            rating_info['would_take_again'] = wt_again
            rating_info['difficulty'] = difficulty
        
        tag_elements = soup.find_all('span', class_=re.compile(r'Tag-.*'))
        if tag_elements:
            tags = set([tag.text.lower().strip() for tag in tag_elements])
            rating_info['tags'] = tags
        
        print(rating_info)
        return rating_info
    except Exception as e:
        print(f"Error scraping professor ID {rmp_id}: {e}")
        return None

def load_input_csv_from_stage(stage_name, cursor):
    """
    Download the input.csv file from the given stage and load it as a DataFrame.
    Assumes the CSV has at least the columns 'course_code' and 'rmp_id'.
    """
    tmp_dir = tempfile.mkdtemp()
    csv_df = pd.DataFrame()
    try:
        get_cmd = f"GET '@{stage_name}/input.csv' file://{tmp_dir}/"
        cursor.execute(get_cmd)
        local_csv = os.path.join(tmp_dir, "input.csv")
        if os.path.exists(local_csv):
            csv_df = pd.read_csv(local_csv)
        else:
            print("input.csv not found in stage.")
    except Exception as e:
        print(f"Error loading input.csv: {e}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    if not csv_df.empty:
        csv_df.columns = [col.lower().strip() for col in csv_df.columns]
    return csv_df

def scrape_professor_ratings_table(input_table, output_table, sf_conn_params, stage_name="APP_STG"):
    """
    Enrich course records by merging RMP IDs from input.csv (from the same stage)
    with the enriched PDF data from the input_table, then scrape RateMyProfessor data.
    Finally, write the enriched table to Snowflake.
    
    Args:
        input_table (str): Name of the input table in Snowflake (e.g., "APP_PROCESSED_PDFS").
        output_table (str): Name of the output table to store enriched data.
        sf_conn_params (dict): Snowflake connection parameters.
        stage_name (str): The stage where input.csv is located.
    """
    conn = snowflake.connector.connect(**sf_conn_params)
    cursor = conn.cursor()
    
    # Load enriched PDF data.
    query = f"SELECT * FROM {input_table}"
    cursor.execute(query)
    df = cursor.fetch_pandas_all()
    if df is None or df.empty:
        print("No data found in input table.")
        cursor.close()
        conn.close()
        return
    
    df.columns = [col.lower().strip() for col in df.columns]
    
    # Load input.csv from stage.
    csv_df = load_input_csv_from_stage(stage_name, cursor)
    if csv_df.empty:
        print("input.csv did not load any data; cannot merge RMP IDs.")
    else:
        csv_df.columns = [col.lower().strip() for col in csv_df.columns]
        df['course_code'] = df['course_code'].astype(str).str.strip()
        csv_df['course_code'] = csv_df['course_code'].astype(str).str.strip()
        try:
            df = pd.merge(df, csv_df[['course_code', 'rmp_id']], on='course_code', how='left', suffixes=("", "_csv"))
            if 'rmp_id_csv' in df.columns:
                df['rmp_id'] = df['rmp_id_csv'].combine_first(df.get('rmp_id'))
                df.drop(columns=['rmp_id_csv'], inplace=True)
        except Exception as e:
            print(f"Error during merge with input.csv: {e}")
    
    df.columns = [col.lower().strip() for col in df.columns]
    if 'rmp_id' not in df.columns or df['rmp_id'].dropna().empty:
        print("No RMP IDs found after merging. Exiting.")
        cursor.close()
        conn.close()
        return

    # Add scraped data columns.
    df['scraped_overall_rating'] = None
    df['would_take_again'] = None
    df['difficulty'] = None
    df['tags'] = None
    
    unique_ids = df['rmp_id'].dropna().unique()
    print(f"Found {len(unique_ids)} unique professors with RMP IDs")
    ratings_by_id = {}
    processed = 0
    for rmp_id in unique_ids:
        try:
            rmp_id_clean = str(int(float(rmp_id))).strip()
        except Exception:
            rmp_id_clean = str(rmp_id)
        rating_info = scrape_professor_rating(rmp_id_clean)
        if rating_info:
            ratings_by_id[rmp_id_clean] = rating_info
        processed += 1
        print(f"Processed {processed}/{len(unique_ids)} professors")
        time.sleep(random.uniform(1, 3))
    
    for idx, row in df.iterrows():
        if pd.notna(row.get('rmp_id')):
            try:
                rmp_id_clean = str(int(float(row['rmp_id']))).strip()
            except Exception:
                rmp_id_clean = str(row['rmp_id'])
            if rmp_id_clean in ratings_by_id:
                info = ratings_by_id[rmp_id_clean]
                df.at[idx, 'scraped_overall_rating'] = info.get('scraped_overall_rating')
                df.at[idx, 'would_take_again'] = info.get('would_take_again')
                df.at[idx, 'difficulty'] = info.get('difficulty')
                df.at[idx, 'tags'] = ','.join(info.get('tags', []))
    
    # Extra cleaning: convert any remaining NaN or "nan"/"NAN" values to None, then ensure all values are strings.
    def clean_cell(x):
        if pd.isnull(x):
            return None
        if isinstance(x, str):
            if x.strip().lower() == "nan":
                return None
            return x.strip()
        return str(x)
    
    df = df.applymap(clean_cell)
    
    # Build output table DDL.
    columns = list(df.columns)
    create_sql = f"CREATE OR REPLACE TABLE {output_table} ("
    create_sql += ", ".join([f"\"{col.strip().upper().replace(' ', '_')}\" STRING" for col in columns])
    create_sql += ")"
    print("DDL:", create_sql)
    cursor.execute(create_sql)
    
    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = f"INSERT INTO {output_table} VALUES({placeholders})"
    data = [tuple(row) for row in df.values]
    cursor.executemany(insert_sql, data)
    conn.commit()
    
    cursor.close()
    conn.close()
    print(f"Scraped professor ratings stored in table {output_table}")

if __name__ == "__main__":
    sf_conn_params = {
        'account': "SFEDU02-IOB55870",
        'user': "BISON",
        'password': "voshim-bopcyt-3virBe",
        'role': "TRAINING_ROLE",
        'warehouse': "BISON_WH",
        'database': "BISON_DB",
        'schema': "AIRFLOW"
    }
    scrape_professor_ratings_table("APP_PROCESSED_PDFS", "RMP_ENRICHED_DATA", sf_conn_params, stage_name="APP_STG")


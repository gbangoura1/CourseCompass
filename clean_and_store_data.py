import os
import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def clean_and_store_data(input_table, target_table, sf_conn_params=None):
    """
    Clean the DataFrame and store it in Snowflake.
    Reads from input_table (e.g. RMP_ENRICHED_DATA), cleans the data,
    and writes the result to target_table.
    """
    if sf_conn_params is None:
        sf_conn_params = {
            "account": "SFEDU02-IOB55870",
            "user": "BISON",
            "password": "voshim-bopcyt-3virBe",
            "role": "TRAINING_ROLE",
            "warehouse": "BISON_WH",
            "database": "BISON_DB",
            "schema": "AIRFLOW",
        }
        
    conn = snowflake.connector.connect(**sf_conn_params)
    cursor = conn.cursor()
    
    try:
        query = f"SELECT * FROM {input_table}"
        cursor.execute(query)
        df = cursor.fetch_pandas_all()
        if df.empty:
            print(f"No data found in table {input_table}.")
            return df
        
        # Replace any null-like values with None
        df = df.replace([np.nan, 'NAN', 'nan', 'NaN', '', None], None)
        
        # Do NOT rename FILENAME. Ensure column names match target table.
        # Convert all column names to uppercase.
        df.columns = [col.upper() for col in df.columns]
    
        # Example cleaning:
        if "OVERALL_RATING" in df.columns:
            df["OVERALL_RATING"] = pd.to_numeric(df["OVERALL_RATING"], errors="coerce")
        if "RMP_RATING" in df.columns:
            df["RMP_RATING"] = pd.to_numeric(df["RMP_RATING"], errors="coerce")
        if "DIFFICULTY" in df.columns:
            df["DIFFICULTY"] = pd.to_numeric(df["DIFFICULTY"], errors="coerce")
        if "WOULD TAKE AGAIN" in df.columns:
            df["WOULD TAKE AGAIN"] = pd.to_numeric(df["WOULD TAKE AGAIN"], errors="coerce")
            df["WOULD TAKE AGAIN"] = df["WOULD TAKE AGAIN"].apply(lambda x: x / 100 if pd.notna(x) and x > 1 else x)
        
        print("Cleaned data sample:")
        print(df.head())
        
        # Write the DataFrame to the target table.
        success, nchunks, nrows, _ = write_pandas(conn, df, target_table, quote_identifiers=True)
        if success:
            print(f"Loaded {nrows} rows into table {target_table}")
        else:
            print("Failed to write data to Snowflake.")
    finally:
        cursor.close()
        conn.close()
    
    return df

if __name__ == "__main__":
    sf_conn_params = {
        "account": "SFEDU02-IOB55870",
        "user": "BISON",
        "password": "voshim-bopcyt-3virBe",
        "role": "TRAINING_ROLE",
        "warehouse": "BISON_WH",
        "database": "BISON_DB",
        "schema": "AIRFLOW",
    }
    clean_and_store_data("RMP_ENRICHED_DATA", "MY_WASHU_COURSES_WITH_RATINGS", sf_conn_params)


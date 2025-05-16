from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import dag
from datetime import datetime, timedelta

# Import custom functions.
from process_pdfs import process_pdfs_from_snowflake
from scrape_professor_ratings import scrape_professor_ratings_table
from clean_and_store_data import clean_and_store_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sf_conn_params = {
    "account": "SFEDU02-IOB55870",
    "user": "BISON",
    "password": "voshim-bopcyt-3virBe",
    "role": "TRAINING_ROLE",
    "warehouse": "BISON_WH",
    "database": "BISON_DB",
    "schema": "AIRFLOW",
}

@dag(
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    dag_id="app_data_pipeline"
)
def app_data_pipeline():
    # Task 1: Create the SIGNAL_TABLE
    create_signal_table = SQLExecuteQueryOperator(
        task_id='create_signal_table',
        conn_id='SnowFlakeConn',
        sql="""
            CREATE OR REPLACE TABLE SIGNAL_TABLE (
                TABLE_NAME STRING,
                STATUS STRING,
                TIMESTAMP TIMESTAMP_LTZ
            );
        """,
    )
    
    # Task 1a: Insert a dummy signal so the sensor can run.
    insert_dummy_signal = SQLExecuteQueryOperator(
        task_id='insert_dummy_signal',
        conn_id='SnowFlakeConn',
        sql="""
            INSERT INTO SIGNAL_TABLE (TABLE_NAME, STATUS, TIMESTAMP)
            VALUES ('APP_STG', 'success', CURRENT_TIMESTAMP());
        """,
    )
    
    # Task 2: Wait for the staging signal.
    check_app_stg_signal = SqlSensor(
        task_id='check_app_stg_signal',
        conn_id='SnowFlakeConn',
        sql="SELECT 1 FROM SIGNAL_TABLE WHERE TABLE_NAME = 'APP_STG' AND STATUS = 'success'",
        timeout=600,
        poke_interval=30,
        mode='reschedule'
    )
    
    # Task 3: Create the target table MY_WASHU_COURSES_WITH_RATINGS.
    create_target_table = SQLExecuteQueryOperator(
        task_id='create_target_table',
        conn_id='SnowFlakeConn',
        sql="""
            CREATE OR REPLACE TABLE MY_WASHU_COURSES_WITH_RATINGS (
                FILENAME VARCHAR,
                PROFESSOR VARCHAR,
                SEMESTER VARCHAR,
                COURSE_CODE VARCHAR,
                SECTION VARCHAR,
                COURSE_NAME VARCHAR,
                OVERALL_RATING FLOAT,
                RMP_ID NUMBER(38,0),
                SCRAPED_OVERALL_RATING FLOAT,
                WOULD_TAKE_AGAIN FLOAT,
                DIFFICULTY FLOAT,
                TAGS VARCHAR
            );
        """,
    )
    
    # Task 4: Process PDFs from the stage.
    def run_process_pdfs():
        stage_name = "APP_STG"
        df = process_pdfs_from_snowflake(stage_name)
        if df.empty:
            print("No PDF data extracted; skipping further steps.")
            return
        print("Processed PDF data stored in table APP_PROCESSED_PDFS")
    
    process_pdfs_task = PythonOperator(
        task_id='process_pdfs',
        python_callable=run_process_pdfs,
    )
    
    # Task 5: Scrape RateMyProfessor data.
    def run_scrape_rmp():
        input_table = "APP_PROCESSED_PDFS"
        output_table = "RMP_ENRICHED_DATA"
        scrape_professor_ratings_table(input_table, output_table, sf_conn_params)
        print("Scraped RMP data stored in table RMP_ENRICHED_DATA")
    
    scrape_rmp_task = PythonOperator(
        task_id='scrape_rmp',
        python_callable=run_scrape_rmp,
    )
    
    # Task 6: Clean and load the enriched data.
    def run_clean_and_store():
        clean_and_store_data("RMP_ENRICHED_DATA", "MY_WASHU_COURSES_WITH_RATINGS", sf_conn_params)
        print("Enriched data loaded into table MY_WASHU_COURSES_WITH_RATINGS")
    
    load_enriched_data_task = PythonOperator(
        task_id='load_enriched_data',
        python_callable=run_clean_and_store,
    )
    
    # Task 7: Standardize data and create a view with additional manipulations.
    standardization_sql = """
    CREATE OR REPLACE TABLE WASHU_COURSES_STANDARDIZED AS
    WITH normalized_data AS (
        SELECT
            TRIM(PROFESSOR) AS ORIGINAL_PROFESSOR_NAME,
            SPLIT_PART(TRIM(PROFESSOR), ' ', -1) AS PROFESSOR_NAME,
            TRIM(COURSE_CODE) AS COURSE_CODE,
            TRIM(COURSE_NAME) AS COURSE_NAME,
            TRIM(SEMESTER) AS SEMESTER,
            TRIM(SECTION) AS SECTION,
            RMP_ID,
            TO_NUMBER(OVERALL_RATING) AS OVERALL_RATING_NUM,
            TO_NUMBER(SCRAPED_OVERALL_RATING) AS SCRAPED_OVERALL_RATING_NUM,
            CASE
                WHEN TO_NUMBER(WOULD_TAKE_AGAIN) > 1 THEN TO_NUMBER(WOULD_TAKE_AGAIN)/100
                ELSE TO_NUMBER(WOULD_TAKE_AGAIN)
            END AS WOULD_TAKE_AGAIN_NUM,
            TO_NUMBER(DIFFICULTY) AS DIFFICULTY_NUM,
            TRIM(TAGS) AS TAGS,
            ROW_NUMBER() OVER (
                PARTITION BY
                    SPLIT_PART(TRIM(PROFESSOR), ' ', -1),
                    TRIM(COURSE_CODE),
                    TRIM(SEMESTER),
                    TRIM(SECTION)
                ORDER BY
                    CASE WHEN TO_NUMBER(OVERALL_RATING) IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN TO_NUMBER(SCRAPED_OVERALL_RATING) IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN TRIM(COURSE_NAME) IS NOT NULL THEN 0 ELSE 1 END
            ) AS ROW_RANK
        FROM MY_WASHU_COURSES_WITH_RATINGS
    )
    SELECT
        PROFESSOR_NAME,
        ORIGINAL_PROFESSOR_NAME AS FULL_PROFESSOR_NAME,
        COURSE_CODE,
        COURSE_NAME,
        SEMESTER,
        SECTION,
        RMP_ID,
        OVERALL_RATING_NUM AS OVERALL_RATING,
        SCRAPED_OVERALL_RATING_NUM AS SCRAPED_OVERALL_RATING,
        WOULD_TAKE_AGAIN_NUM AS WOULD_TAKE_AGAIN,
        DIFFICULTY_NUM AS DIFFICULTY,
        TAGS,
        CASE
            WHEN SCRAPED_OVERALL_RATING_NUM IS NOT NULL THEN (SCRAPED_OVERALL_RATING_NUM - 1) * (6/4) + 1
            ELSE NULL
        END AS NORMALIZED_RMP_RATING
    FROM normalized_data
    WHERE ROW_RANK = 1;

    CREATE OR REPLACE VIEW V_WASHU_COURSES AS
    SELECT * FROM WASHU_COURSES_STANDARDIZED;
    """
    
    standardization_task = SQLExecuteQueryOperator(
        task_id='run_standardization',
        conn_id='SnowFlakeConn',
        sql=standardization_sql,
    )
    
    # Task 8: Insert the final signal.
    insert_signal_final = SQLExecuteQueryOperator(
        task_id='insert_signal_final',
        conn_id='SnowFlakeConn',
        sql="""
            INSERT INTO SIGNAL_TABLE (TABLE_NAME, STATUS, TIMESTAMP)
            VALUES ('WASHU_COURSES_STANDARDIZED', 'success', CURRENT_TIMESTAMP());
        """,
    )
    
    # Set task dependencies.
    create_signal_table >> insert_dummy_signal >> check_app_stg_signal >> process_pdfs_task
    process_pdfs_task >> scrape_rmp_task >> create_target_table >> load_enriched_data_task >> standardization_task >> insert_signal_final

app_data_pipeline_dag = app_data_pipeline()


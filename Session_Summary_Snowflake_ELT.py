from airflow.decorators import task
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging
import snowflake.connector

"""
This pipeline will run after Session_Stage_Load_ETL is executed. Below two tables will get created and loaded before executing this DAG:
 - user_session_channel
 - session_timestamp
"""

def return_snowflake_conn():
    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(table, select_sql, primary_key=None, duplicate_check_columns=None):
    logging.info(table)
    logging.info(select_sql)

    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        sql = f"CREATE OR REPLACE TABLE {table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        # Primary key uniqueness check
        if primary_key is not None:
            sql = f"SELECT {primary_key}, COUNT(1) AS cnt FROM {table} GROUP BY 1 ORDER BY 2 DESC LIMIT 1"
            logging.info(sql)
            cur.execute(sql)
            result = cur.fetchone()
            if int(result[1]) > 1:
                raise Exception(f"Primary key uniqueness failed: {result}")
        
        # Check for duplicate records based on multiple columns
        if duplicate_check_columns:
            columns = ', '.join(duplicate_check_columns)
            sql = f"SELECT {columns}, COUNT(1) AS cnt FROM {table} GROUP BY {columns} HAVING cnt > 1"
            logging.info(sql)
            cur.execute(sql)
            duplicates = cur.fetchall()
            if duplicates:
                raise Exception(f"Duplicate records found based on {duplicate_check_columns}: {duplicates}")

        # Hash-based duplicate check
        if duplicate_check_columns:
            columns = ', '.join(duplicate_check_columns)
            hash_sql = f"SELECT MD5({columns}), COUNT(*) FROM {table} GROUP BY MD5({columns}) HAVING COUNT(*) > 1"
            logging.info(f"Running hash-based duplicate check: {hash_sql}")
            cur.execute(hash_sql)
            hash_duplicates = cur.fetchall()
            if hash_duplicates:
                raise Exception(f"Hash-based duplicate records found: {hash_duplicates}")

        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error(f"Failed SQL execution. Completed ROLLBACK! Error: {str(e)}")
        raise


with DAG(
    dag_id='BuildELT_CTAS',
    start_date=datetime(2024, 10, 2),
    catchup=False,
    tags=['ELT'],
    schedule='45 2 * * *'
) as dag:

    table = "dev.analytics.session_summary"
    select_sql = """
    SELECT u.*, s.ts
    FROM dev.raw_data.user_session_channel u
    JOIN dev.raw_data.session_timestamp s ON u.sessionId=s.sessionId
    """

    # Run the task with primary key uniqueness, multi-column check, and hash-based duplicate check
    run_ctas(
        table, 
        select_sql, 
        primary_key='sessionId', 
        duplicate_check_columns=['userId', 'sessionId']  # Example: Check for duplicates based on 'userId' and 'sessionId'
    )

from datetime import datetime
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
import logging
import tempfile
import os

# Constants
MSSQL_CONN_ID = 'mssql_default'
SNOWFLAKE_CONN_ID = 'snowflake_default'
SCHEMA = 'raw'  # Replace with your MSSQL schema name
SNOWFLAKE_STAGING_SCHEMA = 'stg'  # Replace with your Snowflake staging schema

dag = DAG(
    dag_id='mssql_to_snowflake2',
    schedule='@daily',
    start_date=datetime(2023, 12, 12)
)

# Sensor to wait until tables are available
tables_sensor = SqlSensor(
    task_id='wait_for_tables',
    conn_id=MSSQL_CONN_ID,
    sql=f"IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{SCHEMA}') SELECT 1 ELSE SELECT 0",
    timeout=600,  # Adjust timeout as needed
    poke_interval=60,  # Adjust poke_interval as needed
    mode="poke",
    dag=dag,
)

# PythonOperator to fetch tables and transfer data
def transfer_data():
    try:
        mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
        tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{SCHEMA}'"
        tables = mssql_hook.get_records(tables_query)

        for table in tables:
            table_name = table[0]
            extract_query = f'SELECT * FROM {SCHEMA}.{table_name}'
            data = mssql_hook.get_pandas_df(extract_query)

            # Save DataFrame to a temporary CSV file
            csv_file_path = tempfile.NamedTemporaryFile(delete=False, suffix='.csv').name
            data.to_csv(csv_file_path, index=False)

            snowflake_task = SnowflakeOperator(
                task_id=f'transfer_{table_name}_data',
                sql=f'COPY INTO {SNOWFLAKE_STAGING_SCHEMA}.{table_name} FROM @{csv_file_path}',
                snowflake_conn_id=SNOWFLAKE_CONN_ID,
                warehouse='your_snowflake_warehouse',
                database='your_snowflake_database',
                schema=SCHEMA,
            )
            snowflake_task.execute(context=None)

            # Remove temporary CSV file
            os.remove(csv_file_path)

    except Exception as e:
        # Log the error
        logging.error(f"Error transferring data: {str(e)}")
        raise

transfer_data_task = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag,
)

# Trigger transfer_data_task after tables_sensor
tables_sensor >> transfer_data_task

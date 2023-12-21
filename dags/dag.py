# from airflow import DAG
# from datetime import datetime,timedelta
# from astro.sql import Table,merge
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# SCHEMA = 'raw'
# mssql_conn_id = "mssql_default"



# tables= [
#     'account_category',
#     'address_type',
#     'bill_category',
#     'customers',
#     'phone_type',
#     'states',
#     'account_type',
#     'accounts',
#     'addresses',
#     'agent',
#     'balance',
#     'biller',
#     'biller_transaction',
#     'bvn',
#     'lgas',
#     'merchants',
#     'other_banks',
#     'phone',
#     'pos_terminal',
#     'transactions'
# ]


# def generate_create_table_sql(table):
#     mssql = MsSqlHook(mssql_conn_id=mssql_conn_id)
#     records = mssql.get_records(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{SCHEMA}'")
#     columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
#     return f"CREATE TABLE {table} ({columns})"  

# default_args = {
#     "owner": 'josh',
#     "start_date": datetime(2023, 12, 21),
#     "retries": 2,
#     "retry_delay": timedelta(minutes=6),
# }

# with DAG('mssql_to_snowflake_dag', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:

#     for table in tables:
#         extract = MsSqlOperator(
#         task_id=f'extract_{table}',
#         mssql_conn_id='mssql_default',
#         sql=f'SELECT * FROM {table}',
#     )
#         create_table = SnowflakeOperator(
#         task_id=f'create_{table}_in_snowflake',
#         snowflake_conn_id='snowflake_default',
#         sql=f'CREATE TABLE {table}',  
#     )
#         load = SnowflakeOperator(
#         task_id=f'load_{table}_into_snowflake',
#         snowflake_conn_id='snowflake_default',
#         sql=f'INSERT INTO {table} VALUES from {extract}',
#     )
#         extract >> create_table >> load

from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SCHEMA = 'raw'
mssql_conn_id = "mssql_default"

tables = [
    'account_category',
    'address_type',
    'bill_category',
    'customers',
    'phone_type',
    'states',
    'account_type',
    'accounts',
    'addresses',
    'agent',
    'balance',
    'biller',
    'biller_transaction',
    'bvn',
    'lgas',
    'merchants',
    'other_banks',
    'phone',
    'pos_terminal',
    'transactions'
]

def generate_create_table_sql(table):
    mssql = MsSqlHook(mssql_conn_id=mssql_conn_id)
    records = mssql.get_records(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{SCHEMA}'")
    columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
    return f"CREATE TABLE {table} ({columns})"

default_args = {
    "owner": 'josh',
    "start_date": datetime(2023, 12, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
}

with DAG('mssql_to_snowflake_dag', default_args=default_args, schedule_interval='@daily', catchup=True) as dag:

    for table in tables:
        extract = MsSqlOperator(
            task_id=f'extract_{table}',
            mssql_conn_id=mssql_conn_id,
            sql=f'SELECT * FROM {SCHEMA}.{table}',
        )
        create_table = SnowflakeOperator(
            task_id=f'create_{table}_in_snowflake',
            snowflake_conn_id='snowflake_default',
            sql=generate_create_table_sql(table),
        )
        load = SnowflakeOperator(
            task_id=f'load_{table}_into_snowflake',
            snowflake_conn_id='snowflake_default',
            sql=f'INSERT INTO {table} SELECT * FROM {SCHEMA}.{table}',
        )
        extract >> create_table >> load



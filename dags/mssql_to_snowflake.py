from airflow import DAG
from datetime import datetime,timedelta
from astro.sql import Table,merge
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.sensors.sql import SqlSensor

# Ensure to import necessary modules and classes properly

SCHEMA = 'raw'
mssql_conn_id = "mssql_default"



tables= [
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
    records = mssql.get_records(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '{SCHEMA}'")
    columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
    return f"CREATE TABLE {table} ({columns})"  

def get_primary_key_columns(table):
    mssql = MsSqlHook(mssql_conn_id=mssql_conn_id)
    records = mssql.get_records(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = '{table}' AND CONSTRAINT_NAME LIKE 'PK_%'")
    return [row[0] for row in records]


default_args = {
    "owner": 'josh',
    "start_date": datetime(2023, 12, 13),
    "retries": 2,
    "retry_delay": timedelta(minutes=6),
}

with DAG('mssql_to_snowflake5', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # tables_sensor = SqlSensor(
    #     task_id='wait_for_tables',
    #     conn_id=mssql_conn_id, 
    #     sql=f"IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{SCHEMA}') SELECT 1 ELSE SELECT 0",
    #     timeout=600,
    #     poke_interval=60,
    #     mode="poke",
    #     dag=dag,
    # )
    for table in tables:
        create_table = SnowflakeOperator(
            task_id=f'create_{table}_in_snowflake',
            snowflake_conn_id='snowflake_default',
            sql=generate_create_table_sql(table=table)
        )

        source_table = Table(name=table, conn_id='mssql_default')
        destination_table = Table(name=table, conn_id='snowflake_default')


        mssql = MsSqlHook(mssql_conn_id=mssql_conn_id)
        records = mssql.get_records(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{table}' AND table_schema = '{SCHEMA}'")
        columns_mapping = [(row[0], row[0]) for row in records]
        primary_key_columns = get_primary_key_columns(table)
        target_conflict_columns = ', '.join(primary_key_columns)

        upload = merge(
            target_table=destination_table,
            source_table=source_table,
            columns=columns_mapping,
            if_conflicts="update",
            target_conflict_columns=target_conflict_columns
        )


        create_table >> upload  # Define execution flow,

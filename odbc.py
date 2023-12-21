# from airflow import DAG
# from pendulum import datetime
# from astro.sql import Table, merge
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
# from airflow.providers.odbc.hooks.odbc import OdbcHook


# def generate_create_table_sql(table):
#     odbc_hook=OdbcHook(odbc_conn_id='mssql_default')
#     records =odbc_hook.get_records(f"SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}")
#     columns= ', '.join([f"{row[0]} {row[1]}" for row in records])
#     return f"CREATE TABLE {table}({columns})"

# tables=["table1","table2"]

# default_args={
#     'owner':'josh',
#     'start_date':datetime(2023,12,13),
# }




  git config --global user.email "jdada371@gmail.com"/
  git config --global user.name "joshua-dada-mayowa"
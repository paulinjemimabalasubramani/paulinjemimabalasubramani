"""
Move curated data in Snowflake back to on prem SQL Server

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


https://docs.snowflake.com/en/user-guide/spark-connector.html
https://docs.databricks.com/_static/notebooks/snowflake-python.html

"""

# %% Import Libraries

import os, sys

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from pprint import pprint


from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, write_sql, read_snowflake
from modules.azure_functions import get_azure_sp
from modules.snowflake_ddl import snowflake_ddl_params


from pyspark.sql.functions import col, lit



# %% Logging
logger = make_logging(__name__)


# %% Parameters

sql_server = 'DSQLOLTP02'
sql_database = 'FinancialProfessional'
sql_schema = 'edip'
sql_key_vault_account = sql_server

sf_account = snowflake_ddl_params.snowflake_account
sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse
sf_database = snowflake_ddl_params.snowflake_curated_database
sf_schema = 'EDIP'



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark



# %% Read Key Vault Data

_, sql_id, sql_pass = get_azure_sp(sql_key_vault_account.lower())
_, sf_id, sf_pass = get_azure_sp(snowflake_ddl_params.sf_key_vault_account.lower())



# %% Get List of Tables and Columns from Information Schema

@catch_error(logger)
def get_sf_table_list(schema_name:str):
    print('\nGet List of Tables and Columns from Information Schema...')
    tables = read_snowflake(
        spark = spark,
        table_name = 'TABLES',
        schema = 'INFORMATION_SCHEMA',
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        )

    columns = read_snowflake(
        spark = spark,
        table_name = 'COLUMNS',
        schema = 'INFORMATION_SCHEMA',
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        )

    tables = tables.filter((col('TABLE_SCHEMA') == lit(schema_name)) & (col('TABLE_TYPE')==lit('BASE TABLE')))
    columns = columns.filter(col('TABLE_SCHEMA') == lit(schema_name))
    columns = columns.alias('c').join(tables.alias('t'), columns['TABLE_NAME']==tables['TABLE_NAME'], how='inner').select('c.*')

    table_names = columns.select('TABLE_NAME').distinct().rdd.flatMap(lambda x: x).collect()

    print(f'Total of {len(table_names)} tables')
    pprint(table_names)
    return table_names, columns



table_names, columns = get_sf_table_list(schema_name=sf_schema)



# %% Loop over all tables

@catch_error(logger)
def reverse_etl_all_tables():
    for table_name in table_names:
        table = read_snowflake(
            spark = spark,
            table_name = table_name,
            schema = sf_schema,
            database = sf_database,
            warehouse = sf_warehouse,
            role = sf_role,
            account = sf_account,
            user = sf_id,
            password = sf_pass,
            )

        write_sql(
            table = table,
            table_name = table_name.lower(),
            schema = sql_schema,
            database = sql_database,
            server = sql_server,
            user = sql_id,
            password = sql_pass,
            mode = 'overwrite',
        )
    
    print('Finished Reverse ETL for all tables')



reverse_etl_all_tables()


# %%



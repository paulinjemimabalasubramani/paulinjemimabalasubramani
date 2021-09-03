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

from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import logger, catch_error, get_secrets
from modules.spark_functions import create_spark, write_sql, read_snowflake
from modules.snowflake_ddl import snowflake_ddl_params


from pyspark.sql.functions import col, lit



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

_, sql_id, sql_pass = get_secrets(sql_key_vault_account.lower(), logger=logger)
_, sf_id, sf_pass = get_secrets(snowflake_ddl_params.sf_key_vault_account.lower(), logger=logger)



# %% Get List of Tables and Columns from Information Schema

@catch_error(logger)
def get_sf_table_list(schema_name:str):
    logger.info('Get List of Tables and Columns from Information Schema...')
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

    logger.info(f'Total of {len(table_names)} tables')
    logger.info(table_names)
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
    
    logger.info('Finished Reverse ETL for all tables')



reverse_etl_all_tables()


# %%



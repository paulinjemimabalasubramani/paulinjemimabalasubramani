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


from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import  get_azure_sp
from modules.snowflake_ddl import connect_to_snowflake, snowflake_ddl_params
from modules.config import is_pc


from pyspark.sql.functions import col, lit



# %% Logging
logger = make_logging(__name__)


# %% Parameters

sql_server = 'DSQLOLTP02'

sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse
sf_database = snowflake_ddl_params.snowflake_curated_database
sf_schema = 'EDIP'



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark



# %% Read Key Vault Data

_, sql_id, sql_pass = get_azure_sp(sql_server.lower())
_, sf_id, sf_pass = get_azure_sp(snowflake_ddl_params.sf_key_vault_account.lower())


# %% Function to Get Snowflake Table

@catch_error(logger)
def get_sf_table(schema_name, table_name):
    sf_options = {
        'sfUrl': f'{snowflake_ddl_params.snowflake_account}.snowflakecomputing.com',
        'sfUser': sf_id,
        'sfPassword': sf_pass,
        'sfRole': sf_role,
        'sfWarehouse': sf_warehouse,
        'sfDatabase': sf_database,
        'sfSchema': schema_name,
        }

    table = spark.read \
        .format('snowflake') \
        .options(**sf_options) \
        .option('dbtable', table_name) \
        .load()
    
    return table



# %% Get List of Tables from Information Schema

tables = get_sf_table(
    schema_name = 'INFORMATION_SCHEMA',
    table_name = 'TABLES',
    )


if is_pc: tables.show(5)


# %%

tables = tables.filter((col('TABLE_SCHEMA') == lit(sf_schema)) & (col('TABLE_TYPE')==lit('BASE TABLE')))

# %%

table_names = tables.select('TABLE_NAME').rdd.flatMap(lambda x: x).collect()


# %%

table_name = table_names[0]

table = get_sf_table(
    schema_name = sf_schema,
    table_name = table_name,
    )







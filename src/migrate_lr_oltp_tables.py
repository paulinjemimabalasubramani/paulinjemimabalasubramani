"""
Test Spark - SQL connection

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common import make_logging, catch_error
from modules.spark_functions import SparkSession
from modules.config import get_azure_storage_key_valut


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window



# %% Logging
logger = make_logging(__name__)


# %% Parameters

schema = 'OLTP'
database = 'LR'
server = 'TSQLOLTP01'

storage_account_name = "agaggrlakescd"
azure_tenant_id, sp_id, sp_pass = get_azure_storage_key_valut(storage_name=storage_account_name)
container_name = "ingress"
domain_name = 'financial_professional'
format = 'delta'

partitionBy = 'EXECUTION_DATE'

# %% Create Session

ss = SparkSession()

# %% Get Table and Column Metadata from information_schema

df_tables = ss.read_sql(schema='information_schema', table='tables', database=database, server=server)
df_tables.printSchema()

df_columns = ss.read_sql(schema='information_schema', table='columns', database=database, server=server)
df_columns.printSchema()

# %% Filter Tables for Schema

df_tables = df_tables.filter(f"TABLE_TYPE == 'BASE TABLE' and  TABLE_SCHEMA = '{schema}'")
table_list = df_tables.select("TABLE_NAME").rdd.flatMap(lambda x: x).collect()

table_count = len(table_list)
print(f"{table_count} tables in {schema}")

# %% Loop over all selected tables

for i, table in enumerate(table_list):
    if i>0: # for testing
        break

    print(f'\nTable {i+1} of {table_count}: {table}')

    data_type = 'data'
    container_folder = f"{data_type}/{domain_name}/{database}/{schema}"

    df = ss.read_sql(schema=schema, table=table, database=database, server=server)
    df = ss.to_string(df, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.
    df = ss.remove_column_spaces(df) # May create "name not matching" problems as we are saving column metadata as well.
    df = ss.add_etl_columns(df=df, execution_date=datetime.now())

    ss.save_adls_gen2_sp(df=df,
        storage_account_name = storage_account_name,
        azure_tenant_id = azure_tenant_id,
        sp_id = sp_id,
        sp_pass = sp_pass,
        container_name = container_name,
        container_folder = container_folder,
        table = table,
        partitionBy = partitionBy,
        format = format
    )

    # Metadata
    data_type = 'metadata'
    container_folder = f"{data_type}/{domain_name}/{database}/{schema}"

    df_meta = df_columns.filter((col('TABLE_NAME') == table) & (col('TABLE_SCHEMA') == schema))
    df_meta = ss.remove_column_spaces(df_meta)
    df_meta = ss.add_etl_columns(df=df_meta, execution_date=datetime.now())

    ss.save_adls_gen2_sp(df=df_meta,
        storage_account_name = storage_account_name,
        azure_tenant_id = azure_tenant_id,
        sp_id = sp_id,
        sp_pass = sp_pass,
        container_name = container_name,
        container_folder = container_folder,
        table = table,
        partitionBy = partitionBy,
        format = format
    )


#ss.spark.stop()
print('Done')


# %%

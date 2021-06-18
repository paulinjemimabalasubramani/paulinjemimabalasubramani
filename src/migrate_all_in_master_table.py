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

from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark, read_sql, read_sql_config, read_csv
from modules.azure_functions import get_azure_storage_key_vault, save_adls_gen2_sp
from modules.data_functions import to_string, remove_column_spaces, add_etl_columns


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window



# %% Logging
logger = make_logging(__name__)


# %% Parameters

database = 'LR'
server = 'TSQLOLTP01'

table_list_path = os.path.realpath(os.path.dirname(__file__)+'/../config/LNR_Tables.csv')
assert os.path.isfile(table_list_path), f"File not found: {table_list_path}"

storage_account_name = "agaggrlakescd"
azure_tenant_id, sp_id, sp_pass = get_azure_storage_key_vault(storage_name=storage_account_name)
container_name = "ingress"
domain_name = 'financial_professional'
format = 'delta'

partitionBy = 'EXECUTION_DATE'
execution_date = datetime.now()


# %% Create Session

spark = create_spark()


# %% Get List of Tables of interest

@catch_error(logger)
def get_table_list(table_list_path:str):
    """
    Get List of Tables of interest
    """
    table_list = read_csv(spark, table_list_path)
    if is_pc: table_list.printSchema()
    table_list = table_list.filter(F.lower(col('Table of Interest')) == lit('yes').cast("string"))

    column_map = {
        'TableName': 'TABLE_NAME',
        'SchemaName' : 'TABLE_SCHEMA',
    }

    for key, val in column_map.items():
        table_list = table_list.withColumnRenamed(key, val)

    table_list.createOrReplaceTempView('table_list')
    return table_list



table_list = get_table_list(table_list_path)

if is_pc: table_list.show(5)



# %% Read SQL Config

sql_config = read_sql_config()


# %% Get Table and Column Metadata from information_schema

df_tables = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='information_schema', table='tables', database=database, server=server)
df_tables.printSchema()

df_columns = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema='information_schema', table='columns', database=database, server=server)
df_columns.printSchema()

# %% Filter Tables for Schema

table_list = table_list.select(col('TABLE_NAME'), col('TABLE_SCHEMA')).collect()

table_count = len(table_list)
print(f"{table_count} tables total")

# %% Loop over all selected tables

for i, r in enumerate(table_list):
    table = r['TABLE_NAME']
    schema = r['TABLE_SCHEMA']
    #if is_pc and i>0: # for testing
    #    break

    print(f"\nTable {i+1} of {table_count}: {schema}.{table}")

    data_type = 'data'
    container_folder = f"{data_type}/{domain_name}/{database}/{schema}"

    df = read_sql(spark=spark, user=sql_config.sql_user, password=sql_config.sql_password, schema=schema, table=table, database=database, server=server)
    df = to_string(df, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.
    df = remove_column_spaces(df) # May create "name not matching" problems as we are saving column metadata as well.
    df = add_etl_columns(df=df, execution_date=execution_date)

    save_adls_gen2_sp(
        spark=spark,
        df=df,
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
    df_meta = remove_column_spaces(df_meta)
    df_meta = add_etl_columns(df=df_meta, execution_date=execution_date)

    save_adls_gen2_sp(
        spark=spark,
        df=df_meta,
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

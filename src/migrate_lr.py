"""
Migrate all tables from metadata.TableInfo to the ADLS Gen 2

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
from modules.spark_functions import create_spark, read_sql
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, get_azure_sp, \
    container_name, file_format, to_storage_account_name, tableinfo_name
from modules.data_functions import to_string, remove_column_spaces, add_elt_columns, execution_date, partitionBy


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window



# %% Logging
logger = make_logging(__name__)


# %% Parameters

sql_server = 'TSQLOLTP01'

storage_account_name = to_storage_account_name()
domain_name = 'financial_professional'

reception_date = execution_date
tableinfo_source = 'LR'


# %% Create Session

spark = create_spark()


# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=f'{tableinfo_name}_{tableinfo_source}')


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Read SQL Config

_, sql_id, sql_pass = get_azure_sp(sql_server.lower())



# %% Loop over all tables

@catch_error(logger)
def iterate_over_all_tables(table_rows):
    table_count = len(table_rows)

    for i, r in enumerate(table_rows):
        table = r['TableName']
        schema = r['SourceSchema']
        database = r['SourceDatabase']

        print(f"\nTable {i+1} of {table_count}: {schema}.{table}")

        data_type = 'data'
        container_folder = f"{data_type}/{domain_name}/{database}/{schema}"

        sql_table = read_sql(spark=spark, user=sql_id, password=sql_pass, schema=schema, table=table, database=database, server=sql_server)
        sql_table = to_string(sql_table, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.
        sql_table = remove_column_spaces(sql_table)
        sql_table = add_elt_columns(table_to_add=sql_table, reception_date=reception_date, execution_date=execution_date, source=tableinfo_source)

        save_adls_gen2(
            table_to_save = sql_table,
            storage_account_name = storage_account_name,
            container_name = container_name,
            container_folder = container_folder,
            table = table,
            partitionBy = partitionBy,
            file_format = file_format
        )
    
    print('Finished Migrating All Tables')



iterate_over_all_tables(table_rows)



# %%


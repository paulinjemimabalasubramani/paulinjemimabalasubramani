"""
Create and Execute (if required) Snowflake DDL Steps and ingest_data for LR

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.spark_functions import create_spark
from modules.azure_functions import read_tableinfo, tableinfo_name
from modules.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables, create_source_level_tables, snowflake_ddl_params



# %% Parameters

tableinfo_source = 'LR'


# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark, tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source)


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables(tableinfo, table_rows)

# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


# %% Close Showflake connection

snowflake_connection.close()


# %%


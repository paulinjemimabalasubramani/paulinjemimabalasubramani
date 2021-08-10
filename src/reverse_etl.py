"""
Move curated data in Snowflake back to on prem SQL Server

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


https://docs.snowflake.com/en/user-guide/spark-connector.html

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



# %% Logging
logger = make_logging(__name__)


# %% Parameters

sql_server = 'DSQLOLTP02'


# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Read SQL Config

_, sql_id, sql_pass = get_azure_sp(sql_server.lower())


# %%






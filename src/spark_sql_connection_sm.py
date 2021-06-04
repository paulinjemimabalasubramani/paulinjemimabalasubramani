"""
Test Spark - SQL connection

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

from modules.common import make_logging, catch_error
from modules.mysession import MySession


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters

table = 'OLTP.Individual'
database='LR'
server='TSQLOLTP01'

storage_account_name = "agfsclakescd"
storage_account_access_key = "SGILPYErZL2RTSGN8/8fHjBLLlwS6ODMyRUIfts8F0p8UYqxcHxz97ujV9ym4RRCXPDUEoViRcCM8AxpLgsrbA=="
container_name = "ingress"
container_folder = "data/financial_professional/source/LR/OLTP"


# %% Main Body
if __name__ == '__main__':
    ss = MySession()

    df = ss.read_sql(table=table, database=database, server=server)

    df.printSchema()
    
    df = ss.to_string(df, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.

    ss.save_parquet_adls_gen2(df=df,
        storage_account_name = storage_account_name,
        storage_account_access_key = storage_account_access_key,
        container_name = container_name,
        container_folder = container_folder,
        table_name=table
    )
    
    #ss.spark.stop()
    print('Done')



# %%






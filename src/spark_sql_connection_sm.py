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


# %% Main Body
if __name__ == '__main__':
    ss = MySession()

    table = 'OLTP.Individual'

    df = ss.read_sql(table=table, database='LR', server='TSQLOLTP01')

    df.printSchema()

    # Convert timestamp's to string - as it cause errors otherwise.
    df = ss.to_string(df, col_types = [])

    print(os.path.realpath(os.path.dirname(__file__)))

    if ss.is_pc:
        data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared')
    else:
        # /usr/local/spark/resources/fileshare/Shared
        data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared')

    #os.makedirs(data_path_folder, exist_ok=True)
    data_path = os.path.join(data_path_folder, table+'.parquet')
    print(f'Data path: {data_path}')

    codec = ss.spark.conf.get("spark.sql.parquet.compression.codec")
    print(f"Write data in parquet format with '{codec}' compression")

    #df.write.parquet(path = data_path, mode='overwrite')
    
    #ss.spark.stop()
    print('Done')




# %% DEBUGGING AND RAW CODE:
# 'INFORMATION_SCHEMA.TABLES', 'INFORMATION_SCHEMA.COLUMNS'

# %% connect to Azure Blob

# https://luminousmen.com/post/azure-blob-storage-with-pyspark

storage_account_name = "agfsclakescd"
storage_account_access_key = "SGILPYErZL2RTSGN8/8fHjBLLlwS6ODMyRUIfts8F0p8UYqxcHxz97ujV9ym4RRCXPDUEoViRcCM8AxpLgsrbA=="
container_name = "ingress"
container_folder = "data/financial_professional/source/LR/OLTP"

ss.spark.conf.set(
    key = f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    value = storage_account_access_key
    )


# %%

data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"

df.write.parquet(path = data_path)


# %%






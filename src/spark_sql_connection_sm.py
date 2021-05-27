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

    df = ss.read_sql(table=table, database='LR')

    df.printSchema()

    # Convert timestamp's to string - as it cause errors otherwise.
    for col_name, col_type in df.dtypes:
        if col_type in ['timestamp']:
            print(f"Converting {col_name} from '{col_type}' to 'string' type")
            df = df.withColumn(col_name, col(col_name).cast('string'))

    print(os.path.realpath(os.path.dirname(__file__)))

    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared')
    #os.makedirs(data_path_folder, exist_ok=True)

    data_path = os.path.join(data_path_folder, table+'.parquet')
    print(f'Data path: {data_path}')

    codec = ss.spark.conf.get("spark.sql.parquet.compression.codec")
    print(f"Write data in parquet format with '{codec}' compression")

    df.write.parquet(path = data_path, mode='overwrite')
    
    print('Done')




# %% DEBUGGING AND RAW CODE:

# 'INFORMATION_SCHEMA.TABLES', 'INFORMATION_SCHEMA.COLUMNS'




# %%




# %%

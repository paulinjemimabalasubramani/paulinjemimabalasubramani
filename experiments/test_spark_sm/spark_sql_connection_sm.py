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

    df = ss.read_sql(table='OLTP.Individual', database='LR')

    df.printSchema()
    



# %% DEBUGGING AND RAW CODE:

# 'INFORMATION_SCHEMA.TABLES', 'INFORMATION_SCHEMA.COLUMNS'

# %% Print DF Info

print(f'Columns: {len(df.columns)}')
print(f'Rows: {df.count()}')
print(f'Partitions: {df.rdd.getNumPartitions()}')

# %% Show Table

print('Show Table')

x = df.select(col('EffDate').cast(StringType())).limit(5).collect()

print(x)


# %%




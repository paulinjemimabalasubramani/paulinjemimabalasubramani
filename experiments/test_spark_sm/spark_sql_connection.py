"""
Test Spark

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/

Download JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15


https://github.com/microsoft/sql-spark-connector


https://spark.apache.org/docs/latest/configuration


"""


# %% Import Libraries

from logging import exception
from modules.common import make_logging
from modules.config import Config

import os, sys, platform

import getpass


# %% Spark Module


from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window

from pyspark.sql import SparkSession


# %% Logging

logger = make_logging(__name__)

app_name = os.path.basename(__file__)
app_info = f'Running {app_name} on {platform.system()}'

print(app_info)
logger.info(app_info)

is_pc = platform.system().lower() == 'windows'

# %% Set Paths

if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    extraClassPath = r'C:\Users\smammadov\OneDrive - Advisor Group Inc\Desktop\EDIP-Code\drivers\mssql-jdbc-9.2.1.jre8.jar'

    secrets_file = r"C:\Users\smammadov\OneDrive - Advisor Group Inc\Desktop\EDIP-Code\config\secrets.yaml"

else:
    extraClassPath = ''
    secrets_file = ''


# %% Read Config Files

defaults = dict(
    user = "svc_ediprolr",
    password = "E0d!pr$L",
)

secrets = Config(file_path=secrets_file, defaults=defaults)

if not secrets.user:
    if not is_pc:
            raise ValueError('Username is missing')
    secrets.user = getpass.getpass('Enter username for SQL Server: ')

if not secrets.password:
    if not is_pc:
            raise ValueError('Password is missing')
    secrets.password = getpass.getpass('Enter password for SQL Server: ')



# %% Initiate a Spark Session

print('Initiate a Spark Session')

spark = (SparkSession
            .builder
            .appName(app_name)
            .config('spark.driver.extraClassPath', extraClassPath)
            .config('spark.executor.extraClassPath', extraClassPath)
            .getOrCreate()
)

sc = spark.sparkContext

spark.getActiveSession()


# %% Connect to the SQL database

print('Connect to the SQL database')

server = 'TSQLOLTP01'
database = 'LR'

#dbtable = 'INFORMATION_SCHEMA.TABLES'
dbtable = 'OLTP.Individual'

url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

df = (
  spark.read
       .format("jdbc")
#       .format("com.microsoft.sqlserver.jdbc.spark")
       .option("url", url)
       .option("driver", driver)
       .option("user", secrets.user)
       .option("password", secrets.password)
       .option("dbtable", dbtable)
       .option("encrypt", "true")
       .option("hostNameInCertificate", "*.database.windows.net")
       .load()
)


# %% Print DF Info
df.printSchema()

print(f'Columns: {len(df.columns)}')
print(f'Rows: {df.count()}')
print(f'Partitions: {df.rdd.getNumPartitions()}')

# %% Show Table

print('Show Table')

x = df.select(col('EffDate').cast(StringType())).limit(5).collect()

print(x)


# %% Stop Spark

spark.stop()

# %%




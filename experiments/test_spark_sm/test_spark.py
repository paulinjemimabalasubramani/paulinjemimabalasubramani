
"""
Test Spark

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/

Download JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15


https://github.com/microsoft/sql-spark-connector


"""


# %% Import Libraries

from modules.common import make_logging
from modules.config import Config, secrets_file

import os

from getpass import getpass

from pyspark.sql import SparkSession


# %% App Name

app_name = os.path.basename(__file__)
print(f'Running {app_name}')

# %% Logging

logger = make_logging(__name__)

# logger.info('Hello Info')


# %% Read Config Files

defaults = dict(
    user = "",
    password = None,
)

secrets = Config(file_path=secrets_file, defaults=defaults)

if not secrets.user:
    secrets.user = getpass('Enter username for SQL Server: ')

if not secrets.password:
    secrets.password = getpass('Enter password for SQL Server: ')


# %% Initiate a Spark Session

path_to_jdbc_jar = r'C:\Users\smammadov\OneDrive - Advisor Group Inc\Desktop\EDIP-Code\drivers\mssql-jdbc-9.2.1.jre8.jar'

spark_master = "spark://10.128.25.82:7077"

spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(spark_master) \
            .config('spark.driver.extraClassPath', path_to_jdbc_jar) \
            .config('spark.executor.extraClassPath', path_to_jdbc_jar) \
            .getOrCreate()

sc = spark.sparkContext

spark


# %% Connect to the SQL database

server = 'TSQLOLTP01'
database = 'LR'

#dbtable = 'INFORMATION_SCHEMA.TABLES'
dbtable = 'OLTP.Individual'

url = f'jdbc:sqlserver://{server};databaseName={database}'
driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'

df = (
  spark.read.format("jdbc")
       .option("url", url)
       .option("dbtable", dbtable)
       .option("user", secrets.user)
       .option("password", secrets.password)
       .option("driver", driver)
       .load()
)

# %%

df.limit(5).show()


# %% Release Spark Session - To be used at the end.


spark.stop()

# %%

# %%

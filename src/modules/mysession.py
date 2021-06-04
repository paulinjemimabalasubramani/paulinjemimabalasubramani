
"""
Library for starting a Spark session


USEFUL LINKS:
Download JDBC Driver:
https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-ver15

https://github.com/microsoft/sql-spark-connector

https://spark.apache.org/docs/latest/configuration

"""

# %% libraries
from .common import make_logging, catch_error
from .config import Config, secrets_file, is_pc, extraClassPath

import os, sys, platform
import getpass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# %% Logging
logger = make_logging(__name__)

# %% Driver Folder Path



# %% Main Class
class MySession():
    def __init__(self):
        self.app_name = os.path.basename(__file__)
        self.read_config()
        self.initiate_spark()


    @catch_error(logger)
    def read_config(self):
        """
        Read configuration files, username/passwords
        """
        defaults = dict(
            user = None,
            password = None,
        )

        self.secrets = Config(file_path=secrets_file, defaults=defaults)

        if not self.secrets.user:
            if not is_pc:
                    raise ValueError('Username is missing')
            self.secrets.user = getpass.getpass('Enter username for SQL Server: ')

        if not self.secrets.password:
            if not is_pc:
                    raise ValueError('Password is missing')
            self.secrets.password = getpass.getpass('Enter password for SQL Server: ')


    @catch_error(logger)
    def initiate_spark(self):
        """
        Initiate a new spark session
        """
        self.spark = (
            SparkSession
            .builder
            .appName(self.app_name)
            .config('spark.driver.extraClassPath', extraClassPath)
            .config('spark.executor.extraClassPath', extraClassPath)
            .getOrCreate()
            )

        self.sc = self.spark.sparkContext
        self.spark.getActiveSession()

        print(f"\nSpark version = {self.spark.version}")
        print(f"Hadoop version = {self.sc._jvm.org.apache.hadoop.util.VersionInfo.getVersion()}")



    @catch_error(logger)
    def read_sql(self, table:str, database:str='LR', server:str='TSQLOLTP01'):
        """
        Read a table from SQL server
        """
        print(f"Reading SQL: server='{server}', database='{database}', table='{table}'")

        url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

        df = (
            self.spark.read
                .format("jdbc")
                .option("url", url)
                .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
                .option("user", self.secrets.user)
                .option("password", self.secrets.password)
                .option("dbtable", table)
                .option("encrypt", "true")
                .option("hostNameInCertificate", "*.database.windows.net")
                .load()
            )
        
        return df


    @catch_error(logger)
    def read_xml(self, file_path:str, rowTag:str="?xml", schema=None):
        """
        Read XML Files using Spark
        """
        df = (self.spark.read
            .format("com.databricks.spark.xml")
            .option("rowTag", rowTag)
            .option("inferSchema", 'false')
            .option("excludeAttribute", 'false')
            .option("ignoreSurroundingSpaces", 'true')
            .option("mode", "PERMISSIVE")
        )

        if schema:
            df = df.schema(schema=schema)

        return df.load(file_path)


    @catch_error(logger)
    def stop_spark(self):
        """
        Stop Spark session
        """
        self.spark.stop()


    @catch_error(logger)
    def to_string(self, df, col_types=['timestamp']):
        """
        Convert timestamp's or other types to string - as it cause errors otherwise.
        """
        for col_name, col_type in df.dtypes:
            if not col_types or col_type in col_types:
                print(f"Converting {col_name} from '{col_type}' to 'string' type")
                df = df.withColumn(col_name, col(col_name).cast('string'))
        
        return df


    @catch_error(logger)
    def save_parquet_adls_gen2(self, df,
            storage_account_name:str,
            storage_account_access_key:str,
            container_name:str,
            container_folder:str,
            table_name:str):

        codec = self.spark.conf.get("spark.sql.parquet.compression.codec")
        print(f"Write {table_name} in parquet format with '{codec}' compression")

        self.spark.conf.set(
            key = f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
            value = storage_account_access_key
            )

        data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table_name}"

        df.write.parquet(path = data_path, mode='overwrite')
        print(f'Finished Writing {table_name}')



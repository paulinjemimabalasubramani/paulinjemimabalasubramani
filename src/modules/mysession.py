
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
from .config import Config, config_path

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
        self.get_env()
        self.set_paths()
        self.read_config()
        self.initiate_spark()


    @catch_error(logger)
    def get_env(self):
        """
        Get environment data, like, app name, os system etc.
        """
        self.app_name = os.path.basename(__file__)
        self.app_info = f'Running {self.app_name} on {platform.system()}'

        print(self.app_info)
        logger.info(self.app_info)

        self.is_pc = platform.system().lower() == 'windows'


    @catch_error(logger)
    def set_paths(self):
        """
        Set correct paths for Spark, config files, drivers, etc. based on system.
        """
        if self.is_pc:
            os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
            os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
            os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'

            sys.path.insert(0, '%SPARK_HOME%\bin')
            sys.path.insert(0, '%HADOOP_HOME%\bin')
            sys.path.insert(0, '%JAVA_HOME%\bin')

            self.drivers_path = os.path.realpath(os.path.dirname(__file__)+'/../../drivers')
            joinstr = ';' # for extraClassPath
        
        else:
            self.drivers_path = '/usr/local/spark/resources/fileshare/EDIP-Code/drivers'
            joinstr = ':' # for extraClassPath
        
        drivers = []
        for file in os.listdir(self.drivers_path):
            if file.endswith('.jar'):
                drivers.append(os.path.join(self.drivers_path, file))
        self.extraClassPath = joinstr.join(drivers)
        print(f'extraClassPath: {self.extraClassPath}')

        self.secrets_file = os.path.join(config_path, "secrets.yaml")


    @catch_error(logger)
    def read_config(self):
        """
        Read configuration files, username/passwords
        """
        defaults = dict(
            user = "svc_ediprolr",
            password = "E0d!pr$L",
        )

        self.secrets = Config(file_path=self.secrets_file, defaults=defaults)

        if not self.secrets.user:
            if not self.is_pc:
                    raise ValueError('Username is missing')
            self.secrets.user = getpass.getpass('Enter username for SQL Server: ')

        if not self.secrets.password:
            if not self.is_pc:
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
            .config('spark.driver.extraClassPath', self.extraClassPath)
            .config('spark.executor.extraClassPath', self.extraClassPath)
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



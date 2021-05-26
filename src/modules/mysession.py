
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
from .config import Config

import os, sys, platform
import getpass

from pyspark.sql import SparkSession



# %% Logging
logger = make_logging(__name__)


# %% Main Class
class MySession():
    def __init__(self):
        self.get_env()
        self.set_paths()
        self.read_config()
        self.initiate_spark()


    @catch_error(logger)
    def get_env(self):
        self.app_name = os.path.basename(__file__)
        self.app_info = f'Running {self.app_name} on {platform.system()}'

        print(self.app_info)
        logger.info(self.app_info)

        self.is_pc = platform.system().lower() == 'windows'


    @catch_error(logger)
    def set_paths(self):
        if self.is_pc:
            os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
            os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
            os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'

            sys.path.insert(0, '%SPARK_HOME%\bin')
            sys.path.insert(0, '%HADOOP_HOME%\bin')
            sys.path.insert(0, '%JAVA_HOME%\bin')

            self.extraClassPath = r'C:\Users\smammadov\OneDrive - Advisor Group Inc\Desktop\EDIP-Code\drivers\mssql-jdbc-9.2.1.jre8.jar'

            self.secrets_file = r"C:\Users\smammadov\OneDrive - Advisor Group Inc\Desktop\EDIP-Code\config\secrets.yaml"

        else:
            self.extraClassPath = ''
            self.secrets_file = ''


    @catch_error(logger)
    def read_config(self):
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


    @catch_error(logger)
    def read_sql(self, table:str, database:str='LR', server:str='TSQLOLTP01'):
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
    def stop_spark(self):
        self.spark.stop()



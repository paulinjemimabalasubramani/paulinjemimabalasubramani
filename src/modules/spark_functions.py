
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
from .config import Config, is_pc, extraClassPath, config_path

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


# %% Logging
logger = make_logging(__name__)

# %% Driver Folder Path




# %% Main Class
class MySparkSession():
    def __init__(self):
        self.app_name = os.path.basename(__file__)
        self.initiate_spark()
        self.read_sql_config()



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
    def read_sql_config(self):
        """
        Read sql configuration file, username/password
        """
        defaults = dict(
            sql_user = None,
            sql_password = None,
        )

        sql_file = os.path.join(config_path, "sql.yaml")
        self.sql_confing = Config(file_path=sql_file, defaults=defaults)



    def remove_column_spaces(self, df):
        """
        Removes spaces from column names
        """
        new_df = df.select([col(c).alias(c.replace(' ', '_')) for c in df.columns])
        return new_df


    @catch_error(logger)
    def read_sql(self, schema:str, table:str, database:str, server:str):
        """
        Read a table from SQL server
        """
        print(f"Reading SQL: server='{server}', database='{database}', table='{schema}.{table}'")

        url = f'jdbc:sqlserver://{server};databaseName={database};trustServerCertificate=true;'

        df = (
            self.spark.read
                .format("jdbc")
                .option("url", url)
                .option("driver", 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
                .option("user", self.sql_confing.sql_user)
                .option("password", self.sql_confing.sql_password)
                .option("dbtable", f"{schema}.{table}")
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


    """
    @catch_error(logger)
    def save_adls_gen2(self, 
            df,
            storage_account_name:str,
            storage_account_access_key:str,
            container_name:str,
            container_folder:str,
            table:str,
            partitionBy:str=None,
            format:str='delta'):

        data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"
        print(f"Write {format} -> {data_path}")

        self.spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

        df.write.save(path=data_path, format=format, mode='overwrite', partitionBy=partitionBy)
        print(f'Finished Writing {container_folder}/{table}')
    """


    @catch_error(logger)
    def save_adls_gen2_sp(self, 
            df,
            storage_account_name:str,
            azure_tenant_id:str,
            sp_id:str,
            sp_pass:str,
            container_name:str,
            container_folder:str,
            table:str,
            partitionBy:str=None,
            format:str='delta'):

        data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"
        print(f"Write {format} -> {data_path}")

        self.spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        self.spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        self.spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_pass)
        self.spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
        self.spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

        df.write.save(path=data_path, format=format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true")
        print(f'Finished Writing {container_folder}/{table}')



    def add_etl_columns(self, df, reception_date=None, execution_date=None, source:str=None):
        if reception_date:
            df = df.withColumn('RECEPTION_DATE', lit(str(reception_date)))
        
        if execution_date:
            df = df.withColumn('EXECUTION_DATE', lit(str(execution_date)))
        
        if source:
            df = df.withColumn('SOURCE', lit(str(source)))

        return df




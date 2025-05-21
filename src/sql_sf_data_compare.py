#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL Server to Snowflake Data Comparison using PySpark

This script compares data between SQL Server and Snowflake based on configured queries
in the metadata table DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_CONFIG.
"""
description = """

Add Bulk_id to Fixed Width Files

"""

# %% Parse Arguments

# It's better to keep the argument parsing separate and ensure sys is imported early
import argparse
import os
import sys
from datetime import datetime

# Initialize sys.app structure as per your original request
class app:
    pass
sys.app = app
# These will be populated after parse_arguments()
sys.app.args = {} 
sys.app.parent_name = os.path.basename(__file__)

parser = argparse.ArgumentParser(description=description)

parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)
parser.add_argument('--debug', action='store_true', help='Enable debug mode') # Moved here for consistency

args = parser.parse_args().__dict__
sys.app.args = args # Assign parsed args to sys.app.args

# %% Import Libraries
# Import existing modules
from modules3.snowflake_ddl import connect_to_snowflake
from modules3.common_functions import logger, catch_error, Connection # Assuming Connection is still needed for details
from modules3.spark_functions import (
    create_spark,             # To create SparkSession
    # read_sql,               # Not directly used as we use spark.read.jdbc
    # read_snowflake,         # Used, but passed the query as table_name with is_query=True
    remove_column_spaces,     # For Spark DataFrame column normalization
    trim_string_columns       # For Spark DataFrame string trimming
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, LongType


# %% Configuration Functions
@catch_error(logger)
def fetch_config_data(snowflake_conn):
    """Fetch configuration data from Snowflake metadata table."""
    try:
        cursor = snowflake_conn.cursor()
        query = """
        SELECT
            ID,
            SOURCE_DATA,
            SOURCE_DATA_SERVER,
            SOURCE_DATA_DATABASE,
            SOURCE_DATA_TABLE,
            SOURCE_DATA_QUERY,
            TARGET_DATABASE_SCHEMA,
            TARGET_TABLE,
            TARGET_QUERY,
            CREATED_BY,
            CREATED_TS,
            UPDATED_BY,
            UPDATED_TS
        FROM DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_CONFIG
        WHERE SOURCE_DATA_QUERY IS NOT NULL AND TARGET_QUERY IS NOT NULL
        """
        cursor.execute(query)

        # Convert to list of dictionaries for easier handling
        columns = [col_desc[0] for col_desc in cursor.description]
        config_data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        logger.info(f"Fetched {len(config_data)} configuration items from metadata table")
        cursor.close()
        return config_data
    except Exception as e:
        error_msg = f"Error fetching configuration data: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

# %% Data Processing Functions
@catch_error(logger)
def execute_sql_server_query(spark: SparkSession, connection: Connection, query: str):
    """Execute query on SQL Server and return results as Spark DataFrame."""
    try:
        # Use the modified read_sql function that accepts a query parameter
        server = connection.server
        database = connection.database
        
        # Create JDBC URL and options for direct query
        # Using integratedSecurity=true for trusted connection in JDBC if applicable
        # Or ensure you have username/password options if not trusted connection
        url = f'jdbc:sqlserver://{server};databaseName={database};integratedSecurity=true;encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;'
        
        # Execute query using Spark's JDBC connector
        df = spark.read.format("jdbc") \
            .option("url", url) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("query", query) \
            .load()
        
        # Normalize column names (convert to lowercase and remove spaces)
        df = remove_column_spaces(df)
        df = trim_string_columns(df) # Trim string columns in Spark DataFrame
            
        logger.info(f"SQL Server query executed successfully, retrieved {df.count()} rows")
        return df
    except Exception as e:
        error_msg = f"Error executing SQL Server query: {str(e)}\nQuery: {query}"
        logger.error(error_msg)
        raise Exception(error_msg)

@catch_error(logger)
def execute_snowflake_query(spark: SparkSession, snowflake_conn, query: str):
    """Execute query on Snowflake and return results as Spark DataFrame."""
    try:
        # Extract Snowflake connection parameters
        sf_account = snowflake_conn.account
        sf_user = snowflake_conn.user
        sf_warehouse = snowflake_conn.warehouse
        sf_database = snowflake_conn.database
        sf_schema = snowflake_conn.schema
        sf_role = snowflake_conn.role
        sf_password = snowflake_conn.password # Assuming password is part of snowflake_conn object

        # Use the read_snowflake function, passing the query directly
        # You need to ensure your read_snowflake can handle a direct SQL query by setting is_query=True
        df = spark.read.format("snowflake") \
            .option("sfURL", f"{sf_account}.snowflakecomputing.com") \
            .option("sfUser", sf_user) \
            .option("sfPassword", sf_password) \
            .option("sfWarehouse", sf_warehouse) \
            .option("sfDatabase", sf_database) \
            .option("sfSchema", sf_schema) \
            .option("sfRole", sf_role) \
            .option("query", query) \
            .load()
        
        # Normalize column names (convert to lowercase and remove spaces)
        df = remove_column_spaces(df)
        df = trim_string_columns(df) # Trim string columns in Spark DataFrame
            
        logger.info(f"Snowflake query executed successfully, retrieved {df.count()} rows")
        return df
    except Exception as e:
        error_msg = f"Error executing Snowflake query: {str(e)}\nQuery: {query}"
        logger.error(error_msg)
        raise Exception(error_msg)

@catch_error(logger)
def compare_dataframes_spark(sql_df, sf_df):
    """Compare two Spark dataframes and return comparison results."""
    try:
        # Check if column counts match
        if len(sql_df.columns) != len(sf_df.columns):
            return False, f"Column count mismatch: SQL Server: {len(sql_df.columns)}, Snowflake: {len(sf_df.columns)}"
            
        # Column names should already be lowercase and without spaces from the execute_query functions
        # Sort columns alphabetically to ensure consistent comparison
        sql_cols = sorted(sql_df.columns)
        sf_cols = sorted(sf_df.columns)
        
        # Check if column names match
        if sql_cols != sf_cols:
            return False, f"Column names don't match: SQL columns: {sql_cols}, SF columns: {sf_cols}"
            
        # Select columns in the same order for both DataFrames
        sql_df_ordered = sql_df.select(*sql_cols)
        sf_df_ordered = sf_df.select(*sf_cols) # Use sf_cols here, but it should be identical to sql_cols if previous check passed
        
        # Check if row counts match
        sql_count = sql_df_ordered.count()
        sf_count = sf_df_ordered.count()
        if sql_count != sf_count:
            return False, f"Row count mismatch: SQL Server: {sql_count}, Snowflake: {sf_count}"
        
        # Compare data by using exceptAll operations to find differences
        # Rows in SQL Server but not in Snowflake
        sql_except_sf = sql_df_ordered.exceptAll(sf_df_ordered)
        # Rows in Snowflake but not in SQL Server
        sf_except_sql = sf_df_ordered.exceptAll(sql_df_ordered)
        
        sql_diff_count = sql_except_sf.count()
        sf_diff_count = sf_except_sql.count()
        
        total_diff_count = sql_diff_count + sf_diff_count
        
        if total_diff_count > 0:
            diff_examples = []
            
            # Get samples of differences for reporting (limit to avoid large data transfer)
            if sql_diff_count > 0:
                sql_samples = sql_except_sf.limit(5).collect() # Collect to driver for logging
                for row in sql_samples:
                    diff_examples.append(f"Row in SQL Server but not in Snowflake: {row.asDict()}")
            
            if sf_diff_count > 0:
                sf_samples = sf_except_sql.limit(5).collect() # Collect to driver for logging
                for row in sf_samples:
                    diff_examples.append(f"Row in Snowflake but not in SQL Server: {row.asDict()}")
                        
            # Limit examples to prevent excessively long messages
            message_prefix = f"Data mismatch: {total_diff_count} rows differ between sources."
            if len(diff_examples) > 10:
                return False, f"{message_prefix} First 10 examples:\n" + "\n".join(diff_examples[:10])
            else:
                return False, f"{message_prefix}\n" + "\n".join(diff_examples)
            
        return True, "Data matches exactly between SQL Server and Snowflake"
    except Exception as e:
        error_msg = f"Error comparing dataframes: {str(e)}"
        logger.error(error_msg)
        return False, f"Error during comparison: {str(e)}"

# %% Result Storage Functions
@catch_error(logger)
def store_results(spark: SparkSession, snowflake_conn, config_item, match_status, comparison_message):
    """Store comparison results in Snowflake output table using Spark."""
    try:
        # Create output table if it doesn't exist using the direct connection
        # This is more robust as Spark write might not create full DDL with specific types/constraints
        cursor = snowflake_conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_RESULTS (
            COMPARISON_ID NUMBER,
            CONFIG_ID NUMBER,
            SOURCE_DATA VARCHAR(50),
            SOURCE_DATA_SERVER VARCHAR(50),
            SOURCE_DATA_DATABASE VARCHAR(50),
            SOURCE_DATA_TABLE VARCHAR(100),
            TARGET_DATABASE_SCHEMA VARCHAR(50),
            TARGET_TABLE VARCHAR(50),
            MATCH_STATUS BOOLEAN,
            COMPARISON_MESSAGE VARCHAR(16777216),
            COMPARISON_TIMESTAMP TIMESTAMP_LTZ
        )
        """
        cursor.execute(create_table_query)
        
        # Get the next comparison ID using the direct connection
        cursor.execute("SELECT COALESCE(MAX(COMPARISON_ID), 0) + 1 FROM DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_RESULTS")
        comparison_id = cursor.fetchone()[0]
        
        # Extract Snowflake connection parameters for Spark write
        sf_account = snowflake_conn.account
        sf_user = snowflake_conn.user
        sf_password = snowflake_conn.password
        sf_warehouse = snowflake_conn.warehouse
        
        # Hardcode database and schema for the results table as they are fixed
        sf_database = "DATAHUB"
        sf_schema = "PIPELINE_METADATA"
        sf_role = snowflake_conn.role # Assuming role is part of snowflake_conn
        
        # Create a Spark DataFrame with the result data
        # Define schema explicitly to ensure correct types for Snowflake write
        results_schema = StructType([
            StructField("COMPARISON_ID", LongType(), True),
            StructField("CONFIG_ID", LongType(), True),
            StructField("SOURCE_DATA", StringType(), True),
            StructField("SOURCE_DATA_SERVER", StringType(), True),
            StructField("SOURCE_DATA_DATABASE", StringType(), True),
            StructField("SOURCE_DATA_TABLE", StringType(), True),
            StructField("TARGET_DATABASE_SCHEMA", StringType(), True),
            StructField("TARGET_TABLE", StringType(), True),
            StructField("MATCH_STATUS", BooleanType(), True),
            StructField("COMPARISON_MESSAGE", StringType(), True),
            StructField("COMPARISON_TIMESTAMP", TimestampType(), True)
        ])

        result_data_row = (
            comparison_id,
            int(config_item['ID']), # Ensure ID is integer
            config_item['SOURCE_DATA'],
            config_item['SOURCE_DATA_SERVER'],
            config_item['SOURCE_DATA_DATABASE'],
            config_item['SOURCE_DATA_TABLE'],
            config_item['TARGET_DATABASE_SCHEMA'],
            config_item['TARGET_TABLE'],
            match_status,
            comparison_message,
            datetime.now() # Use current datetime
        )
        
        result_df = spark.createDataFrame([result_data_row], schema=results_schema)
        
        # Write to Snowflake using Spark Connector
        result_df.write \
            .format("snowflake") \
            .option("sfURL", f"{sf_account}.snowflakecomputing.com") \
            .option("sfUser", sf_user) \
            .option("sfPassword", sf_password) \
            .option("sfWarehouse", sf_warehouse) \
            .option("sfDatabase", sf_database) \
            .option("sfSchema", sf_schema) \
            .option("sfRole", sf_role) \
            .option("dbtable", "SQL_SNOWFLAKE_DATA_COMPARE_RESULTS") \
            .mode("append") \
            .save()
        
        logger.info(f"Stored comparison results for config ID {config_item['ID']}")
        cursor.close()
        
        return comparison_id
    except Exception as e:
        error_msg = f"Error storing results in Snowflake: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


# %% Main Execution Function
@catch_error(logger)
def run_data_comparison():
    """Main function to run data comparison between SQL Server and Snowflake."""
    spark = None
    snowflake_conn = None
    sql_connections = {} # Still keep track of connection objects if needed

    try:
        # Step 1: Initialize Spark Session using the imported function
        spark = create_spark()
        
        # Step 2: Get Snowflake connection using the imported module
        snowflake_conn = connect_to_snowflake()

        # Step 3: Get configuration data 
        config_data = fetch_config_data(snowflake_conn)

        for config_item in config_data:
            try:
                logger.info(f"Processing comparison config ID: {config_item['ID']}")

                # Get SQL Server connection details (Connection object just holds parameters, not the actual pyodbc connection)
                server = config_item['SOURCE_DATA_SERVER']
                database = config_item['SOURCE_DATA_DATABASE']
                
                # Create a Connection object to pass server/database details to execute_sql_server_query
                # We don't need to actually establish a pyodbc connection here anymore
                sql_conn_obj = Connection(
                    driver='{ODBC Driver 17 for SQL Server}',
                    server=server,
                    database=database,
                    trusted_connection=True # Assuming trusted connection
                )

                # Step 4: Execute queries using Spark
                sql_query = config_item['SOURCE_DATA_QUERY']
                sf_query = config_item['TARGET_QUERY']

                logger.info(f"Executing SQL Server query for {server}.{database}.{config_item['SOURCE_DATA_TABLE']}")
                sql_result = execute_sql_server_query(spark, sql_conn_obj, sql_query)

                logger.info(f"Executing Snowflake query for {config_item['TARGET_DATABASE_SCHEMA']}.{config_item['TARGET_TABLE']}")
                sf_result = execute_snowflake_query(spark, snowflake_conn, sf_query)

                # Step 5: Compare results
                match_status, comparison_message = compare_dataframes_spark(sql_result, sf_result)
                logger.info(f"Comparison result: {'Match' if match_status else 'Mismatch'}")

                # Step 6: Store results
                comparison_id = store_results(spark, snowflake_conn, config_item, match_status, comparison_message)

            except Exception as e:
                logger.error(f"Error processing config ID {config_item['ID']}: {str(e)}")

                # Store error in results table
                try:
                    if snowflake_conn and spark:
                        # Ensure comparison_message isn't too long for VARCHAR(16777216)
                        truncated_message = (f"Error during comparison: {str(e)}")[:16777216]
                        store_results(
                            spark,
                            snowflake_conn,
                            config_item,
                            False,
                            truncated_message
                        )
                except Exception as store_err:
                    logger.error(f"Error storing failure results: {str(store_err)}")

    except Exception as e:
        error_msg = f"Error in run_data_comparison: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    finally:
        # Close Snowflake connection
        if snowflake_conn:
            try:
                snowflake_conn.close()
                logger.info("Closed Snowflake connection")
            except Exception as e:
                logger.warning(f"Error closing Snowflake connection: {str(e)}")
                
        # Stop Spark session
        if spark:
            try:
                spark.stop()
                logger.info("Stopped Spark session")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {str(e)}")

        logger.info("Data comparison job completed")

# %% Main Execution
if __name__ == "__main__":
    # The argparse moved to the top, so `args` is already populated.
    # Set debug level if requested
    if args.get('debug'): # Access from the global args dictionary
        logger.setLevel("DEBUG")
        logger.debug("Debug mode enabled")

    # Run the data comparison
    run_data_comparison()
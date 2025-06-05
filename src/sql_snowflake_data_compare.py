description = """

Comparison between sql server data and snowflake data

"""

# %% Parse Arguments

if True:  # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'SQL_SNOWFLAKE_DATA_COMPARE',
        'source_path': r'C:\myworkdir\Shared\SABOS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\Sabos\Sabos_Schema.csv',
    }

import os, pyodbc, argparse, sys, json, decimal
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, LongType, DoubleType, DecimalType, BooleanType, IntegerType, BinaryType, ShortType, FloatType

PYODBC_TYPE_MAP = {
    pyodbc.SQL_CHAR: StringType(),
    pyodbc.SQL_VARCHAR: StringType(),
    pyodbc.SQL_WCHAR: StringType(),
    pyodbc.SQL_WVARCHAR: StringType(),
    pyodbc.SQL_LONGVARCHAR: StringType(), 
    pyodbc.SQL_WLONGVARCHAR: StringType(),

    pyodbc.SQL_DECIMAL: DecimalType(38, 18),  # Adjust precision/scale as needed
    pyodbc.SQL_NUMERIC: DecimalType(38, 18),
    pyodbc.SQL_SMALLINT: ShortType(),
    pyodbc.SQL_INTEGER: IntegerType(),
    pyodbc.SQL_REAL: FloatType(),
    pyodbc.SQL_FLOAT: DoubleType(),
    pyodbc.SQL_DOUBLE: DoubleType(),
    pyodbc.SQL_BIGINT: LongType(),

    pyodbc.SQL_BIT: BooleanType(),

    pyodbc.SQL_TYPE_DATE: DateType(),
    pyodbc.SQL_TYPE_TIMESTAMP: TimestampType(),

    pyodbc.SQL_BINARY: BinaryType(),
    pyodbc.SQL_VARBINARY: BinaryType(),
    pyodbc.SQL_LONGVARBINARY: BinaryType(),
}


class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)


from modules3.common_functions import logger, data_settings, catch_error, Connection, KeyVaultList, is_pc
from modules3.spark_functions import create_spark
from modules3.snowflake_ddl import connect_to_snowflake


# %% Configuration Functions
@catch_error(logger)
def fetch_config_data(snowflake_conn):
    """Fetch configuration data from Snowflake metadata table."""
    try:
        cursor = snowflake_conn.cursor()
        query = """
        SELECT
            ID,
            SOURCE_DATA_SERVER,
            SOURCE_DATABASE,
            SOURCE_SCHEMA,
            SOURCE_TABLE,
            SOURCE_QUERY,
            TARGET_DATABASE,
            TARGET_SCHEMA,
            TARGET_TABLE,
            TARGET_QUERY,
            KEY_COLUMNS
        FROM DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_CONFIG
        WHERE ACTIVE_FLAG = 'Y'
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
        logger.logger.error(error_msg, exc_info=True)
        raise Exception(error_msg)

@catch_error(logger)
def establish_sql_connections(config_data):
    """
    Establish SQL Server connections for each unique database and server combination.
    Args:
        config_data (list): List of configuration dictionaries
    Returns:
        dict: Dictionary with connection keys and Connection objects
    """
    sql_connections = {}
    unique_combinations = set()

    # Extract unique server-database combinations
    for config in config_data:
        server = config.get('SOURCE_DATA_SERVER')
        database = config.get('SOURCE_DATABASE')

        if server and database:
            unique_combinations.add((server, database))

    logger.info(f"Found {len(unique_combinations)} unique server-database combinations")

    # Establish connections for each unique combination
    for server, database in unique_combinations:
        try:
            connection_key = f"{server}_{database}"
            logger.info(f"Establishing connection for {server} - {database}")

            # Prepare the Connection object that contains the connection string details
            connection_details = prepare_sql_connection(server, database)
           
            # Establish the actual pyodbc connection and store it
            pyodbc_conn = pyodbc.connect(connection_details.get_connection_str_sql())
            sql_connections[connection_key] = pyodbc_conn

            logger.info(f"Successfully established live pyodbc connection: {connection_key}")

        except Exception as e:
            error_msg = f"Failed to establish connection for {server} - {database}: {str(e)}"
            logger.logger.error(error_msg, exc_info=True)
            continue

    logger.info(f"Successfully established {len(sql_connections)} SQL Server connections")
    return sql_connections


@catch_error(logger)
# Getting connection based on sql server and database
def prepare_sql_connection(server_name: str, database_name: str):
    connection = Connection(
        driver=data_settings.sql_driver,
        server=server_name,
        database=database_name,
        key_vault_name=data_settings.key_vault_name,
        trusted_connection=is_pc,
        key_vault=key_vault,
    )
    return connection


@catch_error(logger)
def get_connection_for_config(config_item, sql_connections):
    """Get the appropriate SQL connection for a specific configuration item."""
    server = config_item.get('SOURCE_DATA_SERVER')
    database = config_item.get('SOURCE_DATABASE')

    if server and database:
        connection_key = f"{server}_{database}"
        return sql_connections.get(connection_key)

    return None


@catch_error(logger)
def execute_sql_query_with_spark(spark, pyodbc_conn_obj, query):
    """Execute SQL query using pyodbc and convert to Spark DataFrame."""
    
    try:
        if not pyodbc_conn_obj:
            return None, "No active pyodbc connection provided."

        cursor = pyodbc_conn_obj.cursor()
        cursor.execute(query)

        columns = [col_desc[0] for col_desc in cursor.description] #col_desc[0] - column names
        data_types_pyodbc = [col_desc[1] for col_desc in cursor.description] #col_desc[1] - column data types

        spark_fields = []
        for i, col_name in enumerate(columns):
            spark_type = PYODBC_TYPE_MAP.get(data_types_pyodbc[i], StringType())
            spark_fields.append(StructField(col_name, spark_type, True))

        schema = StructType(spark_fields)

        # Fetch all data as pyodbc.Row objects
        raw_data = cursor.fetchall()
        cursor.close()

        # IMPORTANT: Convert pyodbc.Row objects to tuples AND perform type conversions
        processed_data = []
        for row in raw_data:
            new_row = []
            for i, value in enumerate(row):
                # Get the expected Spark type for this column from our schema
                expected_spark_type = schema.fields[i].dataType

                # Perform type conversion if necessary
                if value is None:
                    new_row.append(None)
                elif isinstance(expected_spark_type, StringType):
                    new_row.append(str(value) if value is not None else None)
                elif isinstance(expected_spark_type, IntegerType):
                    # Attempt conversion to int, handle potential float/decimal as string
                    try:
                        new_row.append(int(value))
                    except (ValueError, TypeError):
                        # Handle cases where int comes as float (e.g., 21.0) or string
                        if isinstance(value, (float, decimal.Decimal)):
                            new_row.append(int(value))
                        elif isinstance(value, str):
                            new_row.append(int(float(value))) # Try converting string to float then int
                        else:
                            new_row.append(None) # Or raise error
                elif isinstance(expected_spark_type, LongType): # For BIGINT
                    try:
                        new_row.append(int(value)) # Python int handles arbitrary size
                    except (ValueError, TypeError):
                           if isinstance(value, (float, decimal.Decimal)):
                               new_row.append(int(value))
                           elif isinstance(value, str):
                               new_row.append(int(float(value)))
                           else:
                               new_row.append(None)
                elif isinstance(expected_spark_type, ShortType): # For SMALLINT
                    try:
                        new_row.append(int(value)) # Python int handles arbitrary size
                    except (ValueError, TypeError):
                           if isinstance(value, (float, decimal.Decimal)):
                               new_row.append(int(value))
                           elif isinstance(value, str):
                               new_row.append(int(float(value)))
                           else:
                               new_row.append(None)
                elif isinstance(expected_spark_type, FloatType): # For REAL
                    try:
                        new_row.append(float(value))
                    except (ValueError, TypeError):
                        new_row.append(None)
                elif isinstance(expected_spark_type, DoubleType):
                    try:
                        new_row.append(float(value))
                    except (ValueError, TypeError):
                        new_row.append(None)
                elif isinstance(expected_spark_type, DecimalType):
                    try:
                        # Convert to Python's decimal.Decimal type for Spark DecimalType
                        new_row.append(decimal.Decimal(str(value))) # Important to pass as string
                    except (decimal.InvalidOperation, TypeError):
                        new_row.append(None) # Handle conversion errors
                elif isinstance(expected_spark_type, DateType):
                    # pyodbc often returns datetime.date or datetime.datetime for dates
                    # If it's a string like '20200924', you might need custom parsing
                    if isinstance(value, (datetime.date, datetime.datetime)):
                        new_row.append(value)
                    elif isinstance(value, str):
                        # Attempt to parse common date string formats
                        try:
                            # Example: 'YYYYMMDD'
                            new_row.append(datetime.strptime(value, '%Y%m%d').date())
                        except ValueError:
                            # Example: 'YYYY-MM-DD'
                            try:
                                new_row.append(datetime.strptime(value, '%Y-%m-%d').date())
                            except ValueError:
                                new_row.append(None) # Could not parse
                    else:
                        new_row.append(None)
                elif isinstance(expected_spark_type, TimestampType):
                    # pyodbc typically returns datetime.datetime for timestamps
                    if isinstance(value, datetime):
                        new_row.append(value)
                    elif isinstance(value, str):
                        # Attempt to parse common timestamp string formats
                        try:
                            new_row.append(datetime.fromisoformat(value)) # For ISO format
                        except ValueError:
                            try:
                                new_row.append(datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')) # Common SQL Server format
                            except ValueError:
                                new_row.append(None)
                    else:
                        new_row.append(None)
                elif isinstance(expected_spark_type, BooleanType):
                    # Convert to Python bool
                    if value is True or value == 1:
                        new_row.append(True)
                    elif value is False or value == 0:
                        new_row.append(False)
                    else:
                        new_row.append(None) # Or handle other representations
                elif isinstance(expected_spark_type, BinaryType):
                    new_row.append(bytes(value) if value is not None else None)
                else:
                    # Default fallback: append value as is, or convert to string
                    new_row.append(value)

            processed_data.append(tuple(new_row)) # Convert list to tuple

        # Create DataFrame with the explicit schema and processed data
        df = spark.createDataFrame(processed_data, schema)

        for c in df.columns:
            df = df.withColumnRenamed(c, c.lower())
        logger.info(f"Source columns: {df.columns}") # Update this log to reflect new names
        return df, None

    except Exception as e:
        error_msg = f"Error executing SQL query with pyodbc/Spark: {str(e)}"
        logger.logger.error(error_msg, exc_info=True)
        return None, error_msg


@catch_error(logger)
def execute_snowflake_query_with_spark(spark, snowflake_conn, query):
    """Execute Snowflake query and convert to Spark DataFrame."""
    try:
        cursor = snowflake_conn.cursor()
        cursor.execute(query)

        # Get column names and data
        columns = [col_desc[0] for col_desc in cursor.description]
        data = cursor.fetchall()
        cursor.close()

        # Create Spark DataFrame from the data
        if data:
            df = spark.createDataFrame(data, columns)
            for c in df.columns:
                df = df.withColumnRenamed(c, c.lower())
            logger.info(f"Target columns: {df.columns}") # Update this log to reflect new names
            return df, None
        else:
            # Create empty DataFrame with proper schema
            # Infer schema from columns if no data, default to StringType
            schema = StructType([StructField(col_name, StringType(), True) for col_name in columns])
            df = spark.createDataFrame([], schema)

        logger.info(f"Snowflake query executed successfully with Spark, returned {df.count()} rows")
        return df, None

    except Exception as e:
        error_msg = f"Error executing Snowflake query with Spark: {str(e)}"
        logger.logger.error(error_msg, exc_info=True)
        return None, error_msg


@catch_error(logger)
def compare_spark_dataframes(spark, sql_df, snowflake_df, key_columns):
    """
    Compare two Spark DataFrames and return detailed comparison results.
    Args:
        spark (SparkSession): The SparkSession object.
        sql_df (DataFrame): The DataFrame from SQL Server.
        snowflake_df (DataFrame): The DataFrame from Snowflake.
        key_columns (list): A list of column names to use as key for comparison.
    Returns:
        dict: A dictionary containing all comparison results for the 'comparison_result' column.
    """
    comparison_results = {
        "source_count": 0,
        "target_count": 0,
        "source_query_result": [],
        "target_query_result": [],
        "data_not_in_source": [],
        "data_not_in_target": [],
        "mismatch_column_values": [],
        "comparison_summary_message": "Comparison initiated."
    }
    comparison_message = "Comparison initiated."

    try:
        if sql_df is None and snowflake_df is None:
            comparison_results["comparison_summary_message"] = "Both queries failed to execute."
            return comparison_results
        if sql_df is None:
            comparison_results["target_count"] = snowflake_df.count() if snowflake_df else 0
            comparison_results["comparison_summary_message"] = "SQL query failed to execute."
            try:
                comparison_results["target_query_result"] = [row.asDict() for row in sql_df.collect()] if snowflake_df else []
            except Exception as e:
                logger.warning(f"Failed to collect target_df (on SQL query failure): {e}")
                comparison_results["target_query_result"] = [{"error": str(e), "message": "Failed to collect target DataFrame"}]
            return comparison_results
        if snowflake_df is None:
            comparison_results["source_count"] = sql_df.count() if sql_df else 0
            comparison_results["comparison_summary_message"] = "Snowflake query failed to execute."
            try:
                comparison_results["source_query_result"] = [row.asDict() for row in sql_df.collect()] if sql_df else []
            except Exception as e:
                logger.warning(f"Failed to collect source_df (on Snowflake query failure): {e}")
                comparison_results["source_query_result"] = [{"error": str(e), "message": "Failed to collect source DataFrame"}]
            return comparison_results

        comparison_results["source_count"] = sql_df.count()
        comparison_results["target_count"] = snowflake_df.count()

        try:
            comparison_results["source_query_result"] = [row.asDict() for row in sql_df.collect()]
        except Exception as e:
            logger.warning(f"Failed to collect source_df: {e}")
            comparison_results["source_query_result"] = [{"error": str(e), "message": "Failed to collect source DataFrame"}]
        try:
            comparison_results["target_query_result"] = [row.asDict() for row in snowflake_df.collect()]
        except Exception as e:
            logger.warning(f"Failed to collect target_df: {e}")
            comparison_results["target_query_result"] = [{"error": str(e), "message": "Failed to collect target DataFrame"}]

        # Ensure key_columns are present in both DFs and are lowercased
        key_columns_lower = [col.lower() for col in key_columns]
        for key_col in key_columns_lower:
            if key_col not in sql_df.columns or key_col not in snowflake_df.columns:
                error_msg = f"Key column '{key_col}' not found in both source and target DataFrames. Cannot perform detailed mismatch comparison."
                logger.logger.error(error_msg)
                comparison_results["mismatch_column_values"] = [{"error": error_msg}]
                comparison_results["comparison_summary_message"] = "Key column missing."
                return comparison_results

        # Find rows in source but not in target by key_column(DATA_NOT_IN_TARGET)
        sql_df_keys = sql_df.select(*key_columns_lower)
        snowflake_df_keys = snowflake_df.select(*key_columns_lower)

        # Rows in source whose keys are not in target
        data_not_in_target_df = sql_df_keys.exceptAll(snowflake_df_keys)
        if data_not_in_target_df.count() > 0:
            comparison_message = f"Data mismatch: {data_not_in_target_df.count()} rows only in source (based on keys)."
            try:
                # Collect full rows for context, not just keys, so join back
                data_not_in_target_full_df = sql_df.join(data_not_in_target_df, on=key_columns_lower, how="inner")
                comparison_results["data_not_in_target"] = [row.asDict() for row in data_not_in_target_full_df.collect()]
            except Exception as e:
                logger.warning(f"Failed to collect data_not_in_target_df: {e}")
                comparison_results["data_not_in_target"] = [{"error": str(e), "message": "Failed to collect data_not_in_target DataFrame"}]

        # Find rows in target but not in source (DATA_NOT_IN_SOURCE) based on KEY COLUMNS
        # Rows in target whose keys are not in source
        data_not_in_source_df = snowflake_df_keys.exceptAll(sql_df_keys)
        if data_not_in_source_df.count() > 0:
            if comparison_message == "Comparison initiated.":
                comparison_message = f"Data mismatch: {data_not_in_source_df.count()} rows only in target (based on keys)."
            else:
                comparison_message = f"{comparison_message} {data_not_in_source_df.count()} rows only in target (based on keys)."
            try:
                # Collect full rows for context, not just keys, so join back
                data_not_in_source_full_df = snowflake_df.join(data_not_in_source_df, on=key_columns_lower, how="inner")
                comparison_results["data_not_in_source"] = [row.asDict() for row in data_not_in_source_full_df.collect()]
            except Exception as e:
                logger.warning(f"Failed to collect data_not_in_source_df: {e}")
                comparison_results["data_not_in_source"] = [{"error": str(e), "message": "Failed to collect data_not_in_source DataFrame"}]


        # --- Identify Column-Level Mismatches for existing rows ---
        # Join the dataframes on key columns to find rows that exist in both
        # but might have differing values in other columns.
        if key_columns_lower:
            joined_df = sql_df.alias("source").join(
                snowflake_df.alias("target"),
                on=key_columns_lower,
                how="inner"
            )

            common_columns_all = list(set(sql_df.columns) & set(snowflake_df.columns))
            non_key_common_columns = [c for c in common_columns_all if c not in key_columns_lower]

            # Use a dictionary to group mismatches by their key_columns values
            grouped_mismatches = {}

            for col_name in non_key_common_columns:
                mismatched_rows_df = joined_df.filter(
                    (col(f"source.{col_name}").isNotNull() & col(f"target.{col_name}").isNull()) |
                    (col(f"source.{col_name}").isNull() & col(f"target.{col_name}").isNotNull()) |
                    (col(f"source.{col_name}") != col(f"target.{col_name}"))
                ).select(
                    *[col(f"source.{k_col}").alias(k_col) for k_col in key_columns_lower],
                    col(f"source.{col_name}").alias(f"{col_name}_source"),
                    col(f"target.{col_name}").alias(f"{col_name}_target")
                )

                if mismatched_rows_df.count() > 0:
                    logger.info(f"Mismatch found in column '{col_name}'. Collecting details...")
                    rows_with_mismatch = [row.asDict() for row in mismatched_rows_df.collect()]

                    for row_dict in rows_with_mismatch:
                        # Construct the key for grouping. If multiple key columns, use a tuple of values.
                        # If a single key column, just use its value directly as per your example.
                        if len(key_columns_lower) == 1:
                            key_value = row_dict.get(key_columns_lower[0])
                            grouping_key = key_value # Use the single key's value directly
                            key_columns_dict_for_output = {key_columns_lower[0]: key_value}
                        else:
                            key_value = tuple(row_dict.get(k) for k in key_columns_lower)
                            grouping_key = key_value # Use a tuple for the dictionary key
                            key_columns_dict_for_output = {k: row_dict.get(k) for k in key_columns_lower}


                        if grouping_key not in grouped_mismatches:
                            grouped_mismatches[grouping_key] = {
                                "key_columns": key_columns_dict_for_output,
                                "mismatches": []
                            }
                            # If there's only one key column, flat-pack it as requested
                            if len(key_columns_lower) == 1:
                                grouped_mismatches[grouping_key] = {
                                    "key_columns": key_columns_lower[0], # The column name
                                    "key_value": key_value,            # The actual value
                                    "mismatches": []
                                }

                        grouped_mismatches[grouping_key]["mismatches"].append({
                            "column_name": col_name,
                            "source_value": row_dict.get(f"{col_name}_source"),
                            "target_value": row_dict.get(f"{col_name}_target")
                        })
           
            # Convert the grouped mismatches dictionary to the final list format
            comparison_results["mismatch_column_values"] = list(grouped_mismatches.values())
        else:
            logger.warning("No key columns provided. Skipping detailed column-level mismatch comparison.")


        if comparison_results["source_count"] == comparison_results["target_count"] and \
           not comparison_results["data_not_in_source"] and \
           not comparison_results["data_not_in_target"] and \
           not comparison_results["mismatch_column_values"]:
            comparison_results["comparison_summary_message"] = f"Data matches perfectly: {comparison_results['source_count']} rows."
        else:
            # If there are any differences detected
            if not comparison_results["comparison_summary_message"].startswith("Data mismatch:"):
                comparison_results["comparison_summary_message"] = "Data differences detected (counts or row-level mismatches)."
            if comparison_results["mismatch_column_values"]:
                comparison_results["comparison_summary_message"] += " Column-level value mismatches detected."


        return comparison_results

    except Exception as e:
        error_msg = f"Error during Spark DataFrame comparison: {str(e)}"
        logger.logger.error(error_msg, exc_info=True)
        comparison_results["mismatch_column_values"] = [{"error": error_msg, "message": "Comparison failed during processing"}]
        comparison_results["comparison_summary_message"] = error_msg
        return comparison_results


@catch_error(logger)
def store_comparison_result(
    cursor,
    config_id,
    comparison_data_dict 
):
    try:
        logger.info(f"Generated JSON string for COMPARISON_RESULT (config ID {config_id}): {comparison_data_dict}")

        insert_query = """
        INSERT INTO DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_RESULTS (
            CONFIG_ID,
            COMPARISON_DATE,
            COMPARISON_TIMESTAMP,
            COMPARISON_RESULT
        ) SELECT %s,
                 CURRENT_DATE(),
                 CURRENT_TIMESTAMP(),
                 PARSE_JSON(%s)
        """

        cursor.execute(insert_query, (config_id, json.dumps(comparison_data_dict)))
        logger.info(f"Successfully stored comparison result for config ID {config_id}.")

    except Exception as e:
        logger.error(f"Error storing comparison result for config ID {config_id}: {e}")
        raise

key_vault = KeyVaultList()


# %% Main Execution Function
@catch_error(logger)
def run_data_comparison(): 
    """Main function to run data comparison between SQL Server and Snowflake."""
    spark = None
    snowflake_conn = None
    sql_connections = {}

    try:
        # Step 1: Initialize Spark Session
        logger.info("Initializing Spark Session...")
        spark = create_spark()
        logger.info("Spark Session initialized.")

        # Step 2: Get Snowflake connection
        # If this fails, the @catch_error decorator on connect_to_snowflake will
        # log and re-raise, and the @catch_error on run_data_comparison will
        # catch and re-raise, thus aborting the entire process as required.
        logger.info("Connecting to Snowflake...")
        snowflake_conn = connect_to_snowflake()
        logger.info(f"Snowflake connection established: {snowflake_conn}")

        # Step 3: Get configuration data
        logger.info("Fetching configuration data from Snowflake...")
        config_data = fetch_config_data(snowflake_conn)
        logger.info(f"Configuration data fetched. Number of configurations: {len(config_data)}")

        # Step 4: Establish SQL Server connections for all unique server-database combinations
        logger.info("Establishing SQL Server connections...")
        sql_connections = establish_sql_connections(config_data)
        logger.info(f"Established SQL connections: {list(sql_connections.keys())}")

        # Step 5: Process each configuration item
        successful_comparisons = 0
        failed_comparisons = 0

        for config_item in config_data:
            # Initialize comparison results for the current config item
            comparison_data_to_store = {
                "source_count": 0,
                "target_count": 0,
                "source_query_result": [],
                "target_query_result": [],
                "data_not_in_source": [],
                "data_not_in_target": [],
                "mismatch_column_values": [],
                "comparison_summary_message": "Comparison could not be performed due to an error."
            }

            sql_df = None
            sql_error = None
            snowflake_df = None
            snowflake_error = None

            try:
                config_id = config_item['ID']

                # Retrieve key columns from config_item, split by comma and strip whitespace
                key_columns_str = config_item.get('KEY_COLUMNS', '')
                key_columns = [k.strip().lower() for k in key_columns_str.split(',') if k.strip()]

                logger.info(f"Processing configuration ID: {config_id} with Key Columns: {key_columns}")

                # --- Handle SQL Connection and Query Execution ---
                sql_connection = get_connection_for_config(config_item, sql_connections)
                if not sql_connection:
                    sql_error = f"No SQL connection available for config ID {config_id}. Skipping SQL query execution."
                    logger.logger.error(sql_error)
                    # sql_df remains None
                else:
                    source_query = config_item.get('SOURCE_QUERY') or f"SELECT * FROM {config_item['SOURCE_SCHEMA']}.{config_item['SOURCE_TABLE']}"
                    logger.info(f"Executing SQL query for config ID {config_id}...")
                    sql_df, sql_error = execute_sql_query_with_spark(spark, sql_connection, source_query)

                # --- Handle Snowflake Query Execution (Always attempt if snowflake_conn is active) ---
                target_query = config_item.get('TARGET_QUERY') or f"SELECT * FROM {config_item['TARGET_SCHEMA']}.{config_item['TARGET_TABLE']}"
                logger.info(f"Executing Snowflake query for config ID {config_id}...")
                snowflake_df, snowflake_error = execute_snowflake_query_with_spark(spark, snowflake_conn, target_query)


                # --- Comparison and Result Aggregation ---
                if sql_error and snowflake_error: # Both failed
                    comparison_data_to_store["comparison_summary_message"] = f"Both queries failed to execute. SQL error: {sql_error}, Snowflake error: {snowflake_error}"
                    logger.logger.error(comparison_data_to_store["comparison_summary_message"])
                    failed_comparisons += 1
                elif sql_error: # Only SQL failed (Snowflake ran)
                    comparison_data_to_store = compare_spark_dataframes(spark, sql_df, snowflake_df, key_columns)
                    comparison_data_to_store["comparison_summary_message"] = f"SQL query failed ({sql_error}). Snowflake query succeeded. Comparison performed with partial data. {comparison_data_to_store['comparison_summary_message']}"
                    logger.logger.warning(comparison_data_to_store["comparison_summary_message"])
                    failed_comparisons += 1 # Count as failure because source was an issue
                elif snowflake_error: # Only Snowflake failed (SQL ran)
                    comparison_data_to_store = compare_spark_dataframes(spark, sql_df, snowflake_df, key_columns)
                    comparison_data_to_store["comparison_summary_message"] = f"Snowflake query failed ({snowflake_error}). SQL query succeeded. Comparison performed with partial data. {comparison_data_to_store['comparison_summary_message']}"
                    logger.logger.error(comparison_data_to_store["comparison_summary_message"])
                    failed_comparisons += 1 # Count as failure because target was an issue
                else: # Both queries succeeded
                    logger.info(f"Comparing DataFrames for config ID {config_id}...")
                    comparison_data_to_store = compare_spark_dataframes(spark, sql_df, snowflake_df, key_columns)
                    if comparison_data_to_store["comparison_summary_message"] == "Data matches perfectly":
                        successful_comparisons += 1
                    else:
                        failed_comparisons += 1

                # --- Store Result in Snowflake for every processed config item ---
                logger.info(f"Storing comparison result for config ID {config_id}: {comparison_data_to_store['comparison_summary_message']}")
                snowflake_cursor = snowflake_conn.cursor()
                store_comparison_result(
                    snowflake_cursor,
                    config_id,
                    comparison_data_to_store
                )
                snowflake_cursor.close()

            except Exception as e_config_item:
                # This catches errors specific to processing *one* config item (e.g., key column issue, spark collect issue)
                error_msg = f"An unexpected error occurred while processing config ID {config_id}: {str(e_config_item)}"
                logger.logger.error(error_msg, exc_info=True)
                failed_comparisons += 1
                comparison_data_to_store["comparison_summary_message"] = error_msg
                try:
                    snowflake_cursor = snowflake_conn.cursor()
                    store_comparison_result(snowflake_cursor, config_id, comparison_data_to_store)
                    snowflake_cursor.close()
                except Exception as e_store:
                    logger.logger.error(f"Failed to store error details for config ID {config_id}: {e_store}", exc_info=True)
                continue # Move to the next config item

    except Exception as e_overall:
        # This catches any high-level errors not handled inside the loop,
        # e.g., initial Spark or Snowflake connection failures (which will re-raise)
        error_msg = f"Overall process failed: {str(e_overall)}"
        logger.logger.error(error_msg, exc_info=True)
        # Re-raise to ensure the process truly aborts if it's a critical failure like Snowflake connection
        raise

    finally:
        # Step 6: Close connections and stop Spark
        if spark:
            logger.info("Stopping Spark Session...")
            spark.stop()
            logger.info("Spark Session stopped.")

        for conn_key, pyodbc_conn_obj in sql_connections.items():
            if pyodbc_conn_obj:
                try:
                    logger.info(f"Closing SQL Server connection: {conn_key}")
                    pyodbc_conn_obj.close()
                except Exception as e:
                    logger.warning(f"Failed to close SQL Server connection {conn_key}: {e}")

        if snowflake_conn:
            logger.info("Closing Snowflake connection...")
            snowflake_conn.close()
            logger.info("Snowflake connection closed.")
    logger.info(f"Data comparison process completed. Successful comparisons: {successful_comparisons}, Failed comparisons: {failed_comparisons}")


# %% Call Main Function
if __name__ == "__main__":
    run_data_comparison()
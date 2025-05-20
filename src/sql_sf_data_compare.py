#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL Server to Snowflake Data Comparison

This script compares data between SQL Server and Snowflake based on configured queries
in the metadata table DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_CONFIG.
"""

# %% Import Libraries
import os, sys, argparse, pyodbc
import pandas as pd
from datetime import datetime

# Import existing modules
from modules3.snowflake_ddl import connect_to_snowflake
from modules3.common_functions import logger, catch_error, Connection

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
        columns = [col[0] for col in cursor.description]
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
def execute_query(connection, query, is_snowflake=False):
    """Execute query on the specified connection and return results as DataFrame."""
    try:
        if is_snowflake:
            cursor = connection.cursor()
            cursor.execute(query)
            result = cursor.fetch_pandas_all() if hasattr(cursor, 'fetch_pandas_all') else pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
        else:
            result = pd.read_sql(query, connection)

        return result
    except Exception as e:
        error_msg = f"Error executing query: {str(e)}\nQuery: {query}"
        logger.error(error_msg)
        raise Exception(error_msg)

@catch_error(logger)
def compare_dataframes(sql_df, sf_df):
    """Compare two dataframes and return comparison results."""
    try:
        # Check if column counts match
        if len(sql_df.columns) != len(sf_df.columns):
            return False, f"Column count mismatch: SQL Server: {len(sql_df.columns)}, Snowflake: {len(sf_df.columns)}"

        # Normalize column names (case insensitive)
        sql_df.columns = [col.lower() for col in sql_df.columns]
        sf_df.columns = [col.lower() for col in sf_df.columns]

        # Sort columns for consistency
        sql_df = sql_df.sort_index(axis=1)
        sf_df = sf_df.sort_index(axis=1)

        # Check if row counts match
        if len(sql_df) != len(sf_df):
            return False, f"Row count mismatch: SQL Server: {len(sql_df)}, Snowflake: {len(sf_df)}"

        # Compare data
        # Sort both dataframes if they have common columns that can be used as identifiers
        common_cols = list(set(sql_df.columns) & set(sf_df.columns))
        if common_cols:
            sql_df = sql_df.sort_values(by=common_cols).reset_index(drop=True)
            sf_df = sf_df.sort_values(by=common_cols).reset_index(drop=True)

        # Compare data values
        if not sql_df.equals(sf_df):
            # Get detailed differences
            diff_mask = (sql_df != sf_df) | (sql_df.isna() != sf_df.isna())
            diff_locations = [(i, col) for i in range(len(sql_df)) for col in sql_df.columns if diff_mask.loc[i, col]]

            if len(diff_locations) > 10:
                return False, f"Data mismatch at {len(diff_locations)} locations"
            else:
                diff_examples = []
                for i, col in diff_locations[:10]:  # Limit to first 10 differences
                    sql_val = sql_df.loc[i, col]
                    sf_val = sf_df.loc[i, col]
                    diff_examples.append(f"Row {i}, Column '{col}': SQL='{sql_val}', SF='{sf_val}'")

                return False, "Data mismatch:\n" + "\n".join(diff_examples)

        return True, "Data matches"
    except Exception as e:
        error_msg = f"Error comparing dataframes: {str(e)}"
        logger.error(error_msg)
        return False, f"Error during comparison: {str(e)}"

# %% Result Storage Functions
@catch_error(logger)
def store_results(snowflake_conn, config_item, match_status, comparison_message):
    """Store comparison results in Snowflake output table."""
    try:
        cursor = snowflake_conn.cursor()

        # Create output table if it doesn't exist
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
            COMPARISON_TIMESTAMP TIMESTAMP
        )
        """
        cursor.execute(create_table_query)

        # Get the next comparison ID
        cursor.execute("SELECT COALESCE(MAX(COMPARISON_ID), 0) + 1 FROM DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_RESULTS")
        comparison_id = cursor.fetchone()[0]

        # Insert results
        insert_query = """
        INSERT INTO DATAHUB.PIPELINE_METADATA.SQL_SNOWFLAKE_DATA_COMPARE_RESULTS (
            COMPARISON_ID, CONFIG_ID, SOURCE_DATA, SOURCE_DATA_SERVER, SOURCE_DATA_DATABASE,
            SOURCE_DATA_TABLE, TARGET_DATABASE_SCHEMA, TARGET_TABLE,
            MATCH_STATUS, COMPARISON_MESSAGE, COMPARISON_TIMESTAMP
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        cursor.execute(insert_query, (
            comparison_id,
            config_item['ID'],
            config_item['SOURCE_DATA'],
            config_item['SOURCE_DATA_SERVER'],
            config_item['SOURCE_DATA_DATABASE'],
            config_item['SOURCE_DATA_TABLE'],
            config_item['TARGET_DATABASE_SCHEMA'],
            config_item['TARGET_TABLE'],
            match_status,
            comparison_message,
            datetime.now()
        ))

        snowflake_conn.commit()
        cursor.close()
        logger.info(f"Stored comparison results for config ID {config_item['ID']}")

        return comparison_id
    except Exception as e:
        error_msg = f"Error storing results in Snowflake: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


# %% Main Execution Function
@catch_error
def run_data_comparison():
    """Main function to run data comparison between SQL Server and Snowflake."""
    snowflake_conn = None
    sql_connections = {}

    try:
        # Step 1: Get Snowflake connection using the imported module
        snowflake_conn = connect_to_snowflake()

        # Step 2: Get configuration data
        config_data = fetch_config_data(snowflake_conn)

        for config_item in config_data:
            try:
                logger.info(f"Processing comparison config ID: {config_item['ID']}")

                # Get SQL Server connection (or reuse existing)
                server = config_item['SOURCE_DATA_SERVER']
                database = config_item['SOURCE_DATA_DATABASE']
                conn_key = f"{server}_{database}"

                if conn_key not in sql_connections:
                    # Use the Connection class from common_functions
                    sql_conn_obj = Connection(
                        driver='{ODBC Driver 17 for SQL Server}',  # Ensure this driver name is correct
                        server=server,
                        database=database,
                        trusted_connection=True  # Assuming trusted connection as in the original script
                        # If not trusted connection, you'd need to provide username/password or key_vault details
                    )
                    sql_connections[conn_key] = pyodbc.connect(sql_conn_obj.get_connection_str_sql())

                sql_conn = sql_connections[conn_key]

                # Step 3: Execute queries
                sql_query = config_item['SOURCE_DATA_QUERY']
                sf_query = config_item['TARGET_QUERY']

                logger.info(f"Executing SQL Server query for {server}.{database}.{config_item['SOURCE_DATA_TABLE']}")
                sql_result = execute_query(sql_conn, sql_query)

                logger.info(f"Executing Snowflake query for {config_item['TARGET_DATABASE_SCHEMA']}.{config_item['TARGET_TABLE']}")
                sf_result = execute_query(snowflake_conn, sf_query, is_snowflake=True)

                # Step 4: Compare results
                match_status, comparison_message = compare_dataframes(sql_result, sf_result)
                logger.info(f"Comparison result: {'Match' if match_status else 'Mismatch'}")

                # Step 5: Store results
                comparison_id = store_results(snowflake_conn, config_item, match_status, comparison_message)

            except Exception as e:
                logger.error(f"Error processing config ID {config_item['ID']}: {str(e)}")

                # Store error in results table
                try:
                    if snowflake_conn:
                        store_results(
                            snowflake_conn,
                            config_item,
                            False,
                            f"Error during comparison: {str(e)}"
                        )
                except Exception as store_err:
                    logger.error(f"Error storing failure results: {str(store_err)}")

    except Exception as e:
        error_msg = f"Error in run_data_comparison: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    finally:
        # Close all connections
        try:
            for conn_key, conn in sql_connections.items():
                conn.close()
                logger.info(f"Closed SQL connection: {conn_key}")
        except Exception as e:
            logger.warning(f"Error closing SQL connections: {str(e)}")

        logger.info("Data comparison job completed")

# %% Command Line Interface
def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='SQL Server to Snowflake Data Comparison')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    return parser.parse_args()

# %% Main Execution
if __name__ == "__main__":
    args = parse_arguments()

    # Set debug level if requested
    if args.debug:
        logger.setLevel("DEBUG")
        logger.debug("Debug mode enabled")

    # Run the data comparison
    run_data_comparison()
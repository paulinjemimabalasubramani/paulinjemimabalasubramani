description = """

Extract data from snowflake

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
        'pipelinekey': 'net_new_assets_extract',
        'source_path': r'C:\myworkdir\Shared\SABOS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\Sabos\Sabos_Schema.csv',
    }

import os, csv, argparse, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)



from modules3.common_functions import logger, data_settings, catch_error
from modules3.snowflake_ddl import connect_to_snowflake
from datetime import datetime


def get_data_feed_file_inventory(conn):
    """
    Retrieves ALL active outbound metadata records from DATA_FEED_FILE_INVENTORY.
    This function no longer filters by pipeline_key, but fetches all relevant entries.

    Args:
        conn: Snowflake connection object.

    Returns:
        list: A list of dictionaries, where each dictionary contains the metadata
              for a file feed, or an empty list if no records are found.
    """
    cursor = conn.cursor()

    try:
        query = f"""
            SELECT
                SOURCE_QUERY,
                FILE_STORAGE_LOCATION_TEXT,
                DELIMITER_CHARACTER,
                HEADER_AVAILABLE_FLG,
                FILE_NAME_PREFIX,
                FILE_NAME_SUFFIX,
                FILE_FORMAT,
                PIPELINEKEY,
                FILE_ID
            FROM DATAHUB.PIPELINE_METADATA.DATA_FEED_FILE_INVENTORY
            WHERE INBOUND_OUTBOUND_TEXT = 'OUTBOUND'
            AND PIPELINEKEY = '{data_settings.pipelinekey.upper()}'
            AND ACTIVE_FLG = 'Y'
        """
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            columns = [desc[0] for desc in cursor.description]
            metadata = dict(zip(columns, result))
            print(f"Metadata found for pipelinekey: {metadata}")
            return metadata
        else:
            print(f"No active outbound metadata found for pipelinekey.")
            return None
    except Exception as e:
        print(f"Error retrieving metadata for pipelinekey: {e}")
        raise
    finally:
        cursor.close()


def execute_source_query_and_fetch_data(conn, source_query):
    """
    Executes the source query and fetches all data.

    Args:
        conn: Snowflake connection object.
        source_query (str): The SQL query to execute.

    Returns:
        tuple: A tuple containing (list of column names, list of data rows).
    """
    cursor = conn.cursor()
    try:
        print(f"Executing source query:\n{source_query}")
        cursor.execute(source_query)
        column_names = [desc[0] for desc in cursor.description]
        data_rows = cursor.fetchall()
        print(f"Fetched {len(data_rows)} rows.")
        return column_names, data_rows
    except Exception as e:
        print(f"Error executing source query: {e}")
        raise
    finally:
        cursor.close()


def write_data_to_file(file_path, column_names, data_rows, delimiter, include_header):
    """
    Writes the fetched data to a file with the specified format.

    Args:
        file_path (str): The full path to the output file.
        column_names (list): List of column names.
        data_rows (list): List of data tuples/lists.
        delimiter (str): Delimiter character for the file.
        include_header (bool): True if header should be included, False otherwise.
    """
    try:
        output_column_names = column_names

        # Prepare data rows by prepending run_date_str
        output_data_rows = data_rows
        
        # Ensure the output directory exists before writing
        output_dir = os.path.dirname(file_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=delimiter)
            if include_header:
                writer.writerow(output_column_names)
            writer.writerows(output_data_rows)
        print(f"Data successfully written to: {file_path}")
    except Exception as e:
        print(f"Error writing data to file '{file_path}': {e}")
        raise

def snowflake_outbound_extract():

    print(f"Starting data extraction for pipelinekey: {data_settings.pipelinekey.upper()}")
    snowflake_conn = None
    try:
        snowflake_conn = connect_to_snowflake()

        # Get all metadata records for all active outbound files
        metadata = get_data_feed_file_inventory(snowflake_conn)

        if not metadata:
            print(f"Exiting: No active outbound metadata record found for pipelinekey '{data_settings.pipelinekey.upper()}'.")
            return
        
        source_query = metadata.get('SOURCE_QUERY')
        file_location = metadata.get('FILE_STORAGE_LOCATION_TEXT')
        delimiter = metadata.get('DELIMITER_CHARACTER', '|')
        header_available_flg = metadata.get('HEADER_AVAILABLE_FLG', 'N')
        include_header = (header_available_flg.upper() == 'Y')

        file_name_prefix = metadata.get('FILE_NAME_PREFIX')
        file_name_suffix = metadata.get('FILE_NAME_SUFFIX', '') # Default to empty string
        file_format = metadata.get('FILE_FORMAT', 'csv')

        # Get current date for suffix formatting
        current_date = datetime.now()
        date_suffix = ''

        if file_name_suffix.lower() == 'yyyymmdd':
            date_suffix = current_date.strftime('%Y%m%d')
        elif file_name_suffix.lower() == 'yyyymm':
            date_suffix = current_date.strftime('%Y%m')
        # Else, if file_name_suffix is empty or anything else, date_suffix remains empty

        # Construct output filename based on rules
        if date_suffix:
            output_filename = f"{file_name_prefix}_{date_suffix}.{file_format}"
        else:
            output_filename = f"{file_name_prefix}.{file_format}"

        full_output_path = os.path.join(file_location, output_filename)

        if not source_query or not file_location:
            print("Error: SOURCE_QUERY or FILE_STORAGE_LOCATION_TEXT missing in metadata.")
            return

        column_names, data_rows = execute_source_query_and_fetch_data(snowflake_conn, source_query)

        # Call write_data_to_file without run_date_str
        write_data_to_file(full_output_path, column_names, data_rows, delimiter, include_header)

    except Exception as e:
        print(f"An unhandled error occurred during extraction: {e}")
        raise # Re-raise the exception to signal failure to Airflow
    finally:
        if snowflake_conn:
            snowflake_conn.close()
            print("Snowflake connection closed.")

if __name__ == "__main__":
    snowflake_outbound_extract()
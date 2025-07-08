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
                PIPELINEKEY, -- Include PIPELINEKEY for logging/context if needed
                FILE_ID -- Include FILE_ID for logging/context if needed
            FROM DATAHUB.PIPELINE_METADATA.DATA_FEED_FILE_INVENTORY
            WHERE INBOUND_OUTBOUND_TEXT = 'OUTBOUND' -- Ensure it's an outbound feed
            AND ACTIVE_FLG = 'Y' -- Ensure the feed is active
        """
        cursor.execute(query)
        results = cursor.fetchall() # Fetch all matching records

        metadata_list = []
        if results:
            columns = [desc[0] for desc in cursor.description]
            for row in results:
                metadata_list.append(dict(zip(columns, row)))
            print(f"Found {len(metadata_list)} active outbound metadata records.")
        else:
            print(f"No active outbound metadata records found in DATA_FEED_FILE_INVENTORY.")
        return metadata_list
    except Exception as e:
        print(f"Error retrieving all metadata records: {e}")
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


def write_data_to_file(file_path, column_names, data_rows, delimiter, include_header, run_date_str):
    """
    Writes the fetched data to a file with the specified format.

    Args:
        file_path (str): The full path to the output file.
        column_names (list): List of column names.
        data_rows (list): List of data tuples/lists.
        delimiter (str): Delimiter character for the file.
        include_header (bool): True if header should be included, False otherwise.
        run_date_str (str): The current run date string (e.g., 'YYYYMMDD').
    """
    try:
        # Prepend 'RUNDATE' to column names if header is included
        output_column_names = ['RUNDATE'] + column_names if include_header else []

        # Prepare data rows by prepending run_date_str
        output_data_rows = []
        for row in data_rows:
            output_data_rows.append([run_date_str] + list(row))

        with open(file_path, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile, delimiter=delimiter)
            if include_header:
                writer.writerow(output_column_names)
            writer.writerows(output_data_rows)
        print(f"Data successfully written to: {file_path}")
    except Exception as e:
        print(f"Error writing data to file '{file_path}': {e}")
        raise

def main():

    print(f"Starting data extraction for all active outbound files.")
    snowflake_conn = None
    try:
        snowflake_conn = connect_to_snowflake()

        # Get all metadata records for all active outbound files
        metadata_records = get_data_feed_file_inventory(snowflake_conn)

        if not metadata_records:
            print(f"Exiting: No active outbound metadata records found to process.")
            return
        
        # Get current run date for filename and RUNDATE column (common for all files in this run)
        current_date = datetime.now()
        run_date_str = current_date.strftime('%Y%m%d') # Format as YYYYMMDD

        # Loop through each metadata record and process it
        for i, metadata in enumerate(metadata_records):
            file_id = metadata.get('FILE_ID', 'N/A')
            pipeline_key_for_record = metadata.get('PIPELINEKEY', 'N/A')
            print(f"\n--- Processing file ID: {file_id} (PipelineKey: {pipeline_key_for_record}) - Record {i+1}/{len(metadata_records)} ---")

            source_query = metadata.get('SOURCE_QUERY')
            file_location = metadata.get('FILE_STORAGE_LOCATION_TEXT')
            delimiter = metadata.get('DELIMITER_CHARACTER', '|') # Default to '|'
            header_available_flg = metadata.get('HEADER_AVAILABLE_FLG', 'N')
            include_header = (header_available_flg.upper() == 'Y')
            file_name_prefix = metadata.get('FILE_NAME_PREFIX', f"FILE_{file_id}") # Use FILE_ID if prefix is missing
            file_name_suffix = metadata.get('FILE_NAME_SUFFIX', '')
            file_format = metadata.get('FILE_FORMAT', 'csv')

            if not source_query or not file_location:
                print(f"Skipping record for File ID {file_id} due to missing SOURCE_QUERY or FILE_STORAGE_LOCATION_TEXT in metadata: {metadata}")
                continue # Skip to the next metadata record

            try:
                column_names, data_rows = execute_source_query_and_fetch_data(snowflake_conn, source_query)

                # Construct output filename
                # Example: FILE_NAME_PREFIX_YYYYMMDD_FILE_NAME_SUFFIX.FILE_FORMAT
                output_filename = f"{file_name_prefix}_{run_date_str}{file_name_suffix}.{file_format}"
                full_output_path = os.path.join(file_location, output_filename)

                write_data_to_file(full_output_path, column_names, data_rows, delimiter, include_header, run_date_str)

            except Exception as e:
                print(f"Error processing metadata record for File ID {file_id}: {e}")
                # Continue to the next record even if one fails
                continue

    except Exception as e:
        print(f"An unhandled error occurred during the overall extraction process: {e}")
        raise
    finally:
        if snowflake_conn:
            snowflake_conn.close()
            print("Snowflake connection closed.")

if __name__ == "__main__":
    main()
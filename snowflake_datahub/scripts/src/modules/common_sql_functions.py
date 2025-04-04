import os, shutil, pyodbc, csv, glob, logging
from functools import wraps
from datetime import datetime

batch_size = 100000

file_ext = '.csv'

def set_pipeline_params(pipeline_name, database_name, connection_string):
    data_folder = os.getenv('app_folder')+ '\\data'
    params_config={
        'database': database_name,
        'pipeline': pipeline_name,
        'connection_string' : connection_string,
        'data_file_path' : os.path.join(data_folder, pipeline_name, "Extract_table_data"),
        'zip_file_path' : os.path.join(data_folder, pipeline_name, "Zip"),
        'output_file_path': os.path.join(data_folder, pipeline_name,"Output")
        }
    logging.info(f"Pipeline params set: {params_config}")
    return params_config

# %%

def extract_data(params_config: dict, tables:list):
    create_folder_paths(list_of_paths=[params_config['data_file_path'], params_config['zip_file_path'], params_config['output_file_path']])
    delete_files(paths = [params_config['data_file_path'], params_config['zip_file_path'], params_config['output_file_path']])
    create_table_files(params_config, tables)
    zip_file = os.path.join(params_config['zip_file_path'], params_config['pipeline'].upper()+datetime.now().strftime("%Y%m%d%H%M%S"))
    zip_files(zip_file, params_config)
    move_file(zip_file, params_config)

# %%

def catch_error():
    """
    Wrapper/Decorator function for catching errors
    """
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            response = None
            try:
                response = fn(*args, **kwargs)
            except (BaseException, AssertionError) as e:
                logging.error(f"An error occurred: {e}")
                raise e
            return response
        return inner
    return outer


# %%

def create_folder_paths(list_of_paths:list):
    """
    Create folder paths if not exists
    """
    for file_path in list_of_paths:
        if not os.path.isdir(file_path):
            os.makedirs(file_path, exist_ok=True)
            logging.info(f"Created folder: {file_path}")

# %%

@catch_error()
def delete_files(paths: list):
    """
    Delete all the files in data_file_path and zip_file_path
    """
    for folder_path in paths:
        files = glob.glob(os.path.join(folder_path, '*'))
        for file_path in files:
            try:
                os.remove(file_path)
                logging.info(f"File deleted {file_path}")
            except Exception as e:
                logging.error(f"Error deleting file {file_path}: {e}")
                raise e

# %%

def table_name_to_file_name(params_config: dict, table_name:str):
    """
    Convert table name to full path of file name
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    formatted_table_name = table_name.replace('.', '_').upper()  # Format table name
    file_name = f"{params_config['database']}_{formatted_table_name}_{timestamp}{file_ext}"
    file_path = os.path.join(params_config['data_file_path'], file_name)

    logging.info(f"The file created in {file_path}")

    return file_path


# %%

@catch_error()
def dump_data_to_file(cursor, file_name: str, sql_query: str):
    """
    Dump data from odbc connection to a file
    """
    cursor.execute(sql_query)

    logging.info(f"Writing data to file: {file_name}")

    with open(file=file_name, mode='wt', newline='', encoding='utf-8', errors='replace') as csvfile:
        datawriter = csv.writer(csvfile, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_ALL)

        datawriter.writerow([column[0].lower() for column in cursor.description])

        column_headers = [column[0].lower() for column in cursor.description]
        logging.info(f"Column headers for file {file_name}: {column_headers}")
        
        while True:
            rows = cursor.fetchmany(batch_size)

            if len(rows) == 0:
                logging.info(f"No more rows to fetch.")
                break
            logging.info(f"Fetched {len(rows)} rows.")

            for row in rows:
                try:
                    datawriter.writerow(row)
                except Exception as e:
                    logging.error(f"Error writing row {row}: {e}")
                    raise e
                
    logging.info(f"Data dump completed for {file_name}")


# %%

@catch_error()
def create_table_files(params_config: dict, tables: list):
    """
    Create csv file for each table
    """
    
    try:
        with pyodbc.connect(params_config['connection_string'], autocommit=False) as conn:
            cursor = conn.cursor()
            for table in tables:
                if isinstance(table, list) or isinstance(table, tuple):
                    table_name = table[0]
                    sql_query = table[1]
                elif isinstance(table, str):
                    table_name = table
                    sql_query = f'select * from {table_name} (nolock);'
                else:
                    logging.warning(f'Invalid table type specified: {table}')
                    continue

                if sql_query.lower().find('(nolock)') < 0:
                    logging.warning(f'(nolock) is not implemented in SQL query: {sql_query}')
                    continue

                logging.info(f'Executing query: {sql_query}')

                file_name = table_name_to_file_name(params_config, table_name=table_name)
                dump_data_to_file(cursor, file_name=file_name, sql_query=sql_query)

    except pyodbc.Error as e:
        logging.error(f'Database error occurred: {e}')
        raise e
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')

    finally:
        cursor.close()
        conn.close()
        

# %%

def zip_files(zip_file, params_config: dict):

    # Create the ZIP in Zip folder
    shutil.make_archive(zip_file, 'zip', params_config['data_file_path'])
    logging.info(f"Files zipped to: {zip_file}.zip")

# %%

def move_file(zip_file, params_config: dict):
    
    shutil.move(zip_file+".zip", params_config['output_file_path'])

# %%

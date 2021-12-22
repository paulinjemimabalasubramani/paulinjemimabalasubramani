"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure


Useful Links for dev:
https://stackoverflow.com/questions/33224740/best-way-to-get-the-max-value-in-a-spark-dataframe-column
"""

# %% Import Libraries

import os, sys, re, tempfile, shutil, copy, json, pymssql
from pprint import pprint
from collections import defaultdict
from datetime import datetime

from .common_functions import logger, catch_error, is_pc, execution_date, data_settings, pymssql_execute_non_query, \
    execution_date_start, EXECUTION_DATE_str, cloud_file_hist_conf
from .azure_functions import read_adls_gen2, \
    default_storage_account_name, file_format, save_adls_gen2, setup_spark_adls_gen2_connection, container_name, \
    default_storage_account_abbr, metadata_folder, azure_container_folder_path, data_folder, add_table_to_tableinfo, \
    storage_account_abbr_to_full_name
from .spark_functions import read_csv, IDKeyIndicator, add_id_key, read_sql, column_regex, partitionBy, \
    to_string, remove_column_spaces, add_elt_columns, partitionBy_value, \
    write_sql, ELT_PROCESS_ID_str
from .snowflake_ddl import connect_to_snowflake, snowflake_ddl_params

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, when, to_timestamp
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.window import Window



# %% Parameters

cloud_file_history_name = ('_'.join([data_settings.domain_name, data_settings.schema_name, 'file_history3'])).lower()
cloud_row_history_name = ('_'.join([data_settings.domain_name, data_settings.schema_name, 'row_history3'])).lower()


cloud_file_history_columns = [
    ('database_name', 'varchar(300) NOT NULL'), # data_settings.domain_name
    ('schema_name', 'varchar(300) NOT NULL'), # data_settings.schema_name
    ('table_name', 'varchar(500) NOT NULL'),

    ('file_name', 'varchar(300) NULL'),
    ('file_path', 'varchar(1000) NULL'),
    ('folder_path', 'varchar(1000) NULL'),
    ('zip_file_path', 'varchar(1000) NULL'),

    ('firm_name', 'varchar(300) NULL'), # data_settings.pipeline_firm
    ('is_full_load', 'bit NULL'),
    ('key_datetime', 'datetime NULL'),

    (EXECUTION_DATE_str.lower(), 'datetime NULL'), # execution_date
    (ELT_PROCESS_ID_str.lower(), 'varchar(500) NULL'), # data_settings.elt_process_id
    ('pipelinekey', 'varchar(500) NULL'), # data_settings.pipelinekey
    ]





# %% Get Key Column Names

@catch_error(logger)
def get_key_column_names(
        base:list = ['databse_name', 'schema_name', 'table_name'],
        with_load:list = ['is_full_load'],
        with_load_n_date:list = ['key_datetime'],
        ):
    """
    Get Key Column Names for sorting the data files for uniqueness
    """
    key_column_names = dict() 
    key_column_names['base'] = base
    key_column_names['with_load'] = key_column_names['base'] + with_load
    key_column_names['with_load_n_date'] = key_column_names['with_load'] + with_load_n_date
    return key_column_names



data_settings.key_column_names = get_key_column_names()



# %% Check if file_meta exists in SQL server File History

@catch_error(logger)
def file_meta_exists_in_history(file_meta):
    """
    Check if file_meta exists in SQL server File History
    """
    sqlstr = f"""SELECT COUNT(*) AS CNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{cloud_file_hist_conf['sql_schema'].upper()}'
        AND TABLE_TYPE = 'BASE TABLE'
        AND UPPER(TABLE_NAME) = '{cloud_file_history_name.upper()}'
    ;"""

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr)
            row = cursor.fetchone()
            if int(row['CNT']) == 0:
                full_table_name = f"{cloud_file_hist_conf['sql_schema'.lower()]}.{cloud_file_history_name.lower()}"
                logger.info(f'{full_table_name} table does not exist in SQL server. Creating new table.')





                create_table_sqlstr = f"""
            CREATE TABLE [{full_table_name}] (
                 [table_name] varchar(300) NOT NULL
                ,[schema_name] varchar(200) NOT NULL
                ,[database_name] varchar(200) NOT NULL
                ,[rows_ingested] [int] NOT NULL
                ,[max_timestamp] varchar(300) NULL
                ,[max_timestamp_column_name] varchar(300) NULL
                ,[{EXECUTION_DATE_str.lower()}] [datetime] NOT NULL,
            );
                """
                conn._conn.execute_non_query(create_table_sqlstr)
                return False





# %% Mirgate all files recursively unzipping any files

@catch_error(logger)
def recursive_migrate_all_files(spark, source_path:str, snowflake_connection, fn_extract_file_meta, additional_ingest_columns, fn_process_file, zip_file_path:str=None):
    """
    Mirgate all files recursively unzipping any files
    """
    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)

            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    logger.info(f'Extracting {file_path} to {tmpdir.name}')
                    shutil.unpack_archive(filename=file_path, extract_dir=tmpdir.name)
                    recursive_migrate_all_files(
                        spark = spark,
                        source_path = tmpdir.name,
                        snowflake_connection = snowflake_connection,
                        fn_extract_file_meta = fn_extract_file_meta,
                        additional_ingest_columns = additional_ingest_columns,
                        fn_process_file = fn_process_file,
                        zip_file_path = zip_file_path if zip_file_path else file_path, # to keep original zip file path, rather than the last zip file path
                        )
                    continue

            logger.info(f'Extract File Meta: {file_path}')
            file_meta = fn_extract_file_meta(file_path=file_path, zip_file_path=zip_file_path)
            if not file_meta or (data_settings.key_datetime > file_meta['key_datetime']): continue



            logger.info(file_meta)
            table_list = fn_process_file(file_meta=file_meta)






# %% Migrate All Files

@catch_error(logger)
def migrate_all_files(spark, fn_extract_file_meta, additional_ingest_columns, fn_process_file):
    """
    Migrate All Files
    """
    snowflake_ddl_params.spark = spark
    snowflake_ddl_params.snowflake_connection = connect_to_snowflake()

    recursive_migrate_all_files(
        spark = spark,
        source_path = data_settings.source_path,
        fn_extract_file_meta = fn_extract_file_meta,
        additional_ingest_columns = additional_ingest_columns,
        fn_process_file = fn_process_file
        )

    snowflake_ddl_params.snowflake_connection.close()



# %%




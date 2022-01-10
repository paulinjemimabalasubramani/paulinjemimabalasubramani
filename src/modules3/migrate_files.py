"""
Common Library for migrating any type of data from any source to Azure ADLS Gen 2 and Snowflake.

"""

# %% Import Libraries

import os, sys, tempfile, shutil, pymssql
from datetime import datetime

from .common_functions import logger, catch_error, is_pc, data_settings, EXECUTION_DATE_str, cloud_file_hist_conf, strftime, \
    pipeline_metadata_conf, execution_date
from .azure_functions import save_adls_gen2, setup_spark_adls_gen2_connection
from .spark_functions import ELT_PROCESS_ID_str
from .snowflake_ddl import connect_to_snowflake, snowflake_ddl_params, create_snowflake_ddl, write_DDL_file_per_step



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

    (EXECUTION_DATE_str.lower(), 'datetime NULL'), # data_settings.execution_date
    (ELT_PROCESS_ID_str.lower(), 'varchar(500) NULL'), # data_settings.elt_process_id
    ('pipelinekey', 'varchar(500) NULL'), # data_settings.pipelinekey
    ('total_seconds', 'int NULL'), # file_meta['total_seconds']
    ('file_size_kb', 'numeric(38, 3) NULL'), # os.path.getsize(file_meta['file_path'])/1024
    ('copy_command', 'varchar(2500) NULL'), # ' '.join(copy_commands)
    ]



# %% Get Key Column Names

@catch_error(logger)
def get_metadata_key_column_names(
        base:list = ['database_name', 'schema_name', 'table_name'],
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



data_settings.metadata_key_column_names = get_metadata_key_column_names()



# %% Get Primary Keys from SQL Server

@catch_error(logger)
def get_key_column_names(table_name:str):
    """
    Get Primary Keys from SQL Server
    """
    sqlstr_table_exists = f"""SELECT LOWER(SystemPrimaryKey) AS PK FROM {pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_primary_key']}
        where SystemPrimaryKey IS NOT NULL
            AND UPPER(DomainName) = '{data_settings.domain_name.upper()}'
            AND UPPER(DataSource) = '{data_settings.schema_name.upper()}'
            AND UPPER(AssetUniqueKey) = '{table_name.upper()}'
    """

    with pymssql.connect(
        server = pipeline_metadata_conf['sql_server'],
        user = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
        database = pipeline_metadata_conf['sql_database'],
        appname = sys.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr_table_exists)
            key_column_names = [row['PK'].strip().lower() for row in cursor]

    return sorted(key_column_names)



# %% Create file_meta table in SQL Server if not exists

@catch_error(logger)
def create_file_meta_table_if_not_exists(additional_file_meta_columns:list):
    """
    Create file_meta table in SQL Server if not exists
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_history_name}".lower()

    sqlstr_table_exists = f"""SELECT COUNT(*) AS CNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{cloud_file_hist_conf['sql_schema'].upper()}'
        AND UPPER(TABLE_TYPE) = 'BASE TABLE'
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
            cursor.execute(sqlstr_table_exists)
            row = cursor.fetchone()
            if int(row['CNT']) == 0:
                logger.info(f'{full_table_name} table does not exist in SQL server. Creating new table.')
                file_meta_columns = 'id int identity' + ''.join([f', {c[0]} {c[1]}' for c in cloud_file_history_columns + additional_file_meta_columns])
                create_table_sqlstr = f'CREATE TABLE {full_table_name} ({file_meta_columns});'
                conn._conn.execute_non_query(create_table_sqlstr)



# %% Check if file_meta exists in SQL server File History

@catch_error(logger)
def file_meta_exists_in_history(file_meta:dict):
    """
    Check if file_meta exists in SQL server File History
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_history_name}".lower()

    key_columns = []
    for c in data_settings.metadata_key_column_names['with_load_n_date']:
        cval = file_meta[c]
        cval = cval.strftime(strftime) if isinstance(cval, datetime) else str(cval)
        key_columns.append(f"{c}='{cval}'")
    key_columns = ' AND '.join(key_columns)
    sqlstr_meta_exists = f'SELECT COUNT(*) AS CNT FROM {full_table_name} WHERE {key_columns};'

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr_meta_exists)
            row = cursor.fetchone()
            return int(row['CNT']) > 0



# %% Write file_meta to SQL server File History

@catch_error(logger)
def write_file_meta_to_history(file_meta:dict, additional_file_meta_columns:list):
    """
    Write file_meta to SQL server File History
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_history_name}".lower()
    all_columns = [c[0] for c in cloud_file_history_columns + additional_file_meta_columns]
    all_columns_str = ', '.join(all_columns)

    column_values = []
    for c in all_columns:
        cval = file_meta[c]
        cval = cval.strftime(strftime) if isinstance(cval, datetime) else str(cval)
        column_values.append(f"'{cval}'")
    column_values_str = ', '.join(column_values)

    sqlstr = f'INSERT INTO {full_table_name} ({all_columns_str}) VALUES ({column_values_str});'

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.parent_name,
        autocommit = True,
        ) as conn:
        conn._conn.execute_non_query(sqlstr)



# %% Migrate Single File

@catch_error(logger)
def migrate_single_file(file_path:str, zip_file_path:str, fn_extract_file_meta, fn_process_file, additional_file_meta_columns:list):
    logger.info(f'Extract File Meta: {file_path}')
    start_total_seconds = datetime.now()
    file_meta = fn_extract_file_meta(file_path=file_path, zip_file_path=zip_file_path)
    if not file_meta or (data_settings.key_datetime > file_meta['key_datetime']): return

    file_meta = {
        **file_meta,
        'database_name': data_settings.domain_name,
        'schema_name': data_settings.schema_name,
        'firm_name': data_settings.pipeline_firm,
        EXECUTION_DATE_str.lower(): execution_date,
        ELT_PROCESS_ID_str.lower(): data_settings.elt_process_id,
        'pipelinekey': data_settings.pipelinekey,
    }

    if file_meta_exists_in_history(file_meta=file_meta):
        logger.info(f'File already exists, skipping: {file_path}')
        return

    logger.info(f"Processing: {file_path}")
    logger.info(file_meta)

    table_list = fn_process_file(file_meta=file_meta)
    if not table_list:
        logger.warning(f"No table_list to write")
        return

    copy_commands = []
    for table_name, table in table_list.items():
        setup_spark_adls_gen2_connection(spark=snowflake_ddl_params.spark, storage_account_name=data_settings.storage_account_name) # for data
        if not save_adls_gen2(table=table, table_name=table_name, is_metadata=False): continue

        setup_spark_adls_gen2_connection(spark=snowflake_ddl_params.spark, storage_account_name=data_settings.default_storage_account_name) # for metadata
        copy_commands.append(create_snowflake_ddl(table=table, table_name=table_name))

    file_meta['total_seconds'] = (datetime.now() - start_total_seconds).seconds
    file_meta['file_size_kb'] = os.path.getsize(file_meta['file_path'])/1024
    file_meta['copy_command'] = ' '.join(copy_commands)
    write_file_meta_to_history(file_meta=file_meta, additional_file_meta_columns=additional_file_meta_columns)



# %% Mirgate all files recursively unzipping any files

@catch_error(logger)
def recursive_migrate_all_files(source_path:str, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file, zip_file_path:str=None):
    """
    Mirgate all files recursively unzipping any files
    """
    if not os.path.isdir(source_path):
        logger.info(f'Path does not exist: {source_path}')
        return

    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)

            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=file_path, extract_dir=extract_dir)
                    recursive_migrate_all_files(
                        source_path = extract_dir,
                        fn_extract_file_meta = fn_extract_file_meta,
                        additional_file_meta_columns = additional_file_meta_columns,
                        fn_process_file = fn_process_file,
                        zip_file_path = zip_file_path if zip_file_path else file_path, # to keep original zip file path, rather than the last zip file path
                        )
                    continue

            migrate_single_file(
                file_path = file_path,
                zip_file_path = zip_file_path,
                fn_extract_file_meta = fn_extract_file_meta,
                fn_process_file = fn_process_file,
                additional_file_meta_columns = additional_file_meta_columns,
                )



# %% Migrate All Files

@catch_error(logger)
def migrate_all_files(spark, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file):
    """
    Migrate All Files
    """
    snowflake_ddl_params.spark = spark
    snowflake_ddl_params.snowflake_connection = connect_to_snowflake()

    create_file_meta_table_if_not_exists(additional_file_meta_columns)

    recursive_migrate_all_files(
        source_path = data_settings.source_path,
        fn_extract_file_meta = fn_extract_file_meta,
        additional_file_meta_columns = additional_file_meta_columns,
        fn_process_file = fn_process_file
        )

    write_DDL_file_per_step()
    snowflake_ddl_params.snowflake_connection.close()



# %%




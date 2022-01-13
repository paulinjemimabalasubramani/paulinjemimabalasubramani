"""
Common Library for migrating any type of data from any source to Azure ADLS Gen 2 and Snowflake.

"""

# %% Import Libraries

import os, sys, tempfile, shutil, pymssql
from datetime import datetime
from collections import OrderedDict

from .common_functions import logger, catch_error, is_pc, data_settings, EXECUTION_DATE_str, cloud_file_hist_conf, strftime, \
    pipeline_metadata_conf, execution_date, execution_date_start
from .azure_functions import save_adls_gen2, setup_spark_adls_gen2_connection
from .spark_functions import ELT_PROCESS_ID_str, partitionBy, elt_audit_columns
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
    ('datasourcekey', 'varchar(500) NULL'), # data_settings.pipeline_datasourcekey
    ('total_seconds', 'int NULL'), # file_meta['total_seconds']
    ('file_size_kb', 'numeric(38, 3) NULL'), # os.path.getsize(file_meta['file_path'])/1024
    ('copy_command', 'varchar(2500) NULL'), # ' '.join(copy_commands)
    ('zip_file_fully_ingested', 'bit NULL'), # False if zip_file_path else True
    ('reingest_file', 'bit NULL'), # False
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
        appname = sys.app.parent_name,
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
        appname = sys.app.parent_name,
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



# %% Default table type conversion

@catch_error(logger)
def default_table_dtypes(table, use_varchar:bool=True):
    """
    Default table type conversion
    """
    dtypes = []
    filter_columns = [c.lower() for c in elt_audit_columns] + [partitionBy.lower()]

    for (c, col_type) in table.dtypes:
        if c.lower() not in filter_columns:
            if ':' in col_type or col_type.lower() in ['variant']:
                dtype = 'variant'
            elif use_varchar:
                dtype = 'varchar'
            else:
                dtype = col_type.lower()
            dtypes.append((c.lower(), dtype))

    dtypes = OrderedDict(sorted(dtypes, key=lambda k: k[0]))
    return dtypes



# %% Utility function to convert Python values to SQL equivalent values

@catch_error(logger)
def to_sql_value(cval):
    """
    Utility function to convert Python values to SQL equivalent values
    """
    if isinstance(cval, datetime):
        cval = cval.strftime(strftime)
    else:
        cval = str(cval)
        cval = cval.replace("'", "''")
    return cval



# %% Check if file_meta exists in SQL server File History

@catch_error(logger)
def file_meta_exists_in_history(file_meta:dict):
    """
    Check if file_meta exists in SQL server File History
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_history_name}".lower()

    key_columns = []
    for c in data_settings.metadata_key_column_names['with_load_n_date']:
        cval = to_sql_value(file_meta[c])
        key_columns.append(f"{c}='{cval}'")
    key_columns = ' AND '.join(key_columns)
    sqlstr_meta_exists = f'SELECT COUNT(*) AS CNT FROM {full_table_name} WHERE {key_columns} AND reingest_file = 0;'

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
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
        cval = to_sql_value(file_meta[c])
        column_values.append(f"'{cval}'")
    column_values_str = ', '.join(column_values)

    sqlstr_insert = f'INSERT INTO {full_table_name} ({all_columns_str}) VALUES ({column_values_str});'

    if file_meta['key_datetime'] == execution_date_start:
        key_column_names = data_settings.metadata_key_column_names['with_load']
    else:
        key_column_names = data_settings.metadata_key_column_names['with_load_n_date']

    key_columns = []
    for c in key_column_names:
        cval = to_sql_value(file_meta[c])
        key_columns.append(f"{c}='{cval}'")
    key_columns = ' AND '.join(key_columns)

    sqlstr_update = f'UPDATE {full_table_name} SET reingest_file = 0 WHERE {key_columns} AND reingest_file > 0;'

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        conn._conn.execute_non_query(sqlstr_insert)
        conn._conn.execute_non_query(sqlstr_update)



# %% Migrate Single File

@catch_error(logger)
def migrate_single_file(file_path:str, zip_file_path:str, fn_extract_file_meta, fn_process_file, fn_get_dtypes, additional_file_meta_columns:list):
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
        'datasourcekey': data_settings.pipeline_datasourcekey,
        'file_size_kb': os.path.getsize(file_meta['file_path'])/1024,
        'zip_file_fully_ingested': False if zip_file_path else True,
        'reingest_file': False,
    }

    if file_meta_exists_in_history(file_meta=file_meta):
        logger.info(f'File already exists, skipping: {file_path}')
        return

    logger.info(f'Read File: {file_path}')
    logger.info(file_meta)

    table_list = fn_process_file(file_meta=file_meta)
    if not table_list:
        logger.warning(f"No table_list to write")
        return

    copy_commands = []
    for table_name, table in table_list.items():
        logger.info(f'Write Table "{table_name}" in File: {file_path}')
        setup_spark_adls_gen2_connection(spark=snowflake_ddl_params.spark, storage_account_name=data_settings.storage_account_name) # for data
        if not save_adls_gen2(table=table, table_name=table_name, is_metadata=False): continue

        setup_spark_adls_gen2_connection(spark=snowflake_ddl_params.spark, storage_account_name=data_settings.default_storage_account_name) # for metadata
        copy_commands.append(create_snowflake_ddl(table=table, table_name=table_name, fn_get_dtypes=fn_get_dtypes))

    file_meta['copy_command'] = ' '.join(copy_commands)
    file_meta['total_seconds'] = (datetime.now() - start_total_seconds).seconds

    write_file_meta_to_history(file_meta=file_meta, additional_file_meta_columns=additional_file_meta_columns)



# %% Update SQL Zip File Path - Fully Ingested to True

def update_sql_zip_file_path(zip_file_path:str):
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_history_name}".lower()

    sqlstr_update = f"""UPDATE {full_table_name}
        SET
            zip_file_fully_ingested = 1,
            reingest_file = 0
        WHERE zip_file_path = '{to_sql_value(zip_file_path)}'
    ;"""

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            conn._conn.execute_non_query(sqlstr_update)



# %% Mirgate all files recursively unzipping any files

@catch_error(logger)
def recursive_migrate_all_files(source_path:str, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file, fn_get_dtypes, zip_file_path:str=None, selected_file_paths:list=[]):
    """
    Mirgate all files recursively unzipping any files
    """
    if not selected_file_paths:
        if not os.path.isdir(source_path):
            logger.info(f'Path does not exist: {source_path}')
            return

        selected_file_paths = []
        for root, dirs, files in os.walk(source_path):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                selected_file_paths.append(file_path)

        selected_file_paths = sorted(selected_file_paths)

    for file_path in selected_file_paths:
        file_name = os.path.basename(file_path)
        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() == '.zip':
            with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                extract_dir = tmpdir
                logger.info(f'Extracting {file_path} to {extract_dir}')
                shutil.unpack_archive(filename=file_path, extract_dir=extract_dir)
                zip_file_path = zip_file_path if zip_file_path else file_path # to keep original zip file path, rather than the last zip file path
                recursive_migrate_all_files(
                    source_path = extract_dir,
                    fn_extract_file_meta = fn_extract_file_meta,
                    additional_file_meta_columns = additional_file_meta_columns,
                    fn_process_file = fn_process_file,
                    fn_get_dtypes = fn_get_dtypes,
                    zip_file_path = zip_file_path,
                    selected_file_paths = [],
                    )
                update_sql_zip_file_path(zip_file_path=zip_file_path)
                continue

        migrate_single_file(
            file_path = file_path,
            zip_file_path = zip_file_path,
            fn_extract_file_meta = fn_extract_file_meta,
            fn_process_file = fn_process_file,
            fn_get_dtypes = fn_get_dtypes,
            additional_file_meta_columns = additional_file_meta_columns,
            )



# %% Create Pipeline Instance table if not exists

def create_pipeline_instance_table_if_not_exists():
    """
    Create Pipeline Instance table if not exists
    """
    schema_name = pipeline_metadata_conf['sql_schema']
    table_name = pipeline_metadata_conf['sql_table_name_pipe_instance']
    full_table_name = f"{schema_name}.{table_name}".lower()

    sqlstr_table_exists = f"""SELECT COUNT(*) AS CNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{schema_name.upper()}'
        AND UPPER(TABLE_TYPE) = 'BASE TABLE'
        AND UPPER(TABLE_NAME) = '{table_name.upper()}'
    ;"""

    sqlstr_table_create = f"""CREATE TABLE {full_table_name} (
        PipelineInstanceId int Identity,
        PipelineKey varchar(500) NOT NULL,
        {ELT_PROCESS_ID_str.lower()} varchar(500) NOT NULL,
        {EXECUTION_DATE_str.lower()} datetime NULL,
        total_minutes numeric(38, 3) NULL,
        table_count int NULL,
        aggregate_file_size_kb numeric(38, 3) NULL,
        run_status varchar(100) NULL,
        error_message varchar(1500) NULL,
        error_table varchar(500)
        ;"""

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr_table_exists)
            row = cursor.fetchone()
            if int(row['CNT']) == 0:
                logger.info(f'{full_table_name} table does not exist in SQL server. Creating new table.')
                conn._conn.execute_non_query(sqlstr_table_create)



# %% Migrate All Files

@catch_error(logger)
def migrate_all_files(spark, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file, fn_select_files, fn_get_dtypes):
    """
    Migrate All Files
    """
    create_pipeline_instance_table_if_not_exists()
    create_file_meta_table_if_not_exists(additional_file_meta_columns)

    selected_file_paths = fn_select_files()
    if not selected_file_paths:
        logger.info('No Selected Files to Migrate')
        return

    logger.info(f'Total of {len(selected_file_paths)} file(s) selected for potential migration job.')

    snowflake_ddl_params.spark = spark
    snowflake_ddl_params.snowflake_connection = connect_to_snowflake()

    recursive_migrate_all_files(
        source_path = data_settings.source_path,
        fn_extract_file_meta = fn_extract_file_meta,
        additional_file_meta_columns = additional_file_meta_columns,
        fn_process_file = fn_process_file,
        fn_get_dtypes = fn_get_dtypes,
        selected_file_paths = selected_file_paths,
        )

    write_DDL_file_per_step()

    snowflake_ddl_params.snowflake_connection.close()



# %%




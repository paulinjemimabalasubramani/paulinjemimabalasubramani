"""
Common Library for migrating any type of data from any source to Azure ADLS Gen 2 and Snowflake.

"""

# %% Import Libraries

import os, sys, tempfile, shutil, pymssql
from datetime import datetime
from collections import OrderedDict
from uuid import uuid4

from .common_functions import logger, catch_error, is_pc, data_settings, EXECUTION_DATE_str, cloud_file_hist_conf, \
    pipeline_metadata_conf, execution_date_start, to_sql_value, get_csv_rows, get_secrets
from .azure_functions import save_adls_gen2, setup_spark_adls_gen2_connection
from .spark_functions import ELT_PROCESS_ID_str, partitionBy_str, elt_audit_columns, write_snowflake
from .snowflake_ddl import connect_to_snowflake, snowflake_ddl_params, create_snowflake_ddl, write_DDL_file_per_step



# %% Parameters

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
    ('key_column_names', 'varchar(2500) NULL'), # file_meta['key_column_names']
    ('partition_by', 'varchar(300) NULL'), #  calculated
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

    if(data_settings.get_value('include_file_name_in_process_check',None)):
        with_load_n_date.append('file_name')
        logger.info(f'with_load_n_date, including file name -> {with_load_n_date}')

    key_column_names['with_load_n_date'] = key_column_names['with_load'] + with_load_n_date
    return key_column_names



data_settings.metadata_key_column_names = get_metadata_key_column_names()



# %% Get Primary Keys from SQL Server

@catch_error(logger)
def get_key_column_names(table_name:str, firm_name:str=''):
    """
    Get Primary Keys from SQL Server
    """
    sql_pk_table = f"{pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_primary_key']}"
    sql_ds_table = f"{pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_datasource']}"

    if firm_name:
        firm_name = firm_name.upper() + '_'
    else:
        firm_name = data_settings.pipeline_firm.upper() + '_' if data_settings.pipeline_firm else ''

    datasourcekey = data_settings.pipeline_datasourcekey.upper()

    sqlstr_get_pk = f"""SELECT LOWER(mpk.SystemPrimaryKey) AS PK
FROM {sql_ds_table} mds 
    LEFT JOIN {sql_pk_table} mpk ON  UPPER(mds.DataSourceUniqueKey) = UPPER(mpk.DataSourceUniqueKey)
WHERE
    mpk.DataSourceUniqueKey IS NOT NULL
    AND mpk.SystemPrimaryKey IS NOT NULL
    AND UPPER(mds.DataSourceKey) = '{datasourcekey}'
    AND (UPPER(mpk.AssetUniqueKey) = '{table_name.upper()}'
        OR '{firm_name}'+UPPER(mpk.AssetUniqueKey) = '{table_name.upper()}')
    ;
    """

    if is_pc:
        print(f'\n\n{sqlstr_get_pk}\n\n')

    with pymssql.connect(
        server = pipeline_metadata_conf['sql_server'],
        user = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
        database = pipeline_metadata_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr_get_pk)
            key_column_names = [row['PK'].strip().lower() for row in cursor]

    if not key_column_names:
        logger.warning(f'No Key Columns found for table {table_name} in {sql_pk_table}')
        return []

    flatten = lambda t: [item.strip() for sublist in t for item in sublist if item.strip()]
    key_column_names = [c.lower().split(',') for c in key_column_names]
    key_column_names = flatten(key_column_names)
    key_column_names = list(set(key_column_names))

    return sorted(key_column_names)



# %% Add Firm to Table Name

@catch_error(logger)
def add_firm_to_table_name(table_name:str):
    """
    Add Firm to Table Name
    """
    if data_settings.pipeline_firm and hasattr(data_settings, 'add_firm_to_table_name') and data_settings.add_firm_to_table_name.upper() == 'TRUE':
        prefix = data_settings.pipeline_firm.lower() + '_'
        if not table_name.lower().startswith(prefix):
            table_name = prefix + table_name.lower()

    return table_name.lower()


# %% Check if file_meta table exists

@catch_error(logger)
def check_if_file_meta_table_exists():
    """
    Check if file_meta table exists
    """
    sqlstr_table_exists = f"""SELECT COUNT(*) AS CNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{cloud_file_hist_conf['sql_schema'].upper()}'
        AND UPPER(TABLE_TYPE) = 'BASE TABLE'
        AND UPPER(TABLE_NAME) = '{cloud_file_hist_conf['sql_file_history_table'].upper()}'
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
            return int(row['CNT']) > 0



# %% Create file_meta table in SQL Server if not exists

@catch_error(logger)
def create_file_meta_table_if_not_exists(additional_file_meta_columns:list):
    """
    Create file_meta table in SQL Server if not exists
    """
    if check_if_file_meta_table_exists(): return

    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()
    logger.info(f'{full_table_name} table does not exist in SQL server. Creating new table.')

    file_meta_columns = 'id int identity' + ''.join([f', {c[0]} {c[1]}' for c in cloud_file_history_columns + additional_file_meta_columns])
    create_table_sqlstr = f'CREATE TABLE {full_table_name} ({file_meta_columns});'

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            conn._conn.execute_non_query(create_table_sqlstr)



# %% Default table type conversion

@catch_error(logger)
def default_table_dtypes(table, use_varchar:bool=True):
    """
    Default table type conversion
    """
    dtypes = []
    filter_columns = [c.lower() for c in elt_audit_columns] + [partitionBy_str.lower()]

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



# %% Get Data Type Translation

@catch_error(logger)
def get_data_type_translation(data_type_translation_id:str):
    """
    Get Data Type Translation
    """
    translation = dict()
    for row in get_csv_rows(csv_file_path=data_settings.data_type_translation_file):
        if row['datatypetranslationid'] == data_type_translation_id.lower():
            translation[row['datatypefrom']] = row['datatypeto']

    return translation



# %% Check if file_meta exists in SQL server File History

@catch_error(logger)
def file_meta_exists_in_history(file_meta:dict):
    """
    Check if file_meta exists in SQL server File History
    """
    if not check_if_file_meta_table_exists(): return False

    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()

    key_columns = []
    for c in data_settings.metadata_key_column_names['with_load_n_date']:
        if file_meta.get(c,False):
            cval = to_sql_value(file_meta[c])
            key_columns.append(f"{c}='{cval}'")
    key_columns = ' AND '.join(key_columns)
    sqlstr_meta_exists = f'SELECT COUNT(*) AS CNT FROM {full_table_name} WHERE {key_columns} AND reingest_file = 0;'

    logger.info(f'file_meta_exists_in_history -> sqlstr_meta_exists -> {sqlstr_meta_exists}')

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



# %% Check if file_meta exists in SQL server File History for initial File Selection

@catch_error(logger)
def file_meta_exists_for_select_files(file_path:str):
    """
    Check if file_meta exists in SQL server File History for initial File Selection
    """
    if not check_if_file_meta_table_exists(): return False

    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()

    sqlstr_meta_exists = f"""SELECT COUNT(*) AS CNT FROM {full_table_name}
        WHERE reingest_file = 0 AND
            ('{to_sql_value(file_path)}' = file_path OR ('{to_sql_value(file_path)}' = zip_file_path AND zip_file_fully_ingested = 1))
        ;"""

    sqlstr_reingest_file_exists = f"""SELECT COUNT(*) AS CNT FROM {full_table_name}
        WHERE reingest_file = 1 AND
            ('{to_sql_value(file_path)}' = file_path OR '{to_sql_value(file_path)}' = zip_file_path)
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
            cursor.execute(sqlstr_meta_exists)
            row = cursor.fetchone()
            if int(row['CNT']) == 0: return False

            cursor.execute(sqlstr_reingest_file_exists)
            row = cursor.fetchone()
            return int(row['CNT']) == 0



# %% Write file_meta to SQL server File History

@catch_error(logger)
def write_file_meta_to_history(file_meta:dict, additional_file_meta_columns:list):
    """
    Write file_meta to SQL server File History
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()
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
        if file_meta.get(c,False):
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
def migrate_single_file(file_path:str, zip_file_path:str, fn_extract_file_meta, fn_process_file, fn_get_dtypes, additional_file_meta_columns:list, skip_datalake:bool=False):
    """
    Migrate Single File
    """
    logger.info(f'Extract File Meta: {file_path}')
    start_total_seconds = datetime.now()

    file_meta = fn_extract_file_meta(file_path=file_path, zip_file_path=zip_file_path)
    if not file_meta or (data_settings.key_datetime > file_meta['key_datetime']): return

    if file_meta['file_path'] and os.path.isfile(file_meta['file_path']):
        sys.app.error_file = file_meta['file_path']
        file_size = os.path.getsize(file_meta['file_path'])
    else:
        sys.app.error_file = file_meta['table_name']
        file_size = 0

    partition_by = '_'.join([datetime.strftime(file_meta['key_datetime'], r'%Y_%m_%d_%H_%M_%S'), uuid4().hex])

    file_meta = {
        **file_meta,
        'database_name': data_settings.domain_name,
        'schema_name': data_settings.schema_name,
        'firm_name': data_settings.pipeline_firm.upper(),
        EXECUTION_DATE_str.lower(): execution_date_start,
        ELT_PROCESS_ID_str.lower(): data_settings.elt_process_id,
        'pipelinekey': data_settings.pipelinekey,
        'datasourcekey': data_settings.pipeline_datasourcekey,
        'file_size_kb': file_size / 1024,
        'zip_file_fully_ingested': False if zip_file_path else True,
        'reingest_file': False,
        'partition_by': partition_by,
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
    table_names = []
    key_column_names_list = []
    for table_name, (table, key_column_names) in table_list.items():
        logger.info(f'Write Table "{table_name}" in File: {file_path}')
        table_names.append(table_name)

        if key_column_names:
            key_column_names_list.append(','.join(key_column_names))
        else: 
            key_column_names_list.append('No Primary Key')

        if not skip_datalake:
            setup_spark_adls_gen2_connection(spark=snowflake_ddl_params.spark, storage_account_name=data_settings.storage_account_name) # for data
            if not save_adls_gen2(table=table, table_name=table_name, is_metadata=False): continue

            copy_commands.append(create_snowflake_ddl(table=table, table_name=table_name, fn_get_dtypes=fn_get_dtypes, partition_by=partition_by))
        else:
            _, snowflake_user, snowflake_pass = get_secrets(data_settings.snowflake_key_vault_account)

            write_snowflake(
                table = table,
                table_name = table_name,
                schema = data_settings.schema_name,
                database = snowflake_ddl_params.snowflake_database,
                warehouse = data_settings.snowflake_warehouse,
                role = data_settings.snowflake_role,
                account = data_settings.snowflake_account,
                user = snowflake_user,
                password = snowflake_pass,
                mode = 'overwrite',
                )

    file_meta['copy_command'] = ' '.join(copy_commands)
    file_meta['total_seconds'] = (datetime.now() - start_total_seconds).seconds

    file_meta['table_name'] = ','.join(table_names)
    file_meta['key_column_names'] = ','.join(key_column_names_list)
    if len(table_names)>1:
        file_meta['key_column_names'] = ','.join([f'({table_names[i]}: {key_column_names_list[i]})' for i in range(len(table_names))])

    write_file_meta_to_history(file_meta=file_meta, additional_file_meta_columns=additional_file_meta_columns)

    sys.app.table_count += len(table_list)
    sys.app.aggregate_file_size_kb += file_meta['file_size_kb']
    sys.app.error_file = None



# %% Update SQL Zip File Path - Fully Ingested to True

def update_sql_zip_file_path(zip_file_path:str):
    """
    Update SQL Zip File Path - Fully Ingested to True
    """
    full_table_name = f"{cloud_file_hist_conf['sql_schema']}.{cloud_file_hist_conf['sql_file_history_table']}".lower()

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
def recursive_migrate_all_files(source_path:str, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file, fn_get_dtypes, zip_file_path:str=None, selected_file_paths:list=[], skip_datalake:bool=False, skip_file_check:bool=False):
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

    zip_file_path_exists = True if zip_file_path else False
    for file_path in selected_file_paths:
        if not skip_file_check and not os.path.isfile(file_path):
            logger.warning(f'File not found, skipping -> {file_path}')
            continue

        file_name = os.path.basename(file_path)
        file_name_noext, file_ext = os.path.splitext(file_name)

        if file_ext.lower() == '.zip':
            with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                extract_dir = os.path.join(tmpdir, file_name_noext)
                logger.info(f'Extracting {file_path} to {extract_dir}')
                shutil.unpack_archive(filename=file_path, extract_dir=extract_dir, format='zip')
                zip_file_path = zip_file_path if zip_file_path_exists else file_path # to keep original zip file path, rather than the last zip file path
                recursive_migrate_all_files(
                    source_path = extract_dir,
                    fn_extract_file_meta = fn_extract_file_meta,
                    additional_file_meta_columns = additional_file_meta_columns,
                    fn_process_file = fn_process_file,
                    fn_get_dtypes = fn_get_dtypes,
                    zip_file_path = zip_file_path,
                    selected_file_paths = [],
                    skip_datalake = skip_datalake,
                    skip_file_check = skip_file_check,
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
            skip_datalake = skip_datalake,
            )



# %% Migrate All Files

@catch_error(logger)
def migrate_all_files(spark, fn_extract_file_meta, additional_file_meta_columns:list, fn_process_file, fn_select_files, fn_get_dtypes):
    """
    Migrate All Files
    """
    create_file_meta_table_if_not_exists(additional_file_meta_columns)

    skip_datalake = getattr(data_settings, 'is_sandbox', 'FALSE').upper() == 'TRUE'
    if skip_datalake:
        logger.info('Sandbox environment, skip writing to Data Lake')

    skip_file_check = getattr(data_settings, 'skip_file_check', 'FALSE').upper() == 'TRUE'
    if skip_file_check:
        logger.info('Skipping File Check')

    file_count, selected_file_paths = fn_select_files()

    if file_count == 0:
        logger.warning(f'No file found in the source_path: {data_settings.source_path}')
    else:
        logger.info(f'Total of {file_count} files found in {data_settings.source_path}')

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
        skip_datalake = skip_datalake,
        skip_file_check = skip_file_check,
        )

    if not skip_datalake: write_DDL_file_per_step()

    snowflake_ddl_params.snowflake_connection.close()



# %%




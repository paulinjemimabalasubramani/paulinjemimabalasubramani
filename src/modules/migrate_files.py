"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

import os, sys, re, tempfile, shutil, copy
from pprint import pprint
from collections import defaultdict
from typing import cast
from datetime import datetime

from .common_functions import logger, catch_error, is_pc, execution_date, get_secrets
from .azure_functions import select_tableinfo_columns, tableinfo_container_name, tableinfo_name, read_adls_gen2, \
    default_storage_account_name, file_format, save_adls_gen2, setup_spark_adls_gen2_connection, container_name, \
    default_storage_account_abbr, metadata_folder, azure_container_folder_path, data_folder, to_storage_account_name, \
    add_table_to_tableinfo
from .spark_functions import collect_column, read_csv, IDKeyIndicator, MD5KeyIndicator, add_md5_key, read_sql, column_regex, partitionBy, \
    metadata_DataTypeTranslation, metadata_MasterIngestList, to_string, remove_column_spaces, add_elt_columns, partitionBy_value, \
    get_sql_table_names, write_sql

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, when, to_timestamp
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window



# %% Parameters

created_datetime = execution_date
modified_datetime = execution_date

INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'.upper()
schema_table_names = ['TABLES', 'COLUMNS', 'KEY_COLUMN_USAGE', 'TABLE_CONSTRAINTS']

sql_ingestion = {
    'sql_server': 'DSQLOLTP02',
    'sql_database': 'EDIPIngestion',
    'sql_schema': 'edip',
    'sql_ingest_table_name': lambda tableinfo_source: f'{tableinfo_source.lower()}_ingest',
    'sql_key_vault_account': 'sqledipingestion',
}

_, sql_ingestion['sql_id'], sql_ingestion['sql_pass'] = get_secrets(sql_ingestion['sql_key_vault_account'].lower(), logger=logger)

tmpdirs = []



# %% Get DataTypeTranslation table

@catch_error(logger)
def get_DataTypeTranslation_table(spark, data_type_translation_id:str):
    """
    Get DataTypeTranslation table from Azure
    """
    storage_account_name = default_storage_account_name

    translation = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = metadata_folder,
        table_name = metadata_DataTypeTranslation,
        file_format = file_format
    )

    translation = translation.filter(
        (col('DataTypeTranslationID') == lit(data_type_translation_id).cast("string")) & 
        (col('IsActive') == lit(1))
    )

    if is_pc: translation.show(5)
    return translation



# %% Get Master Ingest List

@catch_error(logger)
def get_master_ingest_list(spark, tableinfo_source:str):
    """
    Get Master Ingest List from Azure - this is the list of table names to be ingested.
    """
    storage_account_name = default_storage_account_name

    master_ingest_list = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source),
        table_name = metadata_MasterIngestList,
        file_format = file_format
    )

    master_ingest_list = master_ingest_list.filter(
        col('IsActive')==lit(1)
    )

    if is_pc: master_ingest_list.show(5)
    return master_ingest_list



# %% Join master_ingest_list with sql tables

@catch_error(logger)
def join_master_ingest_list_sql_tables(master_ingest_list, sql_tables):
    """
    Join master_ingest_list with sql tables
    """
    sql_tables = sql_tables.where(col('TABLE_TYPE')==lit('BASE TABLE'))

    tables = master_ingest_list.alias('master_ingest_list').join(
        sql_tables.alias('sql_tables'),
        (F.upper(col('master_ingest_list.TABLE_NAME')) == F.upper(col('sql_tables.TABLE_NAME'))) &
        (F.upper(col('master_ingest_list.TABLE_SCHEMA')) == F.upper(col('sql_tables.TABLE_SCHEMA'))),
        how = 'left'
        ).select(
            col('master_ingest_list.TABLE_NAME'), 
            col('master_ingest_list.TABLE_SCHEMA'),
            col('sql_tables.TABLE_NAME').alias('SQL_TABLE_NAME'),
            col('sql_tables.TABLE_TYPE'),
            col('sql_tables.TABLE_CATALOG'),
        )

    if is_pc: tables.printSchema()
    if is_pc: tables.show(5)

    # Check if there is a table in the master_ingest_list that is not in the sql_tables
    null_rows = tables.filter(col('SQL_TABLE_NAME').isNull()).select(col('TABLE_NAME')).collect()
    if null_rows:
        logger.warning(f"There are some tables in master_ingest_list that are not in sql_tables: {[x[0] for x in null_rows]}")

    tables = tables.where(col('SQL_TABLE_NAME').isNotNull())
    return tables



# %% Add columns if not exists

@catch_error(logger)
def add_columns_if_not_exists(table, table_name:str, columns:dict):
    """
    Add columns with default values to the table if column does not exit.
    """
    COLUMNS = [c.upper() for c in table.columns]
    for column_name, column_value in columns.items():
        if column_name.upper() not in COLUMNS:
            logger.warning(f'{column_name} is not found in {table_name}, adding the column with value = {column_value}')
            table = table.withColumn(column_name, column_value)
    return table



# %% filter columns by selected tables

@catch_error(logger)
def filter_columns_by_tables(sql_columns, tables):
    """
    Filter COLUMNS table by selected tables in the TABLES table
    """
    columns = tables.join(
        sql_columns.alias('sql_columns'),
        (F.upper(tables.TABLE_NAME) == F.upper(sql_columns.TABLE_NAME)) &
        (F.upper(tables.TABLE_SCHEMA) == F.upper(sql_columns.TABLE_SCHEMA)) &
        (F.upper(tables.TABLE_CATALOG) == F.upper(sql_columns.TABLE_CATALOG)),
        how = 'left'
    ).select('sql_columns.*').where(col('TABLE_NAME').isNotNull())

    if is_pc: columns.printSchema()
    return columns



# %% Join with table constraints and column usage

@catch_error(logger)
def join_tables_with_constraints(columns, sql_table_constraints, sql_key_column_usage):
    """
    Join COLUMNS table with table constraints and column usage
    """
    if (sql_table_constraints and sql_key_column_usage and
        ('TABLE_NAME' in sql_table_constraints.columns) and
        ('TABLE_SCHEMA' in sql_table_constraints.columns) and
        ('TABLE_CATALOG' in sql_table_constraints.columns) and
        ('CONSTRAINT_TYPE' in sql_table_constraints.columns) and
        ('CONSTRAINT_NAME' in sql_table_constraints.columns) and
        ('TABLE_NAME' in sql_key_column_usage.columns) and
        ('TABLE_SCHEMA' in sql_key_column_usage.columns) and
        ('TABLE_CATALOG' in sql_key_column_usage.columns) and
        ('COLUMN_NAME' in sql_key_column_usage.columns) and
        ('CONSTRAINT_NAME' in sql_key_column_usage.columns)
        ):

        constraints = (sql_table_constraints
            .where(col('CONSTRAINT_TYPE')==lit('PRIMARY KEY'))
            .alias('constraints')
            .join(sql_key_column_usage.alias('usage'),
                (F.upper(col('constraints.TABLE_NAME')) == F.upper(col('usage.TABLE_NAME'))) &
                (F.upper(col('constraints.TABLE_SCHEMA')) == F.upper(col('usage.TABLE_SCHEMA'))) &
                (F.upper(col('constraints.TABLE_CATALOG')) == F.upper(col('usage.TABLE_CATALOG'))) &
                (F.upper(col('constraints.CONSTRAINT_NAME')) == F.upper(col('usage.CONSTRAINT_NAME'))),
                how = 'inner')
            .select('constraints.*', 'usage.COLUMN_NAME')
            .distinct()
            )
        if is_pc: constraints.printSchema()

        columns = columns.alias('columns').join(
            constraints.alias('constraints'),
            (F.upper(columns.TABLE_NAME) == F.upper(constraints.TABLE_NAME)) &
            (F.upper(columns.TABLE_SCHEMA) == F.upper(constraints.TABLE_SCHEMA)) &
            (F.upper(columns.TABLE_CATALOG) == F.upper(constraints.TABLE_CATALOG)) &
            (F.upper(columns.COLUMN_NAME) == F.upper(constraints.COLUMN_NAME)),
            how = 'left'
            ).select(
                'columns.*', 
                col('constraints.COLUMN_NAME').alias('KEY_COLUMN_NAME')
                )
    else:
        logger.warning(f'{INFORMATION_SCHEMA}.TABLE_CONSTRAINTS and/or {INFORMATION_SCHEMA}.KEY_COLUMN_USAGE are not found, using default no constraints')
        columns = columns.withColumn('KEY_COLUMN_NAME', F.when(F.upper(col('COLUMN_NAME'))==lit(IDKeyIndicator), col('COLUMN_NAME')).otherwise(lit(None)).cast(StringType()))

        columnspk = (columns
            .where(col('KEY_COLUMN_NAME').isNotNull())
            .select(['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_CATALOG'])
            .distinct()
            )

        columnsnopk = (columns
            .groupBy(['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_CATALOG'])
            .agg(F.count('COLUMN_NAME').alias('column_count'))
            .alias('columnsnopk')
            .join(columnspk.alias('columnspk'), ['TABLE_NAME', 'TABLE_SCHEMA', 'TABLE_CATALOG'], how='left_anti')
            .select('columnsnopk.*')
            .withColumn('COLUMN_NAME', lit(MD5KeyIndicator))
            .withColumn('KEY_COLUMN_NAME', lit(MD5KeyIndicator))
            )

        columns_dict = {
            'DATA_TYPE': lit('char'),
            'CHARACTER_MAXIMUM_LENGTH': lit(0),
            'NUMERIC_PRECISION': lit(0),
            'NUMERIC_SCALE': lit(0),
            'ORDINAL_POSITION': (col('column_count') + lit(1)),
            'IS_NULLABLE': lit('YES'),
            }

        columnsnopk = add_columns_if_not_exists(table=columnsnopk, table_name=INFORMATION_SCHEMA+'.TABLE_CONSTRAINTS', columns=columns_dict)

        columns = columns.select(columns.columns).union(columnsnopk.select(columns.columns)).distinct()

    if is_pc: columns.printSchema()
    return columns



# %% Rename Columns

@catch_error(logger)
def rename_columns(columns, storage_account_name:str, created_datetime:str, modified_datetime:str, storage_account_abbr:str):
    """
    Rename / Add columns to COLUMNS table
    """
    column_map = {
        'TABLE_CATALOG': 'SourceDatabase',
        'TABLE_SCHEMA' : 'SourceSchema',
        'TABLE_NAME'   : 'TableName',
        'COLUMN_NAME'  : 'SourceColumnName',
        'DATA_TYPE'    : 'SourceDataType',
        'CHARACTER_MAXIMUM_LENGTH': 'SourceDataLength',
        'NUMERIC_PRECISION': 'SourceDataPrecision',
        'NUMERIC_SCALE': 'SourceDataScale',
        'ORDINAL_POSITION': 'OrdinalPosition',
    }

    for key, val in column_map.items():
        columns = columns.withColumnRenamed(key, val)

    columns = columns.withColumn('IsNullable', F.when(F.upper(col('IS_NULLABLE'))=='YES', lit(1)).otherwise(lit(0)).cast(IntegerType()))
    columns = columns.withColumn('KeyIndicator', F.when(col('SourceColumnName')==col('KEY_COLUMN_NAME'), lit(1)).otherwise(lit(0)).cast(IntegerType()))
    columns = columns.withColumn('CleanType', col('SourceDataType'))
    columns = columns.withColumn('StorageAccount', lit(storage_account_name).cast(StringType()))
    columns = columns.withColumn('StorageAccountAbbr', lit(storage_account_abbr).cast(StringType()))
    columns = columns.withColumn('TargetColumnName', F.regexp_replace(F.trim(col('SourceColumnName')), column_regex, '_'))
    columns = columns.withColumn('IsActive', lit(1).cast(IntegerType()))
    columns = columns.withColumn('CreatedDateTime', lit(created_datetime).cast(StringType()))
    columns = columns.withColumn('ModifiedDateTime', lit(modified_datetime).cast(StringType()))
    columns = columns.withColumn(partitionBy, lit(partitionBy_value).cast(StringType()))

    columns = columns.withColumn('SourceColumnName', F.regexp_replace(F.trim(col('SourceColumnName')), column_regex, '_'))

    if is_pc: columns.printSchema()
    return columns



# %% Add TargetDataType

@catch_error(logger)
def add_TargetDataType(columns, translation):
    """
    Add TargetDataType to COLUMNS table from TRANSLATION table
    """
    columns = columns.alias('columns').join(
        translation,
        columns.CleanType == translation.DataTypeFrom,
        how = 'left'
        ).select(
            'columns.*', 
            translation.DataTypeTo.alias('TargetDataType')
        )

    if is_pc: columns.printSchema()
    return columns



# %% Add Precision

@catch_error(logger)
def add_precision(columns):
    """
    Add precition information to COLUMNS table
    """
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['varchar'])) & (col('SourceDataLength')>0) & (col('SourceDataLength')<=255), F.concat(lit('varchar('), col('SourceDataLength'), lit(')'))).otherwise(col('TargetDataType')))
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['decimal'])) & (col('SourceDataPrecision')>0), F.concat(lit('decimal('), col('SourceDataPrecision'), lit(','), col('SourceDataScale'), lit(')'))).otherwise(col('TargetDataType')))

    if is_pc: columns.printSchema()
    return columns



# %% Get Files Meta

@catch_error(logger)
def get_files_meta(data_path_folder:str, default_schema:str='dbo'):
    """
    Get Files Metadata from given data path
    """
    files_meta = []
    for root, dirs, files in os.walk(data_path_folder):
        for file in files:
            file_name, file_ext = os.path.splitext(file)
            if (file_ext.lower() in ['.txt', '.csv']):
                file_meta = {
                    'file': file,
                    'root': root,
                    'path': os.path.join(root, file)
                }

                if file_name.upper().startswith(INFORMATION_SCHEMA + '_'):
                    schema = INFORMATION_SCHEMA
                    table = file_name[len(INFORMATION_SCHEMA)+1:].upper()
                elif '_' in file_name:
                    _loc = file_name.find("_")
                    schema = file_name[:_loc].lower()
                    table = file_name[_loc+1:].lower()
                else:
                    schema = default_schema.lower()
                    table = file_name.lower()

                file_meta = {
                    **file_meta,
                    'schema': re.sub(column_regex, '_', schema.strip()),
                    'table': re.sub(column_regex, '_', table.strip()),
                }

                if schema !='' and table !='':
                    files_meta.append(file_meta)

    if is_pc: pprint(files_meta)
    return files_meta



# %% Create Master Ingest List

@catch_error(logger)
def create_master_ingest_list(spark, files_meta):
    """
    Create Master Ingest List table for the files from their metadata
    """
    files_meta_for_master_ingest_list =[{
        'TABLE_SCHEMA': file_meta['schema'],
        'TABLE_NAME': file_meta['table'],
        } for file_meta in files_meta if file_meta['schema'].upper()!=INFORMATION_SCHEMA]

    if not files_meta_for_master_ingest_list:
        logger.warning('No tables found, exiting program.')
        exit()

    master_ingest_list = spark.createDataFrame(files_meta_for_master_ingest_list)

    logger.info(f'Total of {master_ingest_list.count()} tables to ingest')
    return master_ingest_list



# %% create INFORMATION_SCHEMA.TABLES if not exists

@catch_error(logger)
def create_INFORMATION_SCHEMA_TABLES_if_not_exists(sql_tables, master_ingest_list, tableinfo_source:str):
    """
    Create INFORMATION_SCHEMA.TABLES if not exists
    """
    if not (sql_tables and ('TABLE_NAME' in sql_tables.columns) and ('TABLE_SCHEMA' in sql_tables.columns)):
        logger.warning(f'{INFORMATION_SCHEMA}.TABLES is not found, ingesting all tables by default')
        sql_tables = master_ingest_list.select(['TABLE_NAME', 'TABLE_SCHEMA'])

    columns = {
        'TABLE_TYPE': lit('BASE TABLE'),
        'TABLE_CATALOG': lit(tableinfo_source),
        }

    sql_tables = add_columns_if_not_exists(table=sql_tables, table_name=INFORMATION_SCHEMA+'.TABLES', columns=columns)

    return sql_tables



# %% create INFORMATION_SCHEMA.COLUMNS if not exists

@catch_error(logger)
def create_INFORMATION_SCHEMA_COLUMNS_if_not_exists(spark, sql_columns, tableinfo_source:str, files_meta):
    """
    Create INFORMATION_SCHEMA.COLUMNS if not exists
    """
    if not (sql_columns and 
        ('TABLE_NAME' in sql_columns.columns) and 
        ('TABLE_SCHEMA' in sql_columns.columns) and
        ('COLUMN_NAME' in sql_columns.columns)
        ):
        logger.warning(f'{INFORMATION_SCHEMA}.TABLES is not found, ingesting all tables by default')
        columns_list = []
        for file_meta in files_meta:
            if file_meta['schema'].upper()!=INFORMATION_SCHEMA:
                csv_table = read_csv(spark=spark, file_path=file_meta['path'])
                for column_name in csv_table.columns:
                    columns_list.append({
                        'TABLE_NAME': file_meta['table'],
                        'TABLE_SCHEMA': file_meta['schema'],
                        'COLUMN_NAME': column_name,
                    })
        sql_columns = spark.createDataFrame(columns_list)

    columns = {
        'TABLE_CATALOG': lit(tableinfo_source),
        'DATA_TYPE': lit('char'),
        'CHARACTER_MAXIMUM_LENGTH': lit(0),
        'NUMERIC_PRECISION': lit(0),
        'NUMERIC_SCALE': lit(0),
        'ORDINAL_POSITION': row_number().over(Window.partitionBy(['TABLE_NAME', 'TABLE_SCHEMA']).orderBy(col('COLUMN_NAME').asc())),
        'IS_NULLABLE': lit('YES'),
        }

    sql_columns = add_columns_if_not_exists(table=sql_columns, table_name=INFORMATION_SCHEMA+'.COLUMNS', columns=columns)

    return sql_columns



# %% Get Table and Column Metadata from information_schema

@catch_error(logger)
def get_sql_schema_tables_from_files(spark, files_meta, tableinfo_source:str, master_ingest_list):
    """
    Get Table and Column Metadata from information_schema table
    """
    schemas_meta = [file_meta for file_meta in files_meta if file_meta['schema'].upper()==INFORMATION_SCHEMA]
    sql_meta = {schema_table_name: [schema_meta for schema_meta in schemas_meta if schema_meta['table'].upper()==schema_table_name.upper()] for schema_table_name in schema_table_names}

    schema_tables = defaultdict()
    for schema_table_name in schema_table_names:
        schema_meta = sql_meta[schema_table_name]
        if schema_meta:
            schema_table = read_csv(spark=spark, file_path=schema_meta[0]['path'])
            if is_pc: schema_table.printSchema()
        else:
            schema_table = None
        schema_tables[schema_table_name] = schema_table

    schema_tables['TABLES'] = create_INFORMATION_SCHEMA_TABLES_if_not_exists(sql_tables=schema_tables['TABLES'], master_ingest_list=master_ingest_list, tableinfo_source=tableinfo_source)
    schema_tables['COLUMNS'] = create_INFORMATION_SCHEMA_COLUMNS_if_not_exists(spark=spark, sql_columns=schema_tables['COLUMNS'], tableinfo_source=tableinfo_source, files_meta=files_meta)


    return schema_tables



# %% Prepare TableInfo

@catch_error(logger)
def prepare_tableinfo(master_ingest_list, translation, sql_tables, sql_columns, sql_table_constraints, sql_key_column_usage, storage_account_name:str, storage_account_abbr:str):
    """
    Prepare TableInfo table from given master_ingest_list and schema tables
    """

    logger.info('Join master_ingest_list with sql tables')
    tables = join_master_ingest_list_sql_tables(master_ingest_list=master_ingest_list, sql_tables=sql_tables)
    if is_pc: tables.show(5)

    logger.info('Filter columns by selected tables')
    columns = filter_columns_by_tables(sql_columns=sql_columns, tables=tables)
    if is_pc: columns.show(5)

    logger.info('Join with table constraints and column usage')
    columns = join_tables_with_constraints(columns=columns, sql_table_constraints=sql_table_constraints, sql_key_column_usage=sql_key_column_usage)
    if is_pc: columns.show(5)

    logger.info('Rename Columns')
    columns = rename_columns(columns=columns, storage_account_name=storage_account_name, created_datetime=created_datetime, modified_datetime=modified_datetime, storage_account_abbr=storage_account_abbr)
    if is_pc: columns.show(5)

    logger.info('Add TargetDataType')
    columns = add_TargetDataType(columns=columns, translation=translation)
    if is_pc: columns.show(5)

    logger.info('Add Precision')
    columns = add_precision(columns=columns)
    if is_pc: columns.show(5)

    logger.info('Select Relevant columns only')
    tableinfo = select_tableinfo_columns(tableinfo=columns)
    if is_pc: tableinfo.show(5)

    return tableinfo



# %% Get Table and Column Metadata from information_schema

@catch_error(logger)
def get_sql_schema_tables(spark, sql_id:str, sql_pass:str, sql_server:str, sql_database:str):
    """
    Get Table and Column Metadata from information_schema
    """
    schema_tables = defaultdict()
    for schema_table_name in schema_table_names:
        schema_tables[schema_table_name] = read_sql(spark=spark, user=sql_id, password=sql_pass, schema=INFORMATION_SCHEMA, table_name=schema_table_name, database=sql_database, server=sql_server)
        if is_pc: schema_tables[schema_table_name].printSchema()
        if is_pc: schema_tables[schema_table_name].show(5)
    
    return schema_tables



# %% Make TableInfo

@catch_error(logger)
def make_tableinfo(spark, ingest_from_files_flag:bool, data_path_folder:str, default_schema:str, tableinfo_source:str, \
    data_type_translation_id:str, sql_id:str, sql_pass:str, sql_server:str, sql_database:str):
    """
    Make TableInfo and save it to Azure
    """
    storage_account_name = default_storage_account_name
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    translation = get_DataTypeTranslation_table(spark=spark, data_type_translation_id=data_type_translation_id)

    if ingest_from_files_flag:
        files_meta = get_files_meta(data_path_folder=data_path_folder, default_schema=default_schema)
        if not files_meta:
            logger.warning('No files found, exiting program.')
            exit()
        master_ingest_list = create_master_ingest_list(spark=spark, files_meta=files_meta)
        schema_tables = get_sql_schema_tables_from_files(spark=spark, files_meta=files_meta, tableinfo_source=tableinfo_source, master_ingest_list=master_ingest_list)
    else:
        files_meta = []
        master_ingest_list = get_master_ingest_list(spark=spark, tableinfo_source=tableinfo_source)
        schema_tables = get_sql_schema_tables(spark=spark, sql_id=sql_id, sql_pass=sql_pass, sql_server=sql_server, sql_database=sql_database)

    tableinfo = prepare_tableinfo(
        master_ingest_list = master_ingest_list,
        translation = translation,
        sql_tables = schema_tables['TABLES'],
        sql_columns = schema_tables['COLUMNS'],
        sql_table_constraints = schema_tables['TABLE_CONSTRAINTS'],
        sql_key_column_usage = schema_tables['KEY_COLUMN_USAGE'],
        storage_account_name = storage_account_name,
        storage_account_abbr = default_storage_account_abbr,
        )

    save_adls_gen2(
            table = tableinfo,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source),
            table_name = tableinfo_name,
            partitionBy = partitionBy,
            file_format = file_format,
        )

    return files_meta, tableinfo



# %% Keep same column case sensitivity between tableinfo and actual table columns

@catch_error(logger)
def keep_same_case_sensitive_column_names(tableinfo, database:str, schema:str, table_name:str, sql_table):
    """
    Keep same column case sensitivity between tableinfo and actual table columns
    """
    tableinfo_filtered = tableinfo.where(
        (col('SourceDatabase')==lit(database)) &
        (col('SourceSchema')==lit(schema)) &
        (col('TableName')==lit(table_name))
        )

    tableinfo_SourceColumnName = collect_column(table=tableinfo_filtered, column_name='SourceColumnName', distinct=True)

    sql_table_columns_lower = {c.lower():c for c in sql_table.columns}
    column_map = {tc:sql_table_columns_lower[tc.lower()] for tc in tableinfo_SourceColumnName}

    for new_name, existing_name in column_map.items():
        sql_table = sql_table.withColumnRenamed(existing_name, new_name)

    return sql_table



# %% Loop over all tables

@catch_error(logger)
def iterate_over_all_tables_migration(spark, tableinfo, table_rows, files_meta:list, ingest_from_files_flag:bool, sql_id:str,
                                    sql_pass:str, sql_server:str, storage_account_name:str, tableinfo_source:str):
    """
    Loop over all tables to migrate them to Azure
    """
    PARTITION_list = defaultdict(str)
    table_count = len(table_rows)

    for i, r in enumerate(table_rows):
        database = r['SourceDatabase']
        schema = r['SourceSchema']
        table_name = r['TableName']
        logger.info(f"Table {i+1} of {table_count}: {schema}.{table_name}")

        container_folder = azure_container_folder_path(data_type=data_folder, domain_name=sys.domain_name, source_or_database=database, firm_or_schema=schema)

        if ingest_from_files_flag:
            file_path = [file_meta for file_meta in files_meta if file_meta['table'].lower()==table_name.lower() and file_meta['schema'].lower()==schema.lower()][0]['path']
            sql_table = read_csv(spark=spark, file_path=file_path)
            sql_table = add_md5_key(sql_table)
        else:
            sql_table = read_sql(spark=spark, user=sql_id, password=sql_pass, schema=schema, table_name=table_name, database=database, server=sql_server)

        sql_table = remove_column_spaces(sql_table)
        sql_table = to_string(sql_table, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.
        sql_table = keep_same_case_sensitive_column_names(tableinfo=tableinfo, database=database, schema=schema, table_name=table_name, sql_table=sql_table)
        sql_table = add_elt_columns(
            table = sql_table,
            reception_date = execution_date,
            source = tableinfo_source,
            is_full_load = True,
            dml_type = 'I',
            )

        userMetadata = save_adls_gen2(
            table = sql_table,
            storage_account_name = storage_account_name,
            container_name = container_name,
            container_folder = container_folder,
            table_name = table_name,
            partitionBy = partitionBy,
            file_format = file_format
        )

        PARTITION_list[(sys.domain_name, database, schema, table_name, storage_account_name)] = userMetadata

    logger.info('Finished Migrating All Tables')
    return PARTITION_list



# %% Get SQL Ingest Table

@catch_error(logger)
def get_sql_ingest_table(spark, tableinfo_source):
    """
    Get SQL Ingest Table - the history of files ingested in SQL Server
    """
    table_names, sql_tables = get_sql_table_names(
        spark = spark,
        schema = sql_ingestion['sql_schema'],
        database = sql_ingestion['sql_database'],
        server = sql_ingestion['sql_server'],
        user = sql_ingestion['sql_id'],
        password = sql_ingestion['sql_pass'],
        )

    sql_ingest_table_name = sql_ingestion['sql_ingest_table_name'](tableinfo_source)
    sql_ingest_table_exists = sql_ingest_table_name in table_names

    sql_ingest_table = None
    if sql_ingest_table_exists:
        sql_ingest_table = read_sql(
            spark = spark,
            user = sql_ingestion['sql_id'],
            password = sql_ingestion['sql_pass'],
            schema = sql_ingestion['sql_schema'],
            table_name = sql_ingest_table_name,
            database = sql_ingestion['sql_database'],
            server = sql_ingestion['sql_server'],
            )

    return sql_ingest_table



# %% Write SQL Ingest Table

@catch_error(logger)
def write_sql_ingest_table(sql_ingest_table, tableinfo_source, mode:str='append'):
    """
    Write SQL Ingest Table - the history of files ingested
    """
    write_sql(
        table = sql_ingest_table,
        table_name = sql_ingestion['sql_ingest_table_name'](tableinfo_source),
        schema = sql_ingestion['sql_schema'],
        database = sql_ingestion['sql_database'],
        server = sql_ingestion['sql_server'],
        user = sql_ingestion['sql_id'],
        password = sql_ingestion['sql_pass'],
        mode = mode,
    )



# %% Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.

@catch_error(logger)
def save_tableinfo_dict_and_sql_ingest_table(spark, tableinfo:defaultdict, tableinfo_source:str, all_new_files, save_tableinfo_adls_flag:bool=True):
    """
    Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.
    """
    if not tableinfo:
        logger.warning('No data in TableInfo --> Skipping write to Azure')
        return

    tableinfo_values = list(tableinfo.values())

    list_of_dict = []
    for vi in range(len(tableinfo_values[0])):
        list_of_dict.append({k:v[vi] for k, v in tableinfo.items()})

    meta_tableinfo = spark.createDataFrame(list_of_dict)
    meta_tableinfo = select_tableinfo_columns(tableinfo=meta_tableinfo)

    storage_account_name = default_storage_account_name # keep default storage account name for tableinfo
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    if save_tableinfo_adls_flag:
        save_adls_gen2(
                table = meta_tableinfo,
                storage_account_name = storage_account_name,
                container_name = tableinfo_container_name,
                container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source),
                table_name = tableinfo_name,
                partitionBy = partitionBy,
                file_format = file_format,
            )

        if all_new_files: # Write ingest metadata to SQL so that the old files are not re-ingested next time.
            write_sql_ingest_table(sql_ingest_table=all_new_files, tableinfo_source=tableinfo_source, mode='append')

    return meta_tableinfo



# %% Remove temporary folders

@catch_error(logger)
def cleanup_tmpdirs():
    """
    Remove temporary folders created by unzip process
    """
    global tmpdirs
    while len(tmpdirs)>0:
        tmpdirs.pop(0).cleanup()



# %% Process Single File

@catch_error(logger)
def process_one_file(file_meta, fn_process_file):
    """
    Process Single File - Unzipped or ZIP file
    """
    global tmpdirs
    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    logger.info(f'Processing {file_path}')

    if file_path.lower().endswith('.zip'):
        tmpdir = tempfile.TemporaryDirectory(dir=os.path.dirname(file_path))
        tmpdirs.append(tmpdir)
        logger.info(f'Extracting {file_path} to {tmpdir.name}')
        shutil.unpack_archive(filename=file_path, extract_dir=tmpdir.name)
        for root1, dirs1, files1 in os.walk(tmpdir.name):
            for file1 in files1:
                file_meta1 = copy.deepcopy(file_meta)
                file_meta1['folder_path'] = root1
                file_meta1['file_name'] = file1
                return fn_process_file(file_meta=file_meta1)
    else:
        return fn_process_file(file_meta=file_meta)



# %% Get Selected Files

@catch_error(logger)
def get_selected_files(ingest_table, sql_ingest_table, key_column_names, date_column_name:str):
    """
    Select only the files that need to be ingested this time.
    """
    # Union all files
    new_files = ingest_table.alias('t'
        ).join(sql_ingest_table, ingest_table[MD5KeyIndicator]==sql_ingest_table[MD5KeyIndicator], how='left_anti'
        ).select('t.*')

    union_columns = new_files.columns
    all_files = new_files.select(union_columns).union(sql_ingest_table.select(union_columns)).sort(key_column_names['with_load_n_date'])

    # Filter out old files
    full_files = all_files.where('is_full_load').withColumn('row_number', 
            row_number().over(Window.partitionBy(key_column_names['with_load']).orderBy(col(date_column_name).desc(), col(MD5KeyIndicator).desc()))
        ).where(col('row_number')==lit(1))

    all_files = all_files.alias('a').join(full_files.alias('f'), key_column_names['base'], how='left').select('a.*', col(f'f.{date_column_name}').alias('max_date')) \
        .withColumn('full_load_date', when(col('full_load_date').isNull(), col('max_date')).otherwise(col('full_load_date'))).drop('max_date')

    all_files = all_files.withColumn('is_ingested', 
            when(col('ingestion_date').isNull() & col('full_load_date').isNotNull() & (col(date_column_name)<col('full_load_date')), lit(False)).otherwise(col('is_ingested'))
        )

    # Select and Order Files for ingestion
    new_files = all_files.where(col('ingestion_date').isNull()).withColumn('ingestion_date', to_timestamp(lit(execution_date)))
    selected_files = new_files.where('is_ingested').orderBy(col('firm_crd_number').desc(), col('table_name').desc(), col('is_full_load').desc(), col(date_column_name).asc())

    return new_files, selected_files



# %% Write list of tables to Azure

@catch_error(logger)
def write_table_list_to_azure(PARTITION_list:defaultdict, table_list:dict, tableinfo, tableinfo_source, firm_name:str, storage_account_name:str, storage_account_abbr:str, data_path_folder:str, save_data_to_adls_flag:bool):
    """
    Write all tables in table_list to Azure
    """
    if not table_list:
        logger.warning(f"No data to write")
        return PARTITION_list, tableinfo

    for table_name, table in table_list.items():
        logger.info(f'Writing {table_name} to Azure...')

        add_table_to_tableinfo(
            tableinfo = tableinfo, 
            table = table, 
            schema_name = firm_name, 
            table_name = table_name, 
            tableinfo_source = tableinfo_source, 
            storage_account_name = storage_account_name,
            storage_account_abbr = storage_account_abbr,
            )

        container_folder = azure_container_folder_path(data_type=data_folder, domain_name=sys.domain_name, source_or_database=tableinfo_source, firm_or_schema=firm_name)

        if is_pc: # and manual_iteration:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{table_name}'
            pprint(fr'Save to local {local_path}')
            table.coalesce(1).write.json(path = fr'{local_path}.json', mode='overwrite')

        if save_data_to_adls_flag:
            userMetadata = save_adls_gen2(
                table = table,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table_name = table_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

            PARTITION_list[(sys.domain_name, tableinfo_source, firm_name, table_name, storage_account_name)] = userMetadata

    logger.info('Done writing to Azure')
    return PARTITION_list, tableinfo



# %% Get Meta Data from file

@catch_error(logger)
def get_file_meta(file_path:str, firm_crd_number:str, fn_extract_file_meta):
    """
    Get Meta Data from file
    Handles Zip files extraction as well
    """
    if file_path.lower().endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            k = 0
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    file_path1 = os.path.join(root1, file1)
                    file_meta = fn_extract_file_meta(file_path=file_path1, firm_crd_number=firm_crd_number)
                    k += 1
                    break
                if k>0: break

    else:
        file_meta = fn_extract_file_meta(file_path=file_path, firm_crd_number=firm_crd_number)

    return file_meta



# %% Get all files meta data for a given folder_path

@catch_error(logger)
def get_all_file_meta(folder_path, date_start:datetime, firm_crd_number:str, fn_extract_file_meta, inclusive:bool=True):
    """
    Get all files meta data for a given folder_path.
    Filters files for dates after the given date_start
    """
    logger.info(f'Getting list of candidate files from {folder_path}')
    files_meta = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_meta = get_file_meta(file_path=file_path, firm_crd_number=firm_crd_number, fn_extract_file_meta=fn_extract_file_meta)

            if file_meta and (date_start<file_meta['run_datetime'] or (date_start==file_meta['run_datetime'] and inclusive)):
                files_meta.append(file_meta)

    logger.info(f'Finished getting list of files. Total Files = {len(files_meta)}')
    return files_meta



# %% Iterate over selected files for a given Firm and Union List of Tables by table name

@catch_error(logger)
def iterate_over_selected_files_per_firm(selected_files, fn_process_file):
    """
    Iterate over selected files for a given Firm and Union List of Tables by table name
    """
    logger.info("Iterating over Selected Files")
    table_list_union = {}
    for ingest_row in selected_files.rdd.toLocalIterator():
        file_meta = ingest_row.asDict()

        table_list = process_one_file(file_meta=file_meta, fn_process_file=fn_process_file)

        if table_list:
            for table_name, table in table_list.items():
                if table_name in table_list_union.keys():
                    table_prev = table_list_union[table_name]
                    primary_key_columns = [c for c in table_prev.columns if c.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()]]
                    if not primary_key_columns:
                        raise ValueError(f'No Primary Key Found for {table_name}')
                    table_prev = table_prev.alias('tp'
                        ).join(table, primary_key_columns, how='left_anti'
                        ).select('tp.*')
                    union_columns = table_prev.columns
                    table_prev = table_prev.select(union_columns).union(table.select(union_columns))
                    table_list_union[table_name] = table_prev.distinct()
                else:
                    table_list_union[table_name] = table.distinct()

    return table_list_union




# %% Iterate over all the files in all the firms and process them.

@catch_error(logger)
def process_all_files_with_incrementals(
    spark,
    firms:list,
    data_path_folder:str,
    fn_extract_file_meta:object,
    date_start,
    fn_ingest_table_from_files_meta:object,
    fn_process_file:object,
    key_column_names:dict,
    tableinfo_source:str,
    save_data_to_adls_flag: bool,
    date_column_name:str,
    ):

    """
    Iterate over all the files in all the firms and process them.
    """

    PARTITION_list = defaultdict(str)
    tableinfo = defaultdict(list)
    all_new_files = None

    sql_ingest_table = get_sql_ingest_table(spark=spark, tableinfo_source=tableinfo_source)

    for firm in firms:
        folder_path = os.path.join(data_path_folder, firm['crd_number'])
        logger.info(f"Firm: {firm['firm_name']}, Firm CRD Number: {firm['crd_number']}")

        if not os.path.isdir(folder_path):
            logger.warning(f'Path does not exist: {folder_path}   -> SKIPPING')
            continue

        files_meta = get_all_file_meta(folder_path=folder_path, date_start=date_start, firm_crd_number=firm['crd_number'], fn_extract_file_meta=fn_extract_file_meta)
        if not files_meta:
            continue

        storage_account_name = to_storage_account_name(firm_name=firm['storage_account_abbr'])
        setup_spark_adls_gen2_connection(spark, storage_account_name)

        logger.info('Getting New Files')
        ingest_table, sql_ingest_table = fn_ingest_table_from_files_meta(files_meta, firm_name=firm['firm_name'], storage_account_name=storage_account_name, storage_account_abbr=firm['storage_account_abbr'], sql_ingest_table=sql_ingest_table)
        new_files, selected_files = get_selected_files(ingest_table=ingest_table, sql_ingest_table=sql_ingest_table, key_column_names=key_column_names, date_column_name=date_column_name)

        logger.info(f'Total of {new_files.count()} new file(s). {selected_files.count()} eligible for data migration.')

        if all_new_files:
            union_columns = new_files.columns
            all_new_files = all_new_files.select(union_columns).union(new_files.select(union_columns))
        else:
            all_new_files = new_files

        table_list_union = iterate_over_selected_files_per_firm(selected_files=selected_files, fn_process_file=fn_process_file)

        PARTITION_list, tableinfo = write_table_list_to_azure(
            PARTITION_list = PARTITION_list,
            table_list = table_list_union,
            tableinfo = tableinfo,
            tableinfo_source = tableinfo_source,
            firm_name = firm['firm_name'],
            storage_account_name = storage_account_name,
            storage_account_abbr = firm['storage_account_abbr'],
            data_path_folder = data_path_folder,
            save_data_to_adls_flag = save_data_to_adls_flag,
        )

        cleanup_tmpdirs()

    logger.info('Finished processing all Files and Firms')
    return all_new_files, PARTITION_list, tableinfo




# %%



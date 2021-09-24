"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

import os, sys, re
from pprint import pprint
from collections import defaultdict
from typing import cast

from .common_functions import logger, catch_error, is_pc, execution_date
from .azure_functions import select_tableinfo_columns, tableinfo_container_name, tableinfo_name, read_adls_gen2, \
    default_storage_account_name, file_format, save_adls_gen2, setup_spark_adls_gen2_connection, container_name, \
    default_storage_account_abbr, metadata_folder, azure_container_folder_path, data_folder
from .spark_functions import read_csv, IDKeyIndicator, MD5KeyIndicator, add_md5_key, read_sql, column_regex, partitionBy, \
    metadata_DataTypeTranslation, metadata_MasterIngestList, to_string, remove_column_spaces, add_elt_columns, partitionBy_value

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window



# %% Parameters

created_datetime = execution_date
modified_datetime = execution_date

INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'.upper()
schema_table_names = ['TABLES', 'COLUMNS', 'KEY_COLUMN_USAGE', 'TABLE_CONSTRAINTS']


# %% Get DataTypeTranslation table

@catch_error(logger)
def get_DataTypeTranslation_table(spark, data_type_translation_id:str):
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
    COLUMNS = [c.upper() for c in table.columns]
    for column_name, column_value in columns.items():
        if column_name.upper() not in COLUMNS:
            logger.warning(f'{column_name} is not found in {table_name}, adding the column with value = {column_value}')
            table = table.withColumn(column_name, column_value)
    return table



# %% filter columns by selected tables

@catch_error(logger)
def filter_columns_by_tables(sql_columns, tables):
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
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['varchar'])) & (col('SourceDataLength')>0) & (col('SourceDataLength')<=255), F.concat(lit('varchar('), col('SourceDataLength'), lit(')'))).otherwise(col('TargetDataType')))
    columns = columns.withColumn('TargetDataType', F.when((col('TargetDataType').isin(['decimal'])) & (col('SourceDataPrecision')>0), F.concat(lit('decimal('), col('SourceDataPrecision'), lit(','), col('SourceDataScale'), lit(')'))).otherwise(col('TargetDataType')))

    if is_pc: columns.printSchema()
    return columns



# %% Get Files Meta

@catch_error(logger)
def get_files_meta(data_path_folder:str, default_schema:str='dbo'):
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
            table_to_save = tableinfo,
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
    tableinfo_SourceColumnName = tableinfo.where(
        (col('SourceDatabase')==lit(database)) &
        (col('SourceSchema')==lit(schema)) &
        (col('TableName')==lit(table_name))
        ).select('SourceColumnName').distinct().collect()

    tableinfo_SourceColumnName = [x['SourceColumnName'] for x in tableinfo_SourceColumnName]

    sql_table_columns_lower = {c.lower():c for c in sql_table.columns}
    column_map = {tc:sql_table_columns_lower[tc.lower()] for tc in tableinfo_SourceColumnName}
    
    for new_name, existing_name in column_map.items():
        sql_table = sql_table.withColumnRenamed(existing_name, new_name)

    return sql_table




# %% Loop over all tables

@catch_error(logger)
def iterate_over_all_tables_migration(spark, tableinfo, table_rows, files_meta:list, ingest_from_files_flag:bool, sql_id:str,
                                    sql_pass:str, sql_server:str, storage_account_name:str, tableinfo_source:str):

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
            table_to_save = sql_table,
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




# %%



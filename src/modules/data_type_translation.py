"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

import os
from pprint import pprint
from collections import defaultdict
from typing import cast

from .common_functions import make_logging, catch_error
from .config import is_pc
from .data_functions import column_regex, partitionBy, partitionBy_value, execution_date, metadata_DataTypeTranslation, \
    metadata_MasterIngestList
from .azure_functions import select_tableinfo_columns, tableinfo_container_name, read_adls_gen2, to_storage_account_name, \
    file_format
from .spark_functions import read_csv, IDKeyIndicator, MD5KeyIndicator

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters

created_datetime = execution_date
modified_datetime = execution_date

INFORMATION_SCHEMA = 'INFORMATION_SCHEMA'.upper()


# %% Get DataTypeTranslation table

@catch_error(logger)
def get_DataTypeTranslation_table(spark, data_type_translation_id:str):
    storage_account_name = to_storage_account_name()

    translation = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = '',
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
    storage_account_name = to_storage_account_name()

    master_ingest_list = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = tableinfo_source,
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

    tables = master_ingest_list.join(
        sql_tables,
        (master_ingest_list.TABLE_NAME == sql_tables.TABLE_NAME) &
        (master_ingest_list.TABLE_SCHEMA == sql_tables.TABLE_SCHEMA),
        how = 'left'
        ).select(
            master_ingest_list.TABLE_NAME, 
            master_ingest_list.TABLE_SCHEMA,
            sql_tables.TABLE_NAME.alias('SQL_TABLE_NAME'),
            sql_tables.TABLE_TYPE,
            sql_tables.TABLE_CATALOG,
        )

    if is_pc: tables.printSchema()
    if is_pc: tables.show(5)

    # Check if there is a table in the master_ingest_list that is not in the sql_tables
    null_rows = tables.filter(col('SQL_TABLE_NAME').isNull()).select(col('TABLE_NAME')).collect()
    if null_rows:
        print(f"There are some tables in master_ingest_list that are not in sql_tables: {[x[0] for x in null_rows]}")

    tables = tables.where(col('SQL_TABLE_NAME').isNotNull())
    return tables



# %% Add columns if not exists

@catch_error(logger)
def add_columns_if_not_exists(table, table_name:str, columns:dict):
    for column_name, column_value in columns.keys():
        if column_name not in table.columns:
            print(f'{column_name} is not found in {table_name}, adding the column with value = {column_value}')
            table = table.withColumn(column_name, column_value)
    return table



# %% filter columns by selected tables

@catch_error(logger)
def filter_columns_by_tables(sql_columns, tables):
    columns = tables.join(
        sql_columns.alias('sql_columns'),
        (tables.TABLE_NAME == sql_columns.TABLE_NAME) &
        (tables.TABLE_SCHEMA == sql_columns.TABLE_SCHEMA) &
        (tables.TABLE_CATALOG == sql_columns.TABLE_CATALOG),
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

        constraints = sql_table_constraints.where(col('CONSTRAINT_TYPE')==lit('PRIMARY KEY')).alias('constraints') \
            .join(
            sql_key_column_usage.alias('usage'),
            (col('constraints.TABLE_CATALOG') == col('usage.TABLE_CATALOG')) &
            (col('constraints.TABLE_SCHEMA') == col('usage.TABLE_SCHEMA')) &
            (col('constraints.TABLE_NAME') == col('usage.TABLE_NAME')) &
            (col('constraints.CONSTRAINT_NAME') == col('usage.CONSTRAINT_NAME')),
            how = 'inner'
            ).select('constraints.*', 'usage.COLUMN_NAME').distinct()

        columns = columns.alias('columns').join(
            constraints.alias('constraints'),
            (columns.TABLE_NAME == constraints.TABLE_NAME) &
            (columns.TABLE_SCHEMA == constraints.TABLE_SCHEMA) &
            (columns.TABLE_CATALOG == constraints.TABLE_CATALOG) &
            (columns.COLUMN_NAME == constraints.COLUMN_NAME),
            how = 'left'
            ).select(
                'columns.*', 
                col('constraints.COLUMN_NAME').alias('KEY_COLUMN_NAME')
                )
    else:
        print(f'{INFORMATION_SCHEMA}.TABLE_CONSTRAINTS and/or {INFORMATION_SCHEMA}.KEY_COLUMN_USAGE are not found, using default no constraints')
        columnspk = columns.where(F.upper(col('COLUMN_NAME'))==lit(IDKeyIndicator)).distinct()
        columns = columns.alias('columns').join(
            columnspk.alias('columnspk'),
            (columns.TABLE_NAME == columnspk.TABLE_NAME) &
            (columns.TABLE_SCHEMA == columnspk.TABLE_SCHEMA) &
            (columns.TABLE_CATALOG == columnspk.TABLE_CATALOG) & 
            (columns.COLUMN_NAME == columnspk.COLUMN_NAME),
            how = 'left'
            ).select(
                'columns.*', 
                col('columnspk.COLUMN_NAME').alias('KEY_COLUMN_NAME'),
                )

        # TODO: add MD5 Key indicator for tables that does not have any keys.

    if is_pc: columns.printSchema()
    return columns





# %% Rename Columns

@catch_error(logger)
def rename_columns(columns, storage_account_name:str, created_datetime:str, modified_datetime:str):
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
    columns = columns.withColumn('TargetColumnName', F.regexp_replace(col('SourceColumnName'), column_regex, '_'))
    columns = columns.withColumn('IsActive', lit(1).cast(IntegerType()))
    columns = columns.withColumn('CreatedDateTime', lit(created_datetime).cast(StringType()))
    columns = columns.withColumn('ModifiedDateTime', lit(modified_datetime).cast(StringType()))
    columns = columns.withColumn(partitionBy, lit(partitionBy_value).cast(StringType()))

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
                    table = file_name[len(INFORMATION_SCHEMA)+1:].upper().strip()
                elif '_' in file_name:
                    _loc = file_name.find("_")
                    schema = file_name[:_loc].lower().strip()
                    table = file_name[_loc+1:].lower().strip()
                else:
                    schema = default_schema.strip()
                    table = file_name.strip()

                file_meta = {
                    **file_meta,
                    'schema': schema,
                    'table': table,
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
        print('No tables found, exiting program.')
        exit()

    master_ingest_list = spark.createDataFrame(files_meta_for_master_ingest_list)

    print(f'Total of {master_ingest_list.count()} tables to ingest')
    return master_ingest_list




# %% create INFORMATION_SCHEMA.TABLES if not exists

@catch_error(logger)
def create_INFORMATION_SCHEMA_TABLES_if_not_exists(sql_tables, master_ingest_list, tableinfo_source:str):

    if not (sql_tables and ('TABLE_NAME' in sql_tables.columns) and ('TABLE_SCHEMA' in sql_tables.columns)):
        print(f'{INFORMATION_SCHEMA}.TABLES is not found, ingesting all tables by default')
        sql_tables = master_ingest_list.select(['TABLE_NAME', 'TABLE_SCHEMA'])

    columns = {
        'TABLE_TYPE': lit('BASE TABLE'),
        'TABLE_CATALOG': lit(tableinfo_source),
        }

    sql_tables = add_columns_if_not_exists(table=sql_tables, table_name=INFORMATION_SCHEMA+'.TABLES', columns=columns)

    return sql_tables



# %% create INFORMATION_SCHEMA.COLUMNS if not exists

@catch_error(logger)
def create_INFORMATION_SCHEMA_COLUMNS_if_not_exists(spark, sql_columns, master_ingest_list, tableinfo_source:str, files_meta):
    if not (sql_columns and 
        ('TABLE_NAME' in sql_columns.columns) and 
        ('TABLE_SCHEMA' in sql_columns.columns) and
        ('COLUMN_NAME' in sql_columns.columns)
        ):
        print(f'{INFORMATION_SCHEMA}.TABLES is not found, ingesting all tables by default')
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
        'IsNullable': lit('YES'),
        }

    sql_columns = add_columns_if_not_exists(table=sql_columns, table_name=INFORMATION_SCHEMA+'.COLUMNS', columns=columns)

    return sql_columns



# %% Get Table and Column Metadata from information_schema

@catch_error(logger)
def get_sql_schema_tables_from_files(spark, files_meta, tableinfo_source:str, master_ingest_list):
    schema_table_names = ['TABLES', 'COLUMNS', 'KEY_COLUMN_USAGE', 'TABLE_CONSTRAINTS']

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
    schema_tables['COLUMNS'] = create_INFORMATION_SCHEMA_COLUMNS_if_not_exists(spark=spark, sql_columns=schema_tables['COLUMNS'], master_ingest_list=master_ingest_list, tableinfo_source=tableinfo_source, files_meta=files_meta)


    return schema_tables



# %% Prepare TableInfo

@catch_error(logger)
def prepare_tableinfo(master_ingest_list, translation, sql_tables, sql_columns, sql_table_constraints, sql_key_column_usage, storage_account_name:str, tableinfo_source:str):

    # Join master_ingest_list with sql tables
    tables = join_master_ingest_list_sql_tables(master_ingest_list=master_ingest_list, sql_tables=sql_tables)

    # filter columns by selected tables
    columns = filter_columns_by_tables(sql_columns=sql_columns, tables=tables)

    # Join with table constraints and column usage
    columns = join_tables_with_constraints(columns=columns, sql_table_constraints=sql_table_constraints, sql_key_column_usage=sql_key_column_usage)

    # Rename Columns
    columns = rename_columns(columns=columns, storage_account_name=storage_account_name, created_datetime=created_datetime, modified_datetime=modified_datetime)

    # Add TargetDataType
    columns = add_TargetDataType(columns=columns, translation=translation)

    # Add Precision
    columns = add_precision(columns=columns)

    # Select Relevant columns only
    tableinfo = select_tableinfo_columns(tableinfo=columns)

    return tableinfo



# %%



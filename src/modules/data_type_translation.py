"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

from .common_functions import make_logging, catch_error
from .config import is_pc
from .data_functions import column_regex, partitionBy, partitionBy_value, execution_date, metadata_DataTypeTranslation, \
    metadata_MasterIngestList
from .azure_functions import select_tableinfo_columns, tableinfo_container_name, read_adls_gen2, to_storage_account_name, \
    file_format

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType, StringType


# %% Logging
logger = make_logging(__name__)


# %% Parameters

created_datetime = execution_date
modified_datetime = execution_date



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

    return tables




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
    columns = columns.alias('columns').join(
        sql_table_constraints,
        (columns.TABLE_NAME == sql_table_constraints.TABLE_NAME) &
        (columns.TABLE_SCHEMA == sql_table_constraints.TABLE_SCHEMA) &
        (columns.TABLE_CATALOG == sql_table_constraints.TABLE_CATALOG) &
        (sql_table_constraints.CONSTRAINT_TYPE == 'PRIMARY KEY'),
        how = 'left'
        ).select(
            'columns.*', 
            sql_table_constraints.CONSTRAINT_TYPE, 
            sql_table_constraints.CONSTRAINT_NAME
            )
    
    columns = columns.alias('columns').join(
        sql_key_column_usage,
        (columns.TABLE_NAME == sql_key_column_usage.TABLE_NAME) &
        (columns.TABLE_SCHEMA == sql_key_column_usage.TABLE_SCHEMA) &
        (columns.TABLE_CATALOG == sql_key_column_usage.TABLE_CATALOG) &
        (columns.COLUMN_NAME == sql_key_column_usage.COLUMN_NAME) &
        (columns.CONSTRAINT_NAME == sql_key_column_usage.CONSTRAINT_NAME)
        ,
        how = 'left'
        ).select(
            'columns.*',
            sql_key_column_usage.COLUMN_NAME.alias('KEY_COLUMN_NAME')
        )
    
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
    columns = columns.withColumn('KeyIndicator', F.when((F.upper(col('CONSTRAINT_TYPE'))=='PRIMARY KEY') & (col('SourceColumnName')==col('KEY_COLUMN_NAME')), lit(1)).otherwise(lit(0)).cast(IntegerType()))
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



# %% Prepare TableInfo

@catch_error(logger)
def prepare_tableinfo(master_ingest_list, translation, sql_tables, sql_columns, sql_table_constraints, sql_key_column_usage, storage_account_name:str):

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





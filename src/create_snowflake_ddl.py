
# %% Import Libraries
import os, sys
from datetime import datetime

from pyspark import sql

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.data_functions import elt_audit_columns, partitionBy
from modules.config import is_pc
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, read_adls_gen2


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf, expr
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters
storage_account_name = "agaggrlakescd"
container_name = "ingress"
domain_name = 'financial_professional'
format = 'delta'

ddl_folder = f'metadata/{domain_name}/DDL'

# Steps 1-3 parameters:
variant_label = '_VARIANT'
variant_alias = 'SRC'

file_format = 'PARQUET'
wild_card = '.*.parquet'
stream_suffix = '_STREAM'

# Steps 4-6 parameters:
environment = 'QA'
snowflake_raw_database = f'{environment}_RAW_FP'
snowflake_transformed_database = f'{environment}_PERSISTENT_FP'

src_alias = 'src' # Driver Table I.E. Non-Stream
tgt_alias = 'tgt'
hash_column_name = 'MD5_HASH'
stream_alias = 'src_strm' # Stream object
view_prefix = 'VW_'
execution_date_str = 'EXECUTION_DATE'



# %% Create Session

spark = create_spark()

# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Get Column Names

@catch_error(logger)
def get_column_names(tableinfo, source_system, schema_name, table_name):
    filtered_tableinfo = tableinfo.filter(
        (col('TableName') == table_name) &
        (col('SourceSchema') == schema_name) &
        (col('SourceDatabase') == source_system)
        )
    
    column_names = filtered_tableinfo.select('TargetColumnName').rdd.flatMap(lambda x: x).collect()
    src_column_names = filtered_tableinfo.select('TargetColumnName', 'SourceColumnName').collect()
    src_column_dict = {c['TargetColumnName']:c['SourceColumnName'] for c in src_column_names}

    pk_column_names = filtered_tableinfo.filter(
        (col('KeyIndicator') == lit(1))
        ).select('TargetColumnName').rdd.flatMap(lambda x: x).collect()

    return column_names, pk_column_names, src_column_dict



# %% base sqlstr

@catch_error(logger)
def base_sqlstr(source_system):
    sqlstr = f"""USE ROLE AD_SNOWFLAKE_QA_DBA;
USE WAREHOUSE QA_RAW_WH;
USE DATABASE QA_RAW_FP;
USE SCHEMA {source_system};
"""
    return sqlstr


# %% Get partition string for a Table

@catch_error(logger)
def get_partition(source_system:str, schema_name:str, table_name:str):
    data_type = 'data'
    container_folder = f"{data_type}/{domain_name}/{source_system}/{schema_name}"

    df = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = container_folder,
        table = table_name,
        format = format
    )

    PARTITION_LIST = df.select(F.max(col(partitionBy)).alias('part')).collect()
    PARTITION = PARTITION_LIST[0]['part']
    if PARTITION:
        return PARTITION.replace(' ', '%20').replace(':', '%3A')
    else:
        print(f'{container_folder}/{table_name} is EMPTY - SKIPPING')
        return None




# %% Manual Iteration
manual_iteration = True

if not is_pc:
    manual_iteration = False

if manual_iteration:
    i = 0
    n_tables = len(table_rows)

    table = table_rows[i]
    table_name = table['TableName']
    schema_name = table['SourceSchema']
    source_system = table['SourceDatabase']
    print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

    column_names, pk_column_names, src_column_dict = get_column_names(tableinfo, source_system, schema_name, table_name)
    base_sqlstr1 = base_sqlstr(source_system)
    PARTITION = get_partition(source_system, schema_name, table_name)




# %% Create Step 1

@catch_error(logger)
def step1(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list):
    step = 1
    SCHEMA_NAME = source_system.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()
    column_list_string = '  ,'.join([f'{c} string\n' for c in column_names+elt_audit_columns])

    sqlstr = f"""{base_sqlstr}
CREATE OR REPLACE TABLE {SCHEMA_NAME}.{TABLE_NAME}
(
   {column_list_string});

CREATE OR REPLACE TABLE {SCHEMA_NAME}.{TABLE_NAME}{variant_label}
(
  {variant_alias} VARIANT
);
"""

    if manual_iteration:
        print(sqlstr)
    
    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )

if manual_iteration:
    step1(base_sqlstr1, source_system, schema_name, table_name, column_names)


# %% Create Step 2

@catch_error(logger)
def step2(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list):
    step = 2
    SCHEMA_NAME = source_system.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()

    sqlstr = f"""{base_sqlstr}
CREATE OR REPLACE STREAM {SCHEMA_NAME}.{TABLE_NAME}{variant_label}{stream_suffix}
ON TABLE {SCHEMA_NAME}.{TABLE_NAME}{variant_label};
"""

    if manual_iteration:
        print(sqlstr)

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )

if manual_iteration:
    step2(base_sqlstr1, source_system, schema_name, table_name, column_names)


# %% Create Step 3

@catch_error(logger)
def step3(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list, PARTITION:str):
    step = 3
    SCHEMA_NAME = source_system.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()

    sqlstr = f"""{base_sqlstr}
COPY INTO {SCHEMA_NAME}.{TABLE_NAME}{variant_label}
FROM @ELT_STAGE.AGGR_FP_DATALAKE/{source_system}/{schema_name}/{table_name}/{partitionBy}={PARTITION}/
FILE_FORMAT = (type='{file_format}')
PATTERN = '{wild_card}'
ON_ERROR = CONTINUE;

SET SOURCE_SYSTEM = '{SCHEMA_NAME}';
SET TARGET_TABLE = '{TABLE_NAME}{variant_label}';
SET EXCEPTION_DATE_TIME = CURRENT_TIMESTAMP();
SET EXECEPTION_CREATED_BY_USER = CURRENT_USER();
SET EXECEPTION_CREATED_BY_ROLE = CURRENT_ROLE();
SET EXCEPTION_SESSION = CURRENT_SESSION();

SELECT $SOURCE_SYSTEM;
SELECT $TARGET_TABLE;
SELECT $EXCEPTION_DATE_TIME;
SELECT $EXECEPTION_CREATED_BY_USER;
SELECT $EXECEPTION_CREATED_BY_ROLE;
SELECT $EXCEPTION_SESSION;

INSERT INTO ELT_STAGE.ELT_COPY_EXCEPTION
(
   SOURCE_SYSTEM
  ,TARGET_TABLE
  ,EXCEPTION_DATE_TIME
  ,EXECEPTION_CREATED_BY_USER
  ,EXECEPTION_CREATED_BY_ROLE
  ,EXCEPTION_SESSION
  ,ERROR
  ,FILE
  ,LINE
  ,CHARACTER
  ,BYTE_OFFSET
  ,CATEGORY
  ,CODE
  ,SQL_STATE
  ,COLUMN_NAME
  ,ROW_NUMBER
  ,ROW_START_LINE
  ,REJECTED_RECORD
)
SELECT
   $SOURCE_SYSTEM
  ,$TARGET_TABLE
  ,$EXCEPTION_DATE_TIME
  ,$EXECEPTION_CREATED_BY_USER
  ,$EXECEPTION_CREATED_BY_ROLE
  ,$EXCEPTION_SESSION
  ,ERROR
  ,FILE
  ,LINE
  ,CHARACTER
  ,BYTE_OFFSET
  ,CATEGORY
  ,CODE
  ,SQL_STATE
  ,COLUMN_NAME
  ,ROW_NUMBER
  ,ROW_START_LINE
  ,REJECTED_RECORD
FROM TABLE(validate({SCHEMA_NAME}.{TABLE_NAME}{variant_label}, job_id => '_last'));
"""

    if manual_iteration:
        print(sqlstr)

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )

if manual_iteration:
    step3(base_sqlstr1, source_system, schema_name, table_name, column_names, PARTITION)



# %% Create Step 4

@catch_error(logger)
def step4(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:list):
    step = 4
    SCHEMA_NAME = source_system.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()
    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    sqlstr = f"""{base_sqlstr}
CREATE OR REPLACE {SCHEMA_NAME}.{view_prefix}{TABLE_NAME}{variant_label}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{variant_label};
"""

    if manual_iteration:
        print(sqlstr)

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )

if manual_iteration:
    step4(base_sqlstr1, source_system, schema_name, table_name, column_names, src_column_dict)


# %% Create Step 5

@catch_error(logger)
def step5(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:list):
    step = 5
    SCHEMA_NAME = source_system.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()
    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    sqlstr = f"""{base_sqlstr}
CREATE OR REPLACE {SCHEMA_NAME}.{view_prefix}{TABLE_NAME}{variant_label}{stream_suffix}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{variant_label}{stream_suffix};
"""

    if manual_iteration:
        print(sqlstr)

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )

if manual_iteration:
    step5(base_sqlstr1, source_system, schema_name, table_name, column_names, src_column_dict)


# %% Create Step 4x

@catch_error(logger)
def step4x(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list, pk_column_names:list):
    step = 4

    column_list_with_business_key = '\n  ,'.join(column_names+elt_audit_columns)
    column_list_with_alias = '\n  ,'.join([f'{src_alias}.{c}' for c in column_names+elt_audit_columns])
    MD5_columns = "MD5(CONCAT(" + "\n  ,".join([f"COALESCE({c},'N/A')" for c in column_names]) + ")) AS MD5_HASH"

    sqlstr = base_sqlstr
    sqlstr += f"""
CREATE OR REPLACE VIEW {schema_name}.{view_prefix}{table_name}
AS
SELECT  
   INTEGRATION_ID
  ,{column_list_with_business_key}
  ,{hash_column_name}
FROM
(

SELECT TRIM(CONCAT({', '.join(pk_column_names)})) as INTEGRATION_ID
  ,{column_list_with_alias}
  ,{MD5_columns}
  ,ROW_NUMBER() OVER (PARTITION BY {src_alias}.INTEGRATION_ID ORDER BY {src_alias}.INTEGRATION_ID, {src_alias}.{execution_date_str} DESC) AS top_slice
FROM {snowflake_raw_database}.{schema_name}.{table_name}{stream_suffix} {stream_alias}
LEFT OUTER JOIN {snowflake_raw_database}.{schema_name}.{table_name} {src_alias}
    ON {src_alias}.INTEGRATION_ID = {stream_alias}.INTEGRATION_ID                            
WHERE
    TRIM({stream_alias}.INTEGRATION_ID) IS NOT NULL
)
WHERE top_slice = 1 ;
"""
    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )



# %% Iterate Over Steps for all tables

@catch_error(logger)
def iterate_over_all_tables(tableinfo, table_rows):
    n_tables = len(table_rows)

    for i, table in enumerate(table_rows):
        table_name = table['TableName']
        schema_name = table['SourceSchema']
        source_system = table['SourceDatabase']
        print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

        column_names, pk_column_names, src_column_dict = get_column_names(tableinfo, source_system, schema_name, table_name)

        PARTITION = get_partition(source_system, schema_name, table_name)
        if PARTITION:
            base_sqlstr1 = base_sqlstr(source_system)
            step1(base_sqlstr1, source_system, schema_name, table_name, column_names)
            step2(base_sqlstr1, source_system, schema_name, table_name, column_names)
            step3(base_sqlstr1, source_system, schema_name, table_name, column_names, PARTITION)
            step4(base_sqlstr1, source_system, schema_name, table_name, column_names, src_column_dict)
            step5(base_sqlstr1, source_system, schema_name, table_name, column_names, src_column_dict)

    print('Finished Iterating over all tables')

if not manual_iteration:
    iterate_over_all_tables(tableinfo, table_rows)


# %%


# %% Import Libraries
import os, sys
from functools import wraps

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging, catch_error
from modules.data_functions import elt_audit_columns, partitionBy
from modules.config import is_pc
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, read_adls_gen2, get_azure_sp


# %% Import Snowflake

import contextlib
from snowflake.connector import connect as snowflake_connect


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf, expr
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters
save_to_adls = False
execute_at_snowflake = True

manual_iteration = False
if not is_pc:
    manual_iteration = False

snowflake_account = 'advisorgroup-edip'

storage_account_name = "agaggrlakescd"
container_name = "ingress"
domain_name = 'financial_professional'
format = 'delta'

ddl_folder = f'metadata/{domain_name}/DDL'

variant_label = '_VARIANT'
variant_alias = 'SRC'

file_format = 'PARQUET'
wild_card = '.*.parquet'
stream_suffix = '_STREAM'

src_alias = 'src'
tgt_alias = 'tgt'
hash_column_name = 'MD5_HASH'
integration_id = 'INTEGRATION_ID'
stream_alias = 'src_strm'
view_prefix = 'VW_'
execution_date_str = 'EXECUTION_DATE'



# %% Create Session

spark = create_spark()

# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Connect to SnowFlake

@catch_error(logger)
def connect_to_snowflake(
        snowflake_account:str=snowflake_account,
        key_vault_account:str='snowflake',
        snowflake_database:str=None, 
        snowflake_warehouse:str=None,
        snowflake_role:str=None,
        ):

    _, snowflake_user, snowflake_pass = get_azure_sp(key_vault_account)

    snowflake_connection = snowflake_connect(
        user = snowflake_user,
        password = snowflake_pass,
        account = snowflake_account,
        database = snowflake_database,
        warehouse = snowflake_warehouse,
        role = snowflake_role,
        autocommit = True,
    )

    return snowflake_connection


snowflake_connection = connect_to_snowflake()



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
    data_types = filtered_tableinfo.select('TargetColumnName', 'TargetDataType').collect()
    data_types_dict = {c['TargetColumnName']:c['TargetDataType'] for c in data_types}

    pk_column_names = filtered_tableinfo.filter(
        (col('KeyIndicator') == lit(1))
        ).select('TargetColumnName').rdd.flatMap(lambda x: x).collect()

    return column_names, pk_column_names, src_column_dict, data_types_dict



# %% base sqlstr

@catch_error(logger)
def base_sqlstr(schema_name, table_name, source_system, layer:str):
    LAYER = f'_{layer}' if layer else ''
    SCHEMA_NAME = f'{source_system}{LAYER}'.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()

    sqlstr = f"""USE ROLE AD_SNOWFLAKE_QA_DBA;
USE WAREHOUSE QA_RAW_WH;
USE DATABASE QA_RAW_FP;
USE SCHEMA {source_system}{LAYER};
"""
    return SCHEMA_NAME, TABLE_NAME, sqlstr


# %% Get partition string for a Table

@catch_error(logger)
def get_partition(source_system:str, schema_name:str, table_name:str):
    data_type = 'data'
    container_folder = f"{data_type}/{domain_name}/{source_system}/{schema_name}"

    table_for_paritition = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = container_folder,
        table = table_name,
        format = format
    )

    PARTITION_LIST = table_for_paritition.select(F.max(col(partitionBy)).alias('part')).collect()
    PARTITION = PARTITION_LIST[0]['part']
    if PARTITION:
        return PARTITION.replace(':', '%3A') #.replace(' ', '%20')
    else:
        print(f'{container_folder}/{table_name} is EMPTY - SKIPPING')
        return None


# %% Action Step

def action_step(step:int):
    def outer(step_fn):
        @wraps(step_fn)
        def inner(*args, **kwargs):
            sqlstr = step_fn(*args, **kwargs)

            if manual_iteration:
                print(sqlstr)
            
            if save_to_adls:
                save_adls_gen2(
                    table_to_save = spark.createDataFrame([sqlstr], StringType()),
                    storage_account_name = storage_account_name,
                    container_name = container_name,
                    container_folder = f"{ddl_folder}/{kwargs['source_system']}/step_{step}/{kwargs['schema_name']}",
                    table = kwargs['table_name'],
                    format = 'text'
                )

            if execute_at_snowflake:
                print('Executing Snowflake SQL String')
                exec_status = snowflake_connection.execute_string(sql_text=sqlstr)

        return inner
    return outer



# %% Manual Iteration

if manual_iteration:
    i = 0
    n_tables = len(table_rows)

    table = table_rows[i]
    table_name = table['TableName']
    table_name = 'Appointment'
    schema_name = table['SourceSchema']
    source_system = table['SourceDatabase']
    print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

    column_names, pk_column_names, src_column_dict, data_types_dict = get_column_names(tableinfo, source_system, schema_name, table_name)
    PARTITION = get_partition(source_system, schema_name, table_name)




# %% Create Step 1

@catch_error(logger)
@action_step(1)
def step1(source_system:str, schema_name:str, table_name:str, column_names:list):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    sqlstr += f"""
CREATE OR REPLACE TABLE {SCHEMA_NAME}.{TABLE_NAME}{variant_label}
(
  {variant_alias} VARIANT
);
"""
    return sqlstr


if manual_iteration:
    step1(source_system, schema_name, table_name, column_names)


# %% Create Step 2

@catch_error(logger)
@action_step(2)
def step2(source_system:str, schema_name:str, table_name:str, column_names:list):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    sqlstr += f"""
CREATE OR REPLACE STREAM {SCHEMA_NAME}.{TABLE_NAME}{variant_label}{stream_suffix}
ON TABLE {SCHEMA_NAME}.{TABLE_NAME}{variant_label};
"""
    return sqlstr


if manual_iteration:
    step2(source_system, schema_name, table_name, column_names)


# %% Create Step 3

@catch_error(logger)
@action_step(3)
def step3(source_system:str, schema_name:str, table_name:str, column_names:list, PARTITION:str):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    sqlstr += f"""
COPY INTO {SCHEMA_NAME}.{TABLE_NAME}{variant_label}
FROM '@ELT_STAGE.AGGR_FP_DATALAKE/{source_system}/{schema_name}/{table_name}/{partitionBy}={PARTITION}/'
FILE_FORMAT = (type='{file_format}')
PATTERN = '{wild_card}'
ON_ERROR = CONTINUE;

SET SOURCE_SYSTEM = '{SCHEMA_NAME}';
SET TARGET_TABLE = '{SCHEMA_NAME}.{TABLE_NAME}{variant_label}';
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
    return sqlstr


if manual_iteration:
    step3(source_system, schema_name, table_name, column_names, PARTITION)



# %% Create Step 4

@catch_error(logger)
@action_step(4)
def step4(source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:dict):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict.items()] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    sqlstr += f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{view_prefix}{TABLE_NAME}{variant_label}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{variant_label};
"""
    return sqlstr


if manual_iteration:
    step4(source_system, schema_name, table_name, column_names, src_column_dict)


# %% Create Step 5

@catch_error(logger)
@action_step(5)
def step5(source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:dict):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict.items()] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    sqlstr += f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{view_prefix}{TABLE_NAME}{variant_label}{stream_suffix}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{variant_label}{stream_suffix};
"""
    return sqlstr


if manual_iteration:
    step5(source_system, schema_name, table_name, column_names, src_column_dict)


# %% Create Step 6

@catch_error(logger)
@action_step(6)
def step6(source_system:str, schema_name:str, table_name:str, column_names:list, pk_column_names:list):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    environment = 'QA'
    snowflake_raw_database = f'{environment}_RAW_FP'

    column_names_ex_pk = [c for c in column_names if c not in pk_column_names]
    column_list = '\n  ,'.join(column_names+elt_audit_columns)
    column_list_with_alias = '\n  ,'.join([f'{src_alias}.{c}' for c in column_names+elt_audit_columns])
    hash_columns = "MD5(CONCAT(\n       " + "\n      ,".join([f"COALESCE({c},'N/A')" for c in column_names_ex_pk]) + "\n      ))"
    INTEGRATION_ID = "TRIM(CONCAT(" + ', '.join([f"COALESCE({src_alias}.{c},'N/A')" for c in pk_column_names]) + "))"
    pk_column_with_alias = ', '.join([f"COALESCE({src_alias}.{c},'N/A')" for c in pk_column_names])

    sqlstr += f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{view_prefix}{TABLE_NAME}
AS
SELECT
   {integration_id}
  ,{column_list}
  ,{hash_column_name}
FROM
(
SELECT
   {INTEGRATION_ID} as {integration_id}
  ,{column_list_with_alias}
  ,{hash_columns} AS {hash_column_name}
  ,ROW_NUMBER() OVER (PARTITION BY {pk_column_with_alias} ORDER BY {pk_column_with_alias}, {src_alias}.{execution_date_str} DESC) AS top_slice
FROM {snowflake_raw_database}.{SCHEMA_NAME}.{view_prefix}{TABLE_NAME}{variant_label}{stream_suffix} {src_alias}
)
WHERE top_slice = 1;
"""
    return sqlstr


if manual_iteration:
    step6(source_system, schema_name, table_name, column_names, pk_column_names)


# %% Create Step 7

@catch_error(logger)
@action_step(7)
def step7(source_system:str, schema_name:str, table_name:str, column_names:list, data_types_dict:dict):
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    column_list_types = '\n  ,'.join(
        [f'{target_column_name} {target_data_type}' for target_column_name, target_data_type in data_types_dict.items()] +
        [f'{c} VARCHAR(50)' for c in elt_audit_columns]
        )

    sqlstr += f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME}
(
   {integration_id} VARCHAR(1000) NOT NULL
  ,{column_list_types}
  ,{hash_column_name} VARCHAR(100)
  ,CONSTRAINT PK_{SCHEMA_NAME}_{TABLE_NAME} PRIMARY KEY ({integration_id}) NOT ENFORCED
);
"""
    return sqlstr


if manual_iteration:
    step7(source_system, schema_name, table_name, column_names, data_types_dict)


# %% Create Step 8

@catch_error(logger)
@action_step(8)
def step8(source_system:str, schema_name:str, table_name:str, column_names:list):
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    column_list = '\n    ,'.join(column_names+elt_audit_columns)
    merge_update_columns = '\n    ,'.join([f'{tgt_alias}.{c} = {src_alias}.{c}' for c in column_names+elt_audit_columns])
    column_list_with_alias = '\n    ,'.join([f'{src_alias}.{c}' for c in column_names+elt_audit_columns])

    sqlstr += f"""
CREATE OR REPLACE PROCEDURE {SCHEMA_NAME}.USP_{TABLE_NAME}_MERGE()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var sql_command = 
`
MERGE INTO {SCHEMA_NAME}.{TABLE_NAME} {tgt_alias}
USING (
    SELECT * 
    FROM {SCHEMA_NAME}_RAW.{view_prefix}{TABLE_NAME}
) {src_alias}
ON (
TRIM(COALESCE({src_alias}.{integration_id},'N/A')) = TRIM(COALESCE({tgt_alias}.{integration_id},'N/A'))
)
WHEN MATCHED
AND TRIM(COALESCE({src_alias}.{hash_column_name},'N/A')) != TRIM(COALESCE({tgt_alias}.{hash_column_name},'N/A'))
THEN
  UPDATE
  SET
     {merge_update_columns}
    ,{tgt_alias}.{hash_column_name} = {src_alias}.{hash_column_name}

WHEN NOT MATCHED 
THEN
  INSERT
  (
     {integration_id}
    ,{column_list}
    ,{hash_column_name}
  )
  VALUES
  (
     {src_alias}.{integration_id}
    ,{column_list_with_alias}
    ,{src_alias}.{hash_column_name}
  );
`""" + """
try {
    snowflake.execute (
        {sqlText: sql_command}
        );
    return "Succeeded.";
    }
catch (err)  {
    return "Failed: " + err;
    }
$$
"""
    return sqlstr


if manual_iteration:
    step8(source_system, schema_name, table_name, column_names)


# %% Create Step 9

@catch_error(logger)
@action_step(9)
def step9(source_system:str, schema_name:str, table_name:str, column_names:list):
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)
    
    envionment = 'QA'
    warehouse = f'{envionment.upper()}_RAW_WH'
    stored_procedure = f'{SCHEMA_NAME}.USP_{TABLE_NAME}_MERGE'
    task_suffix = '_MERGE_TASK'
    task_name = f'{TABLE_NAME}{task_suffix}'.upper()
    stream_name = f'{SCHEMA_NAME}_RAW.{TABLE_NAME}{variant_label}{stream_suffix}'
    snowflake_role = f'AD_SNOWFLAKE_{envionment}_DBA'

    sqlstr = f"""
USE ROLE AD_SNOWFLAKE_QA_DBA;
USE WAREHOUSE {warehouse};
USE DATABASE QA_RAW_FP;
USE SCHEMA {SCHEMA_NAME};

CREATE OR REPLACE TASK {task_name}
WAREHOUSE = {warehouse}
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('{stream_name}')
AS
    CALL {stored_procedure}();

USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE {snowflake_role};

USE ROLE {snowflake_role};
ALTER TASK {task_name} RESUME;
"""
    return sqlstr


if manual_iteration:
    step9(source_system, schema_name, table_name, column_names)



# %% Iterate Over Steps for all tables

@catch_error(logger)
def iterate_over_all_tables(tableinfo, table_rows):
    n_tables = len(table_rows)

    for i, table in enumerate(table_rows):
        # if i>0 and is_pc: break
        table_name = table['TableName']
        schema_name = table['SourceSchema']
        source_system = table['SourceDatabase']
        print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

        column_names, pk_column_names, src_column_dict, data_types_dict = get_column_names(tableinfo, source_system, schema_name, table_name)

        PARTITION = get_partition(source_system, schema_name, table_name)
        if PARTITION:
            step1(source_system, schema_name, table_name, column_names)
            step2(source_system, schema_name, table_name, column_names)
            step3(source_system, schema_name, table_name, column_names, PARTITION)
            step4(source_system, schema_name, table_name, column_names, src_column_dict)
            step5(source_system, schema_name, table_name, column_names, src_column_dict)
            step6(source_system, schema_name, table_name, column_names, pk_column_names)
            step7(source_system, schema_name, table_name, column_names, data_types_dict)
            step8(source_system, schema_name, table_name, column_names)
            step9(source_system, schema_name, table_name, column_names)

    print('Finished Iterating over all tables')

if not manual_iteration:
    iterate_over_all_tables(tableinfo, table_rows)


# %% Close Showflake connection

snowflake_connection.close()


# %%



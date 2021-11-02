"""
Common Library for creating and executing (if required) Snowflake DDL Steps and ingest_data

"""

# %% Import Libraries
import json, os, sys
from functools import wraps
from collections import defaultdict, OrderedDict

from .common_functions import logger, catch_error, is_pc, data_settings, execution_date, get_secrets, to_OrderedDict, EXECUTION_DATE_str
from .spark_functions import elt_audit_columns
from .azure_functions import azure_container_folder_path, setup_spark_adls_gen2_connection, save_adls_gen2, get_partition, container_name, \
    to_storage_account_name, default_storage_account_abbr, default_storage_account_name, post_log_data, metadata_folder

from snowflake.connector import connect as snowflake_connect
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit



# %% Parameters

class module_params_class:
    """
    Data Class to pass parameters across modules and the main code
    """
    save_to_adls = False # Default False
    execute_at_snowflake = False # Default False
    create_or_replace = True # Default False - Use True for Schema Change Update
    create_cicd_file = data_settings.create_cicd_file

    snowflake_account = data_settings.snowflake_account
    sf_key_vault_account = data_settings.snowflake_key_vault_account

    domain_name = sys.domain_name
    domain_abbr = sys.domain_abbr
    environment = data_settings.environment
    snowflake_raw_warehouse = data_settings.snowflake_warehouse
    snowflake_raw_database = f'{environment}_{domain_abbr}'.upper()
    snowflake_curated_database = f'{environment}_{domain_abbr}'.upper()

    common_elt_stage_name = default_storage_account_abbr
    common_storage_account = default_storage_account_name

    snowflake_role = data_settings.snowflake_role

    ddl_folder = 'DDL'

    variant_label = '_VARIANT'
    variant_alias = 'SRC'

    FILE_FORMAT = 'PARQUET'
    wild_card = '.*.parquet'
    stream_suffix = '_STREAM'

    elt_stage_schema = "ELT_STAGE"

    src_alias = 'src'
    tgt_alias = 'tgt'
    hash_column_name = 'MD5_HASH'
    integration_id = 'INTEGRATION_ID'
    stream_alias = 'src_strm'
    view_prefix = 'VW_'

    spark = None
    snowflake_connection = None
    cicd_file = None
    cicd_str_per_step = defaultdict(str)

    cicd_folder_path = data_settings.output_cicd_path



wid = module_params_class()
snowflake_ddl_params = wid

if wid.create_cicd_file:
    os.makedirs(name=wid.cicd_folder_path, exist_ok=True)



# %% Connect to SnowFlake

@catch_error(logger)
def connect_to_snowflake(
        snowflake_account:str=wid.snowflake_account,
        key_vault_account:str=wid.sf_key_vault_account,
        snowflake_database:str=None, 
        snowflake_warehouse:str=wid.snowflake_raw_warehouse,
        snowflake_role:str=wid.snowflake_role,
        ):
    """
    Connect to SnowFlake account
    """
    _, snowflake_user, snowflake_pass = get_secrets(key_vault_account)

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



# %% Get Column Names

@catch_error(logger)
def get_column_names(tableinfo, source_system, schema_name, table_name):
    """
    Get column names, types and other metadata from tableinfo
    """
    filtered_tableinfo = tableinfo.where(
        (col('TableName')==lit(table_name)) &
        (col('SourceSchema')==lit(schema_name)) &
        (col('SourceDatabase')==lit(source_system))
        )

    filtered_column_names = filtered_tableinfo.select('TargetColumnName', 'SourceColumnName', 'TargetDataType', 'KeyIndicator').distinct().collect()

    column_names = sorted([c['TargetColumnName'] for c in filtered_column_names])

    src_column_dict = to_OrderedDict({c['TargetColumnName']:c['SourceColumnName'] for c in filtered_column_names})

    data_types_dict = to_OrderedDict({c['TargetColumnName']:c['TargetDataType'] for c in filtered_column_names})

    pk_column_names = sorted([c['TargetColumnName'] for c in filtered_column_names if str(c['KeyIndicator'])=='1'])

    return column_names, pk_column_names, src_column_dict, data_types_dict



# %% base sqlstr

@catch_error(logger)
def base_sqlstr(schema_name, table_name, source_system, layer:str):
    """
    Base SQL string to have at the beginning of all SQL Strings
    """
    LAYER = f'_{layer}' if layer else ''
    SCHEMA_NAME = f'{source_system}{LAYER}'.upper()
    TABLE_NAME = f'{schema_name}_{table_name}'.upper()

    sqlstr = f"""USE ROLE {wid.snowflake_role};
USE WAREHOUSE {wid.snowflake_raw_warehouse};
USE DATABASE {wid.snowflake_raw_database};
USE SCHEMA {SCHEMA_NAME};
"""
    return SCHEMA_NAME, TABLE_NAME, sqlstr



# %% Action Step

def action_step(step:int):
    """
    Wrapper function around Step functions -> to save and/or execute output SQL from each Step
    """
    def outer(step_fn):
        @wraps(step_fn)
        def inner(*args, **kwargs):
            logger.info(f"{kwargs['source_system']}/step_{step}/{kwargs['schema_name']}/{kwargs['table_name']}")
            sqlstr = step_fn(*args, **kwargs)

            if wid.save_to_adls:
                storage_account_name = default_storage_account_name
                setup_spark_adls_gen2_connection(wid.spark, storage_account_name)

                save_adls_gen2(
                    table = wid.spark.createDataFrame([sqlstr], StringType()),
                    storage_account_name = storage_account_name,
                    container_name = container_name,
                    container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=wid.domain_name, source_or_database=f"{wid.ddl_folder}/{kwargs['source_system']}", firm_or_schema=f"step_{step}/{kwargs['schema_name']}"),
                    table_name = kwargs['table_name'],
                    file_format = 'text'
                )

            if wid.execute_at_snowflake:
                logger.info(f"Executing Snowflake SQL String: {kwargs['source_system']}/step_{step}/{kwargs['schema_name']}/{kwargs['table_name']}")
                exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)

        return inner
    return outer



# %% Action Source Level Tables

def action_source_level_tables(table_name:str):
    """
    Wrapper function for SQL strings from Source Level tables -> to save and/or execute output SQL from each function
    """
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            logger.info(f"Source Level Table {kwargs['container_folder']}/{table_name}")
            sqlstr = fn(*args, **kwargs)
            if wid.save_to_adls:
                save_adls_gen2(
                    table = wid.spark.createDataFrame([sqlstr], StringType()),
                    storage_account_name = kwargs['storage_account_name'],
                    container_name = container_name,
                    container_folder = kwargs['container_folder'],
                    table_name = table_name,
                    file_format = 'text'
                )

            if wid.execute_at_snowflake:
                logger.info(f"Executing Snowflake SQL String: {kwargs['container_folder']}/{table_name}")
                exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)

            if wid.create_cicd_file:
                wid.cicd_file = sqlstr
                write_CICD_file_per_table(source_system=kwargs['source_system'], schema_name=None, table_name=table_name)

        return inner
    return outer



# %% Write CICD File per Table to local file system

@catch_error(logger)
def write_CICD_file_per_table(source_system:str, schema_name:str, table_name:str):
    """
    Write CICD File per Table to local file system
    """
    if not wid.create_cicd_file:
        return

    file_folder_path = os.path.join(wid.cicd_folder_path + '/per_table', source_system)
    os.makedirs(name=file_folder_path, exist_ok=True)

    if schema_name:
        TABLE_NAME = f'{schema_name}_{table_name}'
    else:
        TABLE_NAME = table_name

    file_path = os.path.join(file_folder_path, f'{TABLE_NAME}.sql')

    logger.info(f'Writing: {file_path}')
    with open(file_path, 'w') as f:
        f.write(wid.cicd_file)



# %% Write CICD File per Step to local file system

@catch_error(logger)
def write_CICD_file_per_step():
    """
    Write CICD File per Step to local file system
    """
    if not wid.create_cicd_file:
        return

    for cicd_source_system, cicd_str in wid.cicd_str_per_step.items():
        folder_name, file_name = cicd_source_system

        file_folder_path = os.path.join(wid.cicd_folder_path + '/per_step', folder_name)
        os.makedirs(name=file_folder_path, exist_ok=True)

        file_path = os.path.join(file_folder_path, f'{file_name}.sql')

        logger.info(f'Writing: {file_path}')
        with open(file_path, 'w') as f:
            f.write(cicd_str)



# %% COPY INTO statement

@catch_error(logger)
def create_copy_into_sql(source_system:str, schema_name:str, table_name:str, PARTITION:str, storage_account_abbr:str):
    """
    Create COPY INTO statement for a given table
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)
    
    INGEST_STAGE_NAME = f'@{wid.elt_stage_schema}.{storage_account_abbr}_{wid.domain_abbr}_DATALAKE/{source_system}/{schema_name}/{table_name}/{PARTITION}/'

    copy_into_sqlstr = f"""COPY INTO {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label} FROM '{INGEST_STAGE_NAME}' FILE_FORMAT = (type='{wid.FILE_FORMAT}') PATTERN = '{wid.wild_card}' ON_ERROR = CONTINUE;"""

    return SCHEMA_NAME, TABLE_NAME, INGEST_STAGE_NAME, copy_into_sqlstr, sqlstr



# %% Create Ingest Data

@catch_error(logger)
def create_ingest_data(source_system:str, schema_name:str, table_name:str, PARTITION:str, storage_account_abbr:str):
    """
    Create Ingest Data
    """
    SCHEMA_NAME, TABLE_NAME, INGEST_STAGE_NAME, copy_into_sqlstr, sqlstr = create_copy_into_sql(source_system=source_system, schema_name=schema_name, table_name=table_name, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)

    ingest_data = {
        "INGEST_STAGE_NAME": INGEST_STAGE_NAME, 
        "EXECUTION_DATE": execution_date,
        "FULL_OBJECT_NAME": TABLE_NAME,
        "COPY_COMMAND": copy_into_sqlstr,
        "INGEST_SCHEMA": SCHEMA_NAME,
        "SOURCE_SYSTEM": source_system,
        "ELT_STAGE_SCHEMA": wid.elt_stage_schema
    }

    log_data = {
        **ingest_data,
        "STORAGE_ACCOUNT_ABBR": storage_account_abbr,
        "STORAGE_ACCOUNT": to_storage_account_name(firm_name=storage_account_abbr),
        "PARTITION": PARTITION,
        }

    post_log_data(log_data=log_data, log_type='AirflowIngestData', logger=logger)

    return ingest_data



# %% Create ingest_data table

@catch_error(logger)
def create_ingest_data_table(ingest_data_per_source_system, container_folder:str, storage_account_name:str):
    """
    Create and Save ingest_data table
    """
    json_string = json.dumps(ingest_data_per_source_system)

    save_adls_gen2(
        table = wid.spark.read.json(wid.spark.sparkContext.parallelize([json_string])).coalesce(1),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = container_folder,
        table_name = 'ingest_data',
        file_format = 'parquet'
    )



# %% Create Task that Triggers USP_INGEST() file

@catch_error(logger)
@action_source_level_tables('usp_ingest')
def create_trigger_usp_ingest_file(source_system:str, container_folder:str, storage_account_name:str):
    """
    Create Task that Triggers USP_INGEST() file and save it as per @action_source_level_tables wrapper
    """
    stream_name = f'INGEST_REQUEST_VARIANT_STREAM'
    task_name = f'{source_system}_INGEST_REQUEST_TASK'

    sqlstr = f"""USE ROLE {wid.snowflake_role};
USE WAREHOUSE {wid.snowflake_raw_warehouse};
USE DATABASE {wid.snowflake_raw_database};
USE SCHEMA {wid.elt_stage_schema};

CREATE TASK IF NOT EXISTS {wid.elt_stage_schema}.{task_name}
WAREHOUSE = {wid.snowflake_raw_warehouse}
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('{wid.elt_stage_schema}.{stream_name}')
AS
  CALL {wid.elt_stage_schema}.USP_INGEST(); 

USE ROLE {wid.snowflake_role};
ALTER TASK {wid.elt_stage_schema}.{task_name} RESUME;
"""

    return sqlstr



# %% Trigger snowpipe

@catch_error(logger)
def trigger_snowpipe(source_system:str):
    """
    Trigger snowpipe to read INGEST_REQUEST files
    """
    sqlstr = f"""
USE ROLE {wid.snowflake_role};
USE WAREHOUSE {wid.snowflake_raw_warehouse};
USE DATABASE {wid.snowflake_raw_database};

ALTER PIPE {wid.elt_stage_schema}.{wid.common_elt_stage_name}_{wid.domain_abbr}_{source_system}_INGEST_REQUEST_PIPE REFRESH;
"""

    logger.info(f'Triggering Snowpipe{sqlstr}')
    exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)

    log_data = {
        'sqlstr': sqlstr,
        'source_system': source_system,
    }

    post_log_data(log_data=log_data, log_type='AirflowSnowflakeRequests', logger=logger)



# %% Create Source Level Tables

@catch_error(logger)
def create_source_level_tables(ingest_data_list:defaultdict):
    """
    Create Source Level Tables
    """
    if not ingest_data_list:
        logger.warning('No data in ingest_data_list -> skipping')
        return

    logger.info(f'Create Source Level Tables')
    for source_system, ingest_data_per_source_system in ingest_data_list.items():
        storage_account_name = default_storage_account_name
        setup_spark_adls_gen2_connection(wid.spark, storage_account_name)
        container_folder = azure_container_folder_path(data_type=metadata_folder, domain_name=wid.domain_name, source_or_database=source_system)

        create_ingest_data_table(
            ingest_data_per_source_system = ingest_data_per_source_system,
            container_folder = container_folder,
            storage_account_name = storage_account_name,
            )

        create_trigger_usp_ingest_file(
            source_system = source_system,
            container_folder = container_folder,
            storage_account_name = storage_account_name,
            )

        trigger_snowpipe(
            source_system = source_system,
            )




# %% Create or Replace Utility Function

@catch_error(logger)
def create_or_replace_func(object_name:str):
    """
    Utility function to choose wheter to use "CREATE OR REPLACE" or "CREATE IF NOT EXISTS" in sql statements
    """
    if wid.create_or_replace:
        sqlstr = f'CREATE OR REPLACE {object_name}'
    else:
        sqlstr = f'CREATE {object_name} IF NOT EXISTS'
    return sqlstr



# %% Create Step 1

@catch_error(logger)
@action_step(1)
def step1(source_system:str, schema_name:str, table_name:str):
    """
    Snowflake DDL: Create Variant Table - Raw data from External Stage table will be copied here.
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)
    
    cicd_source_system = (SCHEMA_NAME, 'V0.0.1__Create_Tables')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    wid.cicd_file = sqlstr

    step = f"""
{create_or_replace_func('TABLE')} {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label}
(
  {wid.variant_alias} VARIANT
);
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 2

@catch_error(logger)
@action_step(2)
def step2(source_system:str, schema_name:str, table_name:str):
    """
    Snowflake DDL: Create Stream on the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)
    
    cicd_source_system = (SCHEMA_NAME, 'V0.0.2__Create_Streams')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    step = f"""
{create_or_replace_func('STREAM')} {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label}{wid.stream_suffix}
ON TABLE {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label};
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 3

@catch_error(logger)
@action_step(3)
def step3(source_system:str, schema_name:str, table_name:str, PARTITION:str, storage_account_abbr:str):
    """
    Snowflake DDL: Add results of last job run on the Variant Table to the ELT_COPY_EXCEPTION table
    Validates the files loaded in a past execution of the COPY INTO <table> command and returns all the errors encountered during the load
    """
    SCHEMA_NAME, TABLE_NAME, INGEST_STAGE_NAME, copy_into_sqlstr, sqlstr = create_copy_into_sql(source_system=source_system, schema_name=schema_name, table_name=table_name, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)

    step = f"""
{copy_into_sqlstr}

SET SOURCE_SYSTEM = '{SCHEMA_NAME}';
SET TARGET_TABLE = '{SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label}';
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

INSERT INTO {wid.elt_stage_schema}.ELT_COPY_EXCEPTION
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
FROM TABLE(validate({SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label}, job_id => '_last'));
"""

    sqlstr += step
    wid.cicd_file += step
    return sqlstr




# %% Create Step 4

@catch_error(logger)
@action_step(4)
def step4(source_system:str, schema_name:str, table_name:str, src_column_dict:OrderedDict):
    """
    Snowflake DDL: Create View on the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.3__Create_Views')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict.items()] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    step = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{wid.view_prefix}{TABLE_NAME}{wid.variant_label}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label};
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 5

@catch_error(logger)
@action_step(5)
def step5(source_system:str, schema_name:str, table_name:str, src_column_dict:OrderedDict):
    """
    Snowflake DDL: Create View on the Stream of the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.3__Create_Views')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    column_list_src = '\n  ,'.join(
        [f'SRC:"{source_column_name}"::string AS {target_column_name}' for target_column_name, source_column_name in src_column_dict.items()] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    step = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{wid.view_prefix}{TABLE_NAME}{wid.variant_label}{wid.stream_suffix}
AS
SELECT
   {column_list_src}
FROM {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label}{wid.stream_suffix};
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 6

@catch_error(logger)
@action_step(6)
def step6(source_system:str, schema_name:str, table_name:str, column_names:list, pk_column_names:list):
    """
    Snowflake DDL: Create View with Integration ID and Hash Column on the Stream of the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.3__Create_Views')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    column_names_ex_pk = [c for c in column_names if c not in pk_column_names]
    column_list = '\n  ,'.join(column_names+elt_audit_columns)
    column_list_with_alias = '\n  ,'.join([f'{wid.src_alias}.{c}' for c in column_names+elt_audit_columns])
    hash_columns = "MD5(CONCAT(\n       " + "\n      ,".join([f"COALESCE({c},'N/A')" for c in column_names_ex_pk]) + "\n      ))"
    INTEGRATION_ID = "TRIM(CONCAT(" + ', '.join([f"COALESCE({wid.src_alias}.{c},'N/A')" for c in pk_column_names]) + "))"
    pk_column_with_alias = ', '.join([f"COALESCE({wid.src_alias}.{c},'N/A')" for c in pk_column_names])

    step = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{wid.view_prefix}{TABLE_NAME}
AS
SELECT
   {wid.integration_id}
  ,{column_list}
  ,{wid.hash_column_name}
FROM
(
SELECT
   {INTEGRATION_ID} as {wid.integration_id}
  ,{column_list_with_alias}
  ,{hash_columns} AS {wid.hash_column_name}
  ,ROW_NUMBER() OVER (PARTITION BY {pk_column_with_alias} ORDER BY {pk_column_with_alias}, {wid.src_alias}.{EXECUTION_DATE_str} DESC) AS top_slice
FROM {SCHEMA_NAME}.{wid.view_prefix}{TABLE_NAME}{wid.variant_label}{wid.stream_suffix} {wid.src_alias}
)
WHERE top_slice = 1;
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 7

@catch_error(logger)
@action_step(7)
def step7(source_system:str, schema_name:str, table_name:str, data_types_dict:OrderedDict):
    """
    Snowflake DDL: Create final destination raw Table
    """
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.1__Create_Tables')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    column_list_types = '\n  ,'.join(
        [f'{target_column_name} {target_data_type}' for target_column_name, target_data_type in data_types_dict.items()] +
        [f'{c} VARCHAR(50)' for c in elt_audit_columns]
        )

    step = f"""
{create_or_replace_func('TABLE')} {SCHEMA_NAME}.{TABLE_NAME}
(
   {wid.integration_id} VARCHAR(1000) NOT NULL
  ,{column_list_types}
  ,{wid.hash_column_name} VARCHAR(100)
  ,CONSTRAINT PK_{SCHEMA_NAME}_{TABLE_NAME} PRIMARY KEY ({wid.integration_id}) NOT ENFORCED
);
"""

    sqlstr += step
    wid.cicd_file += f"\n\nUSE SCHEMA {SCHEMA_NAME};\n\n{step}"
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr




# %% Create Step 8

@catch_error(logger)
@action_step(8)
def step8(source_system:str, schema_name:str, table_name:str, data_types_dict:OrderedDict):
    """
    Snowflake DDL: Create Procedure to UPSERT raw data from variant data stream/view to destination raw table
    """
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.2__Create_Stored_Procedures')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    stored_procedure = f'{SCHEMA_NAME}.USP_{TABLE_NAME}_MERGE'

    column_list = '\n    ,'.join([target_column_name for target_column_name, target_data_type in data_types_dict.items()] + elt_audit_columns)

    def fval(column_name:str, data_type:str):
        if data_type.upper().startswith('variant'.upper()):
            return f'PARSE_JSON({column_name})'
        elif data_type.upper().startswith('string'.upper()) or data_type.upper().startswith('varchar'.upper()):
            return f"COALESCE({column_name}, '')"
        else:
            return column_name

    merge_update_columns     = '\n    ,'.join([f'{wid.tgt_alias}.{c} = ' + fval(f'{wid.src_alias}.{c}', target_data_type) for c, target_data_type in data_types_dict.items()])
    merge_update_elt_columns = '\n    ,'.join([f'{wid.tgt_alias}.{c} = {wid.src_alias}.{c}' for c in elt_audit_columns])
    merge_update_columns    += '\n    ,' + merge_update_elt_columns

    column_list_with_alias     = '\n    ,'.join([fval(f'{wid.src_alias}.{c}', target_data_type) for c, target_data_type in data_types_dict.items()])
    column_list_with_alias_elt = '\n    ,'.join([f'{wid.src_alias}.{c}' for c in elt_audit_columns])
    column_list_with_alias    += '\n    ,' + column_list_with_alias_elt

    step = f"""
CREATE OR REPLACE PROCEDURE {stored_procedure}()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var sql_command = 
`
MERGE INTO {SCHEMA_NAME}.{TABLE_NAME} {wid.tgt_alias}
USING (
    SELECT * 
    FROM {SCHEMA_NAME}_RAW.{wid.view_prefix}{TABLE_NAME}
) {wid.src_alias}
ON (
TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) = TRIM(COALESCE({wid.tgt_alias}.{wid.integration_id},'N/A'))
)
WHEN MATCHED
AND TRIM(COALESCE({wid.src_alias}.{wid.hash_column_name},'N/A')) != TRIM(COALESCE({wid.tgt_alias}.{wid.hash_column_name},'N/A'))
AND TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) != 'N/A'
THEN
  UPDATE
  SET
     {merge_update_columns}
    ,{wid.tgt_alias}.{wid.hash_column_name} = {wid.src_alias}.{wid.hash_column_name}

WHEN NOT MATCHED
AND TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) != 'N/A'
THEN
  INSERT
  (
     {wid.integration_id}
    ,{column_list}
    ,{wid.hash_column_name}
  )
  VALUES
  (
     {wid.src_alias}.{wid.integration_id}
    ,{column_list_with_alias}
    ,{wid.src_alias}.{wid.hash_column_name}
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
$$;
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Create Step 9

@catch_error(logger)
@action_step(9)
def step9(source_system:str, schema_name:str, table_name:str):
    """
    Snowflake DDL: Create task to run the stored procedure for UPSERT every x minute
    """
    layer = ''
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)

    cicd_source_system = (SCHEMA_NAME, 'V0.0.3__Create_Tasks')
    if not wid.cicd_str_per_step[cicd_source_system]:
        wid.cicd_str_per_step[cicd_source_system] = f'USE SCHEMA {SCHEMA_NAME};' + '\n'*4

    stored_procedure = f'{SCHEMA_NAME}.USP_{TABLE_NAME}_MERGE'
    task_suffix = '_MERGE_TASK'
    task_name = f'{TABLE_NAME}{task_suffix}'.upper()
    stream_name = f'{SCHEMA_NAME}_RAW.{TABLE_NAME}{wid.variant_label}{wid.stream_suffix}'

    step = f"""
{create_or_replace_func('TASK')} {task_name}
WAREHOUSE = {wid.snowflake_raw_warehouse}
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('{stream_name}')
AS
    CALL {stored_procedure}();

USE ROLE {wid.snowflake_role};
ALTER TASK {task_name} RESUME;
"""

    sqlstr += step
    wid.cicd_file += step
    wid.cicd_str_per_step[cicd_source_system] += step + '\n'*4
    return sqlstr



# %% Iterate Over Steps for all tables

@catch_error(logger)
def iterate_over_all_tables_snowflake(tableinfo, table_rows, PARTITION_list=None):
    """
    Iterate Over All DDL creation Steps for all tables
    """
    if not tableinfo:
        logger.warning('No data in TableInfo -> Skipping Snowflake Steps')
        return

    n_tables = len(table_rows)
    ingest_data_list = defaultdict(list)

    for i, table in enumerate(table_rows):
        table_name = table['TableName']
        schema_name = table['SourceSchema']
        source_system = table['SourceDatabase']
        storage_account_name = table['StorageAccount']
        storage_account_abbr = table['StorageAccountAbbr']
        logger.info(f'Processing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

        PARTITION = get_partition(spark=wid.spark, domain_name=wid.domain_name, source_system=source_system, schema_name=schema_name, table_name=table_name, storage_account_name=storage_account_name, PARTITION_list=PARTITION_list)
        if not PARTITION: continue

        if wid.create_cicd_file:
            column_names, pk_column_names, src_column_dict, data_types_dict = get_column_names(tableinfo=tableinfo, source_system=source_system, schema_name=schema_name, table_name=table_name)
            step1(source_system=source_system, schema_name=schema_name, table_name=table_name)
            step2(source_system=source_system, schema_name=schema_name, table_name=table_name)
            step3(source_system=source_system, schema_name=schema_name, table_name=table_name, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)
            step4(source_system=source_system, schema_name=schema_name, table_name=table_name, src_column_dict=src_column_dict)
            step5(source_system=source_system, schema_name=schema_name, table_name=table_name, src_column_dict=src_column_dict)
            step6(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, pk_column_names=pk_column_names)
            step7(source_system=source_system, schema_name=schema_name, table_name=table_name, data_types_dict=data_types_dict)
            step8(source_system=source_system, schema_name=schema_name, table_name=table_name, data_types_dict=data_types_dict)
            step9(source_system=source_system, schema_name=schema_name, table_name=table_name)
            write_CICD_file_per_table(source_system=source_system, schema_name=schema_name, table_name=table_name)

        ingest_data = create_ingest_data(source_system=source_system, schema_name=schema_name, table_name=table_name, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)
        ingest_data_list[source_system].append(ingest_data)

    write_CICD_file_per_step()
    logger.info('Finished Iterating over all tables')
    return ingest_data_list



# %%



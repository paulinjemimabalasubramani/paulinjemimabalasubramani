"""
Common Library for creating and executing (if required) Snowflake DDL Steps and ingest_data

"""

# %% Import Libraries
import json, os
from functools import wraps
from collections import defaultdict, OrderedDict

from .common_functions import make_logging, catch_error
from .data_functions import elt_audit_columns, execution_date
from .config import is_pc, data_path
from .azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, get_partition, get_azure_sp, \
    container_name, to_storage_account_name, default_storage_account_abbr, default_storage_account_name

from snowflake.connector import connect as snowflake_connect
from datetime import datetime
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit
import sys
import requests
import hashlib
import hmac
import base64

# %% Logging
logger = make_logging(__name__)


# %% Parameters
class module_params_class:
    save_to_adls = False # Default False
    execute_at_snowflake = False # Default False
    create_or_replace = False # Default False - Use True for Schema Change Update
    create_cicd_file = True # Default True

    snowflake_account = 'advisorgroup-edip'
    sf_key_vault_account = 'snowflake'

    domain_name = 'financial_professional'
    domain_abbr = 'FP'
    envionment = 'QA'
    snowflake_raw_warehouse = f'{envionment}_RAW_WH'.upper()
    snowflake_raw_database = f'{envionment}_RAW_{domain_abbr}'.upper()
    snowflake_curated_database = f'{envionment}_CURATED_{domain_abbr}'.upper()

    common_elt_stage_name = default_storage_account_abbr
    common_storage_account = default_storage_account_name

    snowflake_role = f'AD_SNOWFLAKE_{envionment}_DBA'.upper()
    engineer_role = f"AD_SNOWFLAKE_{envionment}_ENGINEER".upper()

    ddl_folder = f'metadata/{domain_name}/DDL'

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
    execution_date_str = 'EXECUTION_DATE'

    spark = None
    snowflake_connection = None
    cicd_file = None
    cicd_str_per_step = defaultdict(str)

    cicd_folder_path = os.path.join(data_path, 'CICD')



wid = module_params_class()
snowflake_ddl_params = wid

if not is_pc:
    wid.save_to_adls = False # Default False
    wid.execute_at_snowflake = False # Default False
    wid.create_or_replace = True # Default False - Use True for Schema Change Update
    wid.create_cicd_file = True # Default True

if wid.create_cicd_file:
    os.makedirs(name=wid.cicd_folder_path, exist_ok=True)



# %% Connect to SnowFlake

@catch_error(logger)
def connect_to_snowflake(
        snowflake_account:str=wid.snowflake_account,
        key_vault_account:str=wid.sf_key_vault_account,
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



# %% Get Column Names

@catch_error(logger)
def get_column_names(tableinfo, source_system, schema_name, table_name):
    filtered_tableinfo = tableinfo.filter(
        (col('TableName') == table_name) &
        (col('SourceSchema') == schema_name) &
        (col('SourceDatabase') == source_system)
        )

    column_names = sorted(filtered_tableinfo.select('TargetColumnName').rdd.flatMap(lambda x: x).collect())

    pk_column_names = sorted(filtered_tableinfo.filter(
        (col('KeyIndicator') == lit(1))
        ).select('TargetColumnName').rdd.flatMap(lambda x: x).collect())

    src_column_names = filtered_tableinfo.select('TargetColumnName', 'SourceColumnName').collect()
    src_column_dict = {c['TargetColumnName']:c['SourceColumnName'] for c in src_column_names}
    src_column_dict = OrderedDict(sorted(src_column_dict.items(), key=lambda x:x[0], reverse=False))

    data_types = filtered_tableinfo.select('TargetColumnName', 'TargetDataType').collect()
    data_types_dict = {c['TargetColumnName']:c['TargetDataType'] for c in data_types}
    data_types_dict = OrderedDict(sorted(data_types_dict.items(), key=lambda x:x[0], reverse=False))

    return column_names, pk_column_names, src_column_dict, data_types_dict



# %% base sqlstr

@catch_error(logger)
def base_sqlstr(schema_name, table_name, source_system, layer:str):
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
    def outer(step_fn):
        @wraps(step_fn)
        def inner(*args, **kwargs):
            print(f"\n{kwargs['source_system']}/step_{step}/{kwargs['schema_name']}/{kwargs['table_name']}")
            sqlstr = step_fn(*args, **kwargs)

            if is_pc and False:
                print(sqlstr)
            
            if wid.save_to_adls:
                storage_account_name = to_storage_account_name(firm_name=kwargs['schema_name'], source_system=kwargs['source_system'])
                setup_spark_adls_gen2_connection(wid.spark, storage_account_name)

                save_adls_gen2(
                    table_to_save = wid.spark.createDataFrame([sqlstr], StringType()),
                    storage_account_name = storage_account_name,
                    container_name = container_name,
                    container_folder = f"{wid.ddl_folder}/{kwargs['source_system']}/step_{step}/{kwargs['schema_name']}",
                    table_name = kwargs['table_name'],
                    file_format = 'text'
                )

            if wid.execute_at_snowflake:
                print(f"Executing Snowflake SQL String: {kwargs['source_system']}/step_{step}/{kwargs['schema_name']}/{kwargs['table_name']}")
                exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)

        return inner
    return outer




# %% Action Source Level Tables

def action_source_level_tables(table_name:str):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            print(f"\nSource Level Table {kwargs['container_folder']}/{table_name}")
            sqlstr = fn(*args, **kwargs)
            if wid.save_to_adls or True:
                save_adls_gen2(
                    table_to_save = wid.spark.createDataFrame([sqlstr], StringType()),
                    storage_account_name = kwargs['storage_account_name'],
                    container_name = container_name,
                    container_folder = kwargs['container_folder'],
                    table_name = table_name,
                    file_format = 'text'
                )

            if wid.execute_at_snowflake:
                print(f"Executing Snowflake SQL String: {kwargs['container_folder']}/{table_name}")
                exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)
            
            if wid.create_cicd_file:
                wid.cicd_file = sqlstr
                write_CICD_file_per_table(source_system=kwargs['source_system'], schema_name=None, table_name=table_name)

        return inner
    return outer




# %% Write CICD File per Table to local file system

@catch_error(logger)
def write_CICD_file_per_table(source_system:str, schema_name:str, table_name:str):
    if not wid.create_cicd_file:
        return

    file_folder_path = os.path.join(wid.cicd_folder_path + '/per_table', source_system)
    os.makedirs(name=file_folder_path, exist_ok=True)

    if schema_name:
        TABLE_NAME = f'{schema_name}_{table_name}'
    else:
        TABLE_NAME = table_name

    file_path = os.path.join(file_folder_path, f'{TABLE_NAME}.sql')

    print(f'\nWriting: {file_path}\n')
    with open(file_path, 'w') as f:
        f.write(wid.cicd_file)




# %% Write CICD File per Step to local file system

@catch_error(logger)
def write_CICD_file_per_step():
    if not wid.create_cicd_file:
        return

    for cicd_source_system, cicd_str in wid.cicd_str_per_step.items():
        folder_name, file_name = cicd_source_system

        file_folder_path = os.path.join(wid.cicd_folder_path + '/per_step', folder_name)
        os.makedirs(name=file_folder_path, exist_ok=True)

        file_path = os.path.join(file_folder_path, f'{file_name}.sql')

        print(f'\nWriting: {file_path}\n')
        with open(file_path, 'w') as f:
            f.write(cicd_str)



# %% COPY INTO statement

@catch_error(logger)
def create_copy_into_sql(source_system:str, schema_name:str, table_name:str, PARTITION:str, storage_account_abbr:str):
    layer = 'RAW'
    SCHEMA_NAME, TABLE_NAME, sqlstr = base_sqlstr(schema_name=schema_name, table_name=table_name, source_system=source_system, layer=layer)
    
    INGEST_STAGE_NAME = f'@{wid.elt_stage_schema}.{storage_account_abbr}_{wid.domain_abbr}_DATALAKE/{source_system}/{schema_name}/{table_name}/{PARTITION}/'

    copy_into_sqlstr = f"""COPY INTO {SCHEMA_NAME}.{TABLE_NAME}{wid.variant_label} FROM '{INGEST_STAGE_NAME}' FILE_FORMAT = (type='{wid.FILE_FORMAT}') PATTERN = '{wid.wild_card}' ON_ERROR = CONTINUE;"""

    return SCHEMA_NAME, TABLE_NAME, INGEST_STAGE_NAME, copy_into_sqlstr, sqlstr


# %% Log Ingest Commands To Snowflake Into Log Analytics

@catch_error(logger)
def log_ingest_data(ingest_data, source_system:str, schema_name:str, table_name:str, storage_account_abbr:str, partition:str):
    timestamp = datetime.now()
    log_data = {"TimeGenerated": str(timestamp), "Storage_Account": storage_account_abbr, "Source_System": source_system, "Schema_Name": schema_name, "Table": table_name, "Ingest_Data": ingest_data, "Partition": partition}
    tenant_id,customer_id,shared_key = get_azure_sp("loganalytics")
    log_type = "AirflowIngestData"
    log_dump = json.dumps(log_data)
    post_data(customer_id, shared_key, log_dump, log_type)

# %% Create Ingest Files

@catch_error(logger)
def create_ingest_adls(source_system:str, schema_name:str, table_name:str, column_names:list, PARTITION:str, storage_account_abbr:str):
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
    log_ingest_data(ingest_data, source_system, schema_name, table_name, storage_account_abbr, PARTITION)

    return ingest_data



# %% Create ingest_data table

@catch_error(logger)
def create_ingest_data_table(ingest_data_per_source_system, container_folder:str, storage_account_name:str):
    json_string = json.dumps(ingest_data_per_source_system)

    save_adls_gen2(
        table_to_save = wid.spark.read.json(wid.spark.sparkContext.parallelize([json_string])).coalesce(1),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = container_folder,
        table_name = 'ingest_data',
        file_format = 'parquet'
    )



# %% Create grant_permissions file

@catch_error(logger)
@action_source_level_tables('grant_permissions')
def create_grant_permissions_file(source_system:str, container_folder:str, storage_account_name:str):
    sqlstr = f"""USE ROLE {wid.snowflake_role};

GRANT USAGE ON DATABASE {wid.snowflake_raw_database} TO ROLE {wid.engineer_role};

GRANT USAGE ON SCHEMA {wid.snowflake_raw_database}.{source_system}_RAW TO ROLE {wid.engineer_role};
GRANT USAGE ON SCHEMA {wid.snowflake_raw_database}.{source_system} TO ROLE {wid.engineer_role};
GRANT USAGE ON SCHEMA {wid.snowflake_raw_database}.{wid.elt_stage_schema} TO ROLE {wid.engineer_role};

GRANT SELECT, INSERT,UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {wid.snowflake_raw_database}.{source_system}_RAW TO ROLE {wid.engineer_role};
GRANT SELECT, INSERT,UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {wid.snowflake_raw_database}.{source_system} TO ROLE {wid.engineer_role};
GRANT SELECT, INSERT,UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {wid.snowflake_raw_database}.{wid.elt_stage_schema} TO ROLE {wid.engineer_role};

GRANT SELECT ON ALL VIEWS IN SCHEMA {wid.snowflake_raw_database}.{source_system}_RAW TO ROLE {wid.engineer_role};
GRANT SELECT ON ALL VIEWS IN SCHEMA {wid.snowflake_raw_database}.{source_system} TO ROLE {wid.engineer_role};
GRANT SELECT ON ALL VIEWS IN SCHEMA {wid.snowflake_raw_database}.{wid.elt_stage_schema} TO ROLE {wid.engineer_role};
"""

    return sqlstr




# %% Create Trigger USP_INGEST() file

@catch_error(logger)
@action_source_level_tables('usp_ingest')
def create_trigger_usp_ingest_file(source_system:str, container_folder:str, storage_account_name:str):
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
    sqlstr = f"""
USE ROLE {wid.snowflake_role};
USE WAREHOUSE {wid.snowflake_raw_warehouse};

ALTER PIPE {wid.snowflake_raw_database}.{wid.elt_stage_schema}.{wid.common_elt_stage_name}_{wid.domain_abbr}_{source_system}_INGEST_REQUEST_PIPE REFRESH;
"""

    print(f'\nTriggering Snowpipe\n{sqlstr}\n')
    exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)



# %% Create Source Level Tables

@catch_error(logger)
def create_source_level_tables(ingest_data_list:defaultdict):
    print(f'\nCreate Source Level Tables')
    for source_system, ingest_data_per_source_system in ingest_data_list.items():
        storage_account_name = default_storage_account_name
        setup_spark_adls_gen2_connection(wid.spark, storage_account_name)
        container_folder = f'metadata/{wid.domain_name}/{source_system}'

        create_ingest_data_table(
            ingest_data_per_source_system = ingest_data_per_source_system,
            container_folder = container_folder,
            storage_account_name = storage_account_name,
            )

        create_grant_permissions_file(
            source_system = source_system,
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

def create_or_replace_func(object_name:str):
    if wid.create_or_replace:
        sqlstr = f'CREATE OR REPLACE {object_name}'
    else:
        sqlstr = f'CREATE {object_name} IF NOT EXISTS'
    return sqlstr



# %% Create Step 1

@catch_error(logger)
@action_step(1)
def step1(source_system:str, schema_name:str, table_name:str, column_names:list):
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
def step2(source_system:str, schema_name:str, table_name:str, column_names:list):
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
def step3(source_system:str, schema_name:str, table_name:str, column_names:list, PARTITION:str, storage_account_abbr:str):
    
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
def step4(source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:OrderedDict):
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
def step5(source_system:str, schema_name:str, table_name:str, column_names:list, src_column_dict:OrderedDict):
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
  ,ROW_NUMBER() OVER (PARTITION BY {pk_column_with_alias} ORDER BY {pk_column_with_alias}, {wid.src_alias}.{wid.execution_date_str} DESC) AS top_slice
FROM {wid.snowflake_raw_database}.{SCHEMA_NAME}.{wid.view_prefix}{TABLE_NAME}{wid.variant_label}{wid.stream_suffix} {wid.src_alias}
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
def step7(source_system:str, schema_name:str, table_name:str, column_names:list, data_types_dict:OrderedDict):
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
def step8(source_system:str, schema_name:str, table_name:str, column_names:list, data_types_dict:OrderedDict):
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
def step9(source_system:str, schema_name:str, table_name:str, column_names:list):
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
def iterate_over_all_tables(tableinfo, table_rows):
    n_tables = len(table_rows)
    ingest_data_list = defaultdict(list)

    for i, table in enumerate(table_rows):
        #if i>3 and is_pc: break
        table_name = table['TableName']
        schema_name = table['SourceSchema']
        source_system = table['SourceDatabase']
        storage_account_name = table['StorageAccount']
        storage_account_abbr = table['StorageAccountAbbr']
        print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

        column_names, pk_column_names, src_column_dict, data_types_dict = get_column_names(tableinfo=tableinfo, source_system=source_system, schema_name=schema_name, table_name=table_name)

        PARTITION = get_partition(spark=wid.spark, domain_name=wid.domain_name, source_system=source_system, schema_name=schema_name, table_name=table_name, storage_account_name=storage_account_name)
        if PARTITION:
            step1(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names)
            step2(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names)
            step3(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)
            step4(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, src_column_dict=src_column_dict)
            step5(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, src_column_dict=src_column_dict)
            step6(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, pk_column_names=pk_column_names)
            step7(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, data_types_dict=data_types_dict)
            step8(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, data_types_dict=data_types_dict)
            step9(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names)
            write_CICD_file_per_table(source_system=source_system, schema_name=schema_name, table_name=table_name)
            ingest_data = create_ingest_adls(source_system=source_system, schema_name=schema_name, table_name=table_name, column_names=column_names, PARTITION=PARTITION, storage_account_abbr=storage_account_abbr)
            ingest_data_list[source_system].append(ingest_data)

    write_CICD_file_per_step()
    print('Finished Iterating over all tables')
    return ingest_data_list



# %%



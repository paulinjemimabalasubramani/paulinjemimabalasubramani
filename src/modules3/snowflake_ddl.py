"""
Common Library for creating and executing (if required) Snowflake DDL Steps and ingest_data

"""

# %% Import Libraries
import os
from functools import wraps
from collections import defaultdict, OrderedDict

from .common_functions import logger, catch_error, data_settings, execution_date_start, get_secrets, EXECUTION_DATE_str, to_sql_value, is_pc
from .spark_functions import ELT_DELETE_IND_str, elt_audit_columns, delta_partitionBy, IDKeyIndicator
from .azure_functions import post_log_data

from snowflake.connector import connect as snowflake_connect



# %% Parameters

class module_params_class:
    """
    Data Class to pass parameters across modules and the main code
    """
    execute_at_snowflake = False # Default False
    create_or_replace = True # Default False - Use True for Schema Change Update
    create_ddl_files = True # Default True

    snowflake_database = f'{data_settings.environment}_{data_settings.domain_name}'.upper()

    ddl_folder = 'DDL'

    variant_label = '_VARIANT'
    variant_alias = 'SRC'

    FILE_FORMAT = 'PARQUET'
    wild_card = '.*.parquet'
    stream_suffix = '_STREAM'

    elt_stage_schema = "ELT_STAGE"

    src_alias = 'src'
    tgt_alias = 'tgt'
    hash_column_name = 'elt_columns_hash'
    integration_id = 'elt_integration_id'
    stream_alias = 'src_strm'
    view_prefix = 'VW_'
    elt_stream_action_alias = 'elt_stream_action'
    nlines = '\n' * 3

    sql_special_column_names = ['select', 'order', 'by', 'from', 'where', 'having']

    spark = None
    snowflake_connection = None
    ddl_file_per_step = defaultdict(str)



wid = module_params_class()
snowflake_ddl_params = wid

if wid.create_ddl_files:
    os.makedirs(name=data_settings.output_ddl_path, exist_ok=True)



# %% Connect to SnowFlake

@catch_error(logger)
def connect_to_snowflake():
    """
    Connect to SnowFlake account
    """
    _, snowflake_user, snowflake_pass = get_secrets(data_settings.snowflake_key_vault_account)

    logger.info(f"Snowflake Key Valut account : {data_settings.snowflake_key_vault_account}, Snowflake user: {snowflake_user}, Snowflake account: {data_settings.snowflake_account}")

    snowflake_connection = snowflake_connect(
        user = snowflake_user,
        password = snowflake_pass,
        account = data_settings.snowflake_account,
        database = wid.snowflake_database,
        warehouse = data_settings.snowflake_warehouse,
        role = data_settings.snowflake_role,
        autocommit = True,
    )

    return snowflake_connection



# %% Base SQL Statement for USE ROLE, USE WAREHOUSE, USE DATABASE

@catch_error(logger)
def use_role_warehouse_database():
    sqlstr = f"""USE ROLE {data_settings.snowflake_role};
USE WAREHOUSE {data_settings.snowflake_warehouse};
USE DATABASE {wid.snowflake_database};"""
    return sqlstr



# %% base sqlstr

@catch_error(logger)
def base_sqlstr(layer:str):
    """
    Base SQL string to have at the beginning of all SQL Strings
    """
    LAYER = f'_{layer}' if layer else ''
    SCHEMA_NAME = f'{data_settings.schema_name}{LAYER}'.upper()

    sqlstr = f"""{use_role_warehouse_database()}
USE SCHEMA {SCHEMA_NAME};
"""
    return SCHEMA_NAME, sqlstr



# %% Action Step

def action_step(step:int):
    """
    Wrapper function around Step functions -> to save and/or execute output SQL from each Step
    """
    def outer(step_fn):
        @wraps(step_fn)
        def inner(*args, **kwargs):
            sqlstr = step_fn(*args, **kwargs)

            if wid.execute_at_snowflake:
                logger.info(f"Executing Snowflake SQL String for domain {data_settings.domain_name.upper()}, schema: {data_settings.schema_name.upper()}, table {kwargs['table_name']}, step {step}")
                exec_status = wid.snowflake_connection.execute_string(sql_text=sqlstr)

        return inner
    return outer



# %% Write DDL File per Table to local file system

@catch_error(logger)
def write_DDL_file_per_table(table_name:str):
    """
    Write DDL File per Table to local file system
    """
    if not wid.create_ddl_files:
        return

    file_folder_path = os.path.join(data_settings.output_ddl_path, 'per_table', data_settings.domain_name.lower(), data_settings.schema_name.upper())
    os.makedirs(name=file_folder_path, exist_ok=True)

    file_path = os.path.join(file_folder_path, f'{table_name.lower()}.sql')

    logger.info(f'Writing: {file_path}')
    with open(file_path, 'w') as f:
        f.write(wid.ddl_file_per_table)



# %% Write DDL File per Step to local file system

@catch_error(logger)
def write_DDL_file_per_step():
    """
    Write DDL File per Step to local file system
    """
    if not wid.create_ddl_files:
        return

    for ddl_file_per_step_key, sqlstr in wid.ddl_file_per_step.items():
        SCHEMA_NAME, file_name = ddl_file_per_step_key

        file_folder_path = os.path.join(data_settings.output_ddl_path, 'per_pipeline', data_settings.pipelinekey.upper(), SCHEMA_NAME.upper())
        os.makedirs(name=file_folder_path, exist_ok=True)

        file_path = os.path.join(file_folder_path, f'{file_name}.sql')

        logger.info(f'Writing: {file_path}')
        with open(file_path, 'w') as f:
            f.write(sqlstr)



# %% Create Ingest Data

@catch_error(logger)
def create_ingest_data(table_name:str, partition_by:str):
    """
    Create Ingest Data
    """
    VARIANT_SCHEMA_NAME, _ = base_sqlstr(layer='RAW')
    NONVARIANT_SCHEMA_NAME, _ = base_sqlstr(layer='')
    PARTITION = delta_partitionBy(partition_by)

    INGEST_STAGE_NAME = f'@{wid.elt_stage_schema}.{data_settings.azure_storage_account_mid.upper()}_{data_settings.domain_name.upper()}_DATALAKE/{data_settings.schema_name.upper()}/{table_name.lower()}/{PARTITION}/'

    VARIANT_TABLE_NAME = f'{VARIANT_SCHEMA_NAME}.{table_name.upper()}{wid.variant_label}'.upper()
    NONVARIANT_TABLE_NAME = f'{NONVARIANT_SCHEMA_NAME}.{table_name.upper()}'.upper()

    COPY_COMMAND = f"COPY INTO {VARIANT_TABLE_NAME} FROM '{INGEST_STAGE_NAME}' FILE_FORMAT = (type='{wid.FILE_FORMAT}') PATTERN = '{wid.wild_card}' ON_ERROR = CONTINUE;"

    ingest_data = {
        'COPY_COMMAND': COPY_COMMAND,
        'VARIANT_TABLE_NAME': VARIANT_TABLE_NAME,
        'NONVARIANT_TABLE_NAME': NONVARIANT_TABLE_NAME,
        'DATABASE_NAME': data_settings.domain_name.upper(),
        'EXECUTION_DATE': to_sql_value(execution_date_start),
        'INGEST_STAGE_NAME': INGEST_STAGE_NAME,
    }

    log_data = {
        **ingest_data,
        'STORAGE_ACCOUNT_NAME': data_settings.storage_account_name,
        'PARTITION': PARTITION,
        }

    post_log_data(log_data=log_data, log_type='AirflowIngestData', logger=logger)

    return ingest_data



# %% Trigger Snowflake Procedure for ingestion

@catch_error(logger)
def trigger_snowflake_ingestion(table_name:str, partition_by:str):
    """
    Trigger Snowflake Procedure for ingestion
    """
    ingest_data = create_ingest_data(table_name=table_name, partition_by=partition_by)
    copy_command = ingest_data['COPY_COMMAND']

    sqlstr_call_usp_ingest = f"""CALL {wid.elt_stage_schema}.USP_INGEST(
            '{to_sql_value(copy_command)}',
            '{ingest_data['VARIANT_TABLE_NAME']}',
            '{ingest_data['NONVARIANT_TABLE_NAME']}',
            '{ingest_data['DATABASE_NAME']}',
            '{ingest_data['EXECUTION_DATE']}',
            '{ingest_data['INGEST_STAGE_NAME']}',
            '{data_settings.elt_process_id}'
        );"""

    cur = wid.snowflake_connection.cursor()
    cur.execute_async(sqlstr_call_usp_ingest)
    query_id = cur.sfqid

    log_data = {
        'call_usp_ingest': sqlstr_call_usp_ingest,
        'copy_command': copy_command,
        'table_name': ingest_data['VARIANT_TABLE_NAME'],
        'query_id': query_id,
    }
    logger.info(log_data)

    if is_pc:
        print('\n' + sqlstr_call_usp_ingest)
        cur.get_results_from_sfqid(query_id)
        results = cur.fetchall()
        print(f'\n\nQuery Results:\n{results[0]}\n\n')

    cur.close()

    post_log_data(log_data=log_data, log_type='AirflowSnowflakeRequests', logger=logger)
    return copy_command



# %% Create or Replace Utility Function

@catch_error(logger)
def create_or_replace_func(object_name:str):
    """
    Utility function to choose wheter to use "CREATE OR REPLACE <object>" or "CREATE <object> IF NOT EXISTS" in sql statements
    """
    if wid.create_or_replace:
        sqlstr = f'CREATE OR REPLACE {object_name}'
    else:
        sqlstr = f'CREATE {object_name} IF NOT EXISTS'
    return sqlstr



# %% Rename SQL Special Column Names:

@catch_error(logger)
def rspcol(column_name:str):
    """
    Rename SQL Special Column Names
    """
    if column_name.lower() in wid.sql_special_column_names:
        column_name = column_name + '1'
    return column_name



# %% Create Step 1

@catch_error(logger)
@action_step(1)
def step1(table_name:str):
    """
    Snowflake DDL: Create Transient Variant Table - Raw data from External Stage table will be copied here.
    """
    layer = 'RAW'
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    wid.ddl_file_per_table = sqlstr

    step = f"""
{create_or_replace_func('TRANSIENT TABLE')} {SCHEMA_NAME}.{table_name.upper()}{wid.variant_label}
(
  {wid.variant_alias} VARIANT
);
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 2

@catch_error(logger)
@action_step(2)
def step2(table_name:str):
    """
    Snowflake DDL: Create Stream on the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    step = f"""
{create_or_replace_func('STREAM')} {SCHEMA_NAME}.{table_name.upper()}{wid.variant_label}{wid.stream_suffix}
ON TABLE {SCHEMA_NAME}.{table_name.upper()}{wid.variant_label} APPEND_ONLY = TRUE;
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 3

@catch_error(logger)
@action_step(3)
def step3(table_name:str, dtypes:OrderedDict):
    """
    Snowflake DDL: Create View on the Stream of the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    column_list_src = '\n  ,'.join(
        [f'SRC:"{column_name}"::string AS {rspcol(column_name)}' for column_name in dtypes] +
        [f'SRC:"{c}"::string AS {c}' for c in elt_audit_columns]
        )

    step = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{wid.view_prefix}{table_name.upper()}{wid.variant_label}{wid.stream_suffix}
AS
SELECT
   {column_list_src}
  ,METADATA$ACTION AS {wid.elt_stream_action_alias}
FROM {SCHEMA_NAME}.{table_name.upper()}{wid.variant_label}{wid.stream_suffix};
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 4

@catch_error(logger)
@action_step(4)
def step4(table_name:str, dtypes:OrderedDict):
    """
    Snowflake DDL: Create View with Integration ID and Hash Column on the Stream of the Variant Table
    """
    layer = 'RAW'
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    column_list = '\n  ,'.join([rspcol(x) for x in dtypes]+elt_audit_columns)
    column_list_with_alias = '\n  ,'.join([f'{wid.src_alias}.{c}' for c in [rspcol(x) for x in dtypes]+elt_audit_columns])

    pk_column_names = [IDKeyIndicator.lower()]
    table_columns_ex_pk = [rspcol(c) for c in dtypes if c.lower() not in pk_column_names]
    if table_columns_ex_pk:
        hash_columns = "SHA2(CONCAT(\n       " + "\n      ,".join([f"COALESCE({wid.src_alias}.{c},'N/A')" for c in table_columns_ex_pk]) + "\n      ))"
    else:
        hash_columns = "'All columns are Primary Keys'"

    INTEGRATION_ID = "TRIM(CONCAT(" + ', '.join([f"COALESCE({wid.src_alias}.{c},'N/A')" for c in pk_column_names]) + "))"
    pk_column_with_alias = ', '.join([f"COALESCE({wid.src_alias}.{c},'N/A')" for c in pk_column_names])

    step = f"""
CREATE OR REPLACE VIEW {SCHEMA_NAME}.{wid.view_prefix}{table_name.upper()}
AS
SELECT
   {wid.integration_id}
  ,{column_list}
  ,{wid.hash_column_name}
  ,{wid.elt_stream_action_alias}
FROM
(
SELECT
   {INTEGRATION_ID} as {wid.integration_id}
  ,{column_list_with_alias}
  ,{hash_columns} AS {wid.hash_column_name}
  ,{wid.src_alias}.{wid.elt_stream_action_alias}
  ,ROW_NUMBER() OVER (PARTITION BY {pk_column_with_alias}, {wid.src_alias}.{wid.elt_stream_action_alias} ORDER BY {pk_column_with_alias}, {wid.src_alias}.{wid.elt_stream_action_alias}, {wid.src_alias}.{EXECUTION_DATE_str} DESC) AS top_slice
FROM {SCHEMA_NAME}.{wid.view_prefix}{table_name.upper()}{wid.variant_label}{wid.stream_suffix} {wid.src_alias}
)
WHERE top_slice = 1;
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 5

@catch_error(logger)
@action_step(5)
def step5(table_name:str, dtypes:OrderedDict):
    """
    Snowflake DDL: Create final destination raw Table
    """
    layer = ''
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    column_list_types = '\n  ,'.join(
        [f'{rspcol(column_name)} {data_type.upper()}' for column_name, data_type in dtypes.items()] +
        [f'{c} VARCHAR' for c in elt_audit_columns]
        )

    step = f"""
{create_or_replace_func('TABLE')} {SCHEMA_NAME}.{table_name.upper()}
(
   {wid.integration_id} VARCHAR NOT NULL
  ,{column_list_types}
  ,{wid.hash_column_name} VARCHAR
  ,CONSTRAINT PK_{SCHEMA_NAME}_{table_name.upper()} PRIMARY KEY ({wid.integration_id}) NOT ENFORCED
);
"""

    sqlstr += step
    wid.ddl_file_per_table += f"\n\nUSE SCHEMA {SCHEMA_NAME};\n\n{step}"
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 6

@catch_error(logger)
@action_step(6)
def step6(table_name:str, dtypes:OrderedDict):
    """
    Snowflake DDL: Create Procedure to UPSERT raw data from variant data stream/view to destination raw table
    """
    layer = ''
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    VARIANT_SCHEMA_NAME, _ = base_sqlstr(layer='RAW')
    NONVARIANT_SCHEMA_NAME, _ = base_sqlstr(layer='')
    VARIANT_TABLE_NAME = f"{VARIANT_SCHEMA_NAME}.{table_name.upper()}{wid.variant_label}".upper()
    NONVARIANT_TABLE_NAME = f"{NONVARIANT_SCHEMA_NAME}.{table_name.upper()}".upper()

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    stored_procedure = f'{SCHEMA_NAME}.USP_{table_name.upper()}_MERGE'

    column_list = '\n    ,'.join([rspcol(x) for x in dtypes] + elt_audit_columns)

    def fval(column_name:str, data_type:str):
        if data_type.upper().startswith('variant'.upper()):
            return f'PARSE_JSON({column_name})'
        elif data_type.upper().startswith('string'.upper()) or data_type.upper().startswith('varchar'.upper()):
            return f"COALESCE({column_name}, '')"
        else:
            return column_name

    merge_update_columns     = '\n    ,'.join([f'{wid.tgt_alias}.{rspcol(c)} = ' + fval(f'{wid.src_alias}.{rspcol(c)}', data_type) for c, data_type in dtypes.items()])
    merge_update_elt_columns = '\n    ,'.join([f'{wid.tgt_alias}.{c} = {wid.src_alias}.{c}' for c in elt_audit_columns])
    merge_update_columns    += '\n    ,' + merge_update_elt_columns

    column_list_with_alias     = '\n    ,'.join([fval(f'{wid.src_alias}.{rspcol(c)}', data_type) for c, data_type in dtypes.items()])
    column_list_with_alias_elt = '\n    ,'.join([f'{wid.src_alias}.{c}' for c in elt_audit_columns])
    column_list_with_alias    += '\n    ,' + column_list_with_alias_elt

    fn_fix_quote = """
function fixQuote(sql) { return sql.replace(/'/g, "''") }
//'
"""

    step = f"""
CREATE OR REPLACE PROCEDURE {stored_procedure}()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
{fn_fix_quote}

var sql_command = 
`
MERGE INTO {SCHEMA_NAME}.{table_name.upper()} {wid.tgt_alias}
USING (
    SELECT * 
    FROM {SCHEMA_NAME}_RAW.{wid.view_prefix}{table_name.upper()}
) {wid.src_alias}
ON (
TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) = TRIM(COALESCE({wid.tgt_alias}.{wid.integration_id},'N/A'))
)
WHEN MATCHED
AND (TRIM(COALESCE({wid.src_alias}.{wid.hash_column_name},'N/A')) != TRIM(COALESCE({wid.tgt_alias}.{wid.hash_column_name},'N/A')) OR {wid.tgt_alias}.{ELT_DELETE_IND_str}=1)
AND TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) != 'N/A'
AND {wid.src_alias}.{wid.elt_stream_action_alias} = 'INSERT'
THEN
  UPDATE
  SET
     {merge_update_columns}
    ,{wid.tgt_alias}.{wid.hash_column_name} = {wid.src_alias}.{wid.hash_column_name}

WHEN NOT MATCHED
AND TRIM(COALESCE({wid.src_alias}.{wid.integration_id},'N/A')) != 'N/A'
AND {wid.src_alias}.{wid.elt_stream_action_alias} = 'INSERT'
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
`;

var soft_delete_command = "CALL ELT_STAGE.USP_SOFT_DELETE_RAW('{VARIANT_TABLE_NAME}', '{NONVARIANT_TABLE_NAME}');";

var usp_ingestion_command = "CALL ELT_STAGE.USP_INGESTION_RESULT('{stored_procedure}', ARRAY_CONSTRUCT('"+fixQuote(sql_command)+"', '"+fixQuote(soft_delete_command)+"'));";

""" + """
try {
    execObj = snowflake.execute({sqlText: usp_ingestion_command});
    if (execObj.getRowCount()>0) {
        execObj.next();
        return execObj.getColumnValueAsString(1);
        }
    return "Succeeded.";
    }
catch (err)  {
    return "Failed: " + err;
    }
$$;
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 7

@catch_error(logger)
@action_step(7)
def step7(table_name:str):
    """
    Snowflake DDL: Create task to run the stored procedure for UPSERT every x minute
    """
    layer = ''
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, f'V0.0.1__Create_{SCHEMA_NAME}_Seed')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    stored_procedure = f'{SCHEMA_NAME}.USP_{table_name.upper()}_MERGE'
    task_suffix = '_MERGE_TASK'
    task_name = f'{table_name.upper()}{task_suffix}'.upper()
    stream_name = f'{SCHEMA_NAME}_RAW.{table_name.upper()}{wid.variant_label}{wid.stream_suffix}'
    warehouse = '{{ WAREHOUSE }}_RAW_WH' # data_settings.snowflake_warehouse

    step = f"""
{create_or_replace_func('TASK')} {SCHEMA_NAME}.{task_name}
WAREHOUSE = {warehouse}
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('{stream_name}')
AS
    CALL {stored_procedure}();
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + wid.nlines
    return sqlstr



# %% Create Step 8

@catch_error(logger)
@action_step(8)
def step8(table_name:str):
    """
    Snowflake DDL: Resume Tasks
    """
    layer = ''
    SCHEMA_NAME, sqlstr = base_sqlstr(layer=layer)

    ddl_file_per_step_key = (SCHEMA_NAME, 'V0.0.2__Resume_Tasks')
    if not wid.ddl_file_per_step[ddl_file_per_step_key]:
        wid.ddl_file_per_step[ddl_file_per_step_key] = f'USE SCHEMA {SCHEMA_NAME};' + wid.nlines

    task_suffix = '_MERGE_TASK'
    task_name = f'{table_name.upper()}{task_suffix}'.upper()

    step = f"""
ALTER TASK {task_name} RESUME;
"""

    sqlstr += step
    wid.ddl_file_per_table += step
    if step not in wid.ddl_file_per_step[ddl_file_per_step_key]: wid.ddl_file_per_step[ddl_file_per_step_key] += step + '\n'*1
    return sqlstr



# %% Create Snowflake DDL

@catch_error(logger)
def create_snowflake_ddl(table, table_name:str, fn_get_dtypes, partition_by:str):
    logger.info(f'Start Creating Snowflake DDL for table {table_name}')
    dtypes = fn_get_dtypes(table=table, table_name=table_name)

    step1(table_name=table_name)
    step2(table_name=table_name)
    step3(table_name=table_name, dtypes=dtypes)
    step4(table_name=table_name, dtypes=dtypes)
    step5(table_name=table_name, dtypes=dtypes)
    step6(table_name=table_name, dtypes=dtypes)
    step7(table_name=table_name)
    step8(table_name=table_name)

    write_DDL_file_per_table(table_name=table_name)
    copy_command = trigger_snowflake_ingestion(table_name=table_name, partition_by=partition_by)
    logger.info(f'Finished Creating Snowflake DDL for table {table_name}')
    return copy_command



# %% Fetch All Data from Snowflake Execution

@catch_error(logger)
def fetchall_snowflake(sqlstr:str):
    """
    Fetch All Data from Snowflake Execution
    """
    cur = snowflake_ddl_params.snowflake_connection.cursor()
    cur.execute(sqlstr)
    results = cur.fetchall()
    column_names = [c[0] for c in cur.description]
    cur.close()

    results_with_column_names = [{column_names[i].upper():x[i] for i in range(len(column_names))} for x in results]
    return results_with_column_names



# %%



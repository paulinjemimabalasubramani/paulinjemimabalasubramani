"""
Load Snowflake log data to Azure Monitor

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


https://docs.snowflake.com/en/user-guide/spark-connector.html
https://docs.databricks.com/_static/notebooks/snowflake-python.html

"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'financial_professional'
sys.domain_abbr = 'FP'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import logger, catch_error, get_secrets, mark_execution_end, post_log_data, data_settings
from modules.spark_functions import create_spark, read_snowflake, table_to_list_dict
from modules.snowflake_ddl import snowflake_ddl_params, connect_to_snowflake



# %% Parameters

sf_account = snowflake_ddl_params.snowflake_account
sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse
sf_databases = data_settings.copy_history_log_databases
sf_schema = snowflake_ddl_params.elt_stage_schema

copy_stream_name = f'{sf_schema}.SNOWFLAKE_COPY_HISTORY_LOG_STREAM'
copy_stream_dump_name = f'{copy_stream_name}_DUMP'
merge_stream_name = f'{sf_schema}.SNOWFLAKE_MERGE_HISTORY_LOG_STREAM'
merge_stream_dump_name = f'{merge_stream_name}_DUMP'



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection



# %% Read Key Vault Data

_, sf_id, sf_pass = get_secrets(snowflake_ddl_params.sf_key_vault_account.lower(), logger=logger)



# %% Post Snowflake Copy History Log to Azure Monitor

@catch_error(logger)
def post_copy_history(sf_database):
    """
    Post Snowflake Copy History Log to Azure Monitor
    """
    query_insert = f"""
USE ROLE {sf_role};
USE WAREHOUSE {sf_warehouse};
USE DATABASE {sf_database};

INSERT OVERWRITE INTO {copy_stream_dump_name}
    (
     FILE_NAME
    ,STAGE_LOCATION
    ,LAST_LOAD_TIME
    ,ROW_COUNT
    ,ROW_PARSED
    ,FILE_SIZE
    ,FIRST_ERROR_MESSAGE
    ,FIRST_ERROR_LINE_NUMBER
    ,FIRST_ERROR_CHARACTER_POS
    ,FIRST_ERROR_COLUMN_NAME
    ,ERROR_COUNT
    ,ERROR_LIMIT
    ,STATUS
    ,TABLE_CATALOG_NAME
    ,TABLE_SCHEMA_NAME
    ,TABLE_NAME
    ,PIPE_CATALOG_NAME
    ,PIPE_SCHEMA_NAME
    ,PIPE_NAME
    ,PIPE_RECEIVED_TIME
    ,INTEGRATION_ID
    ,EXECUTION_DATE
    )
SELECT  
     FILE_NAME
    ,STAGE_LOCATION
    ,LAST_LOAD_TIME
    ,ROW_COUNT
    ,ROW_PARSED
    ,FILE_SIZE
    ,FIRST_ERROR_MESSAGE
    ,FIRST_ERROR_LINE_NUMBER
    ,FIRST_ERROR_CHARACTER_POS
    ,FIRST_ERROR_COLUMN_NAME
    ,ERROR_COUNT
    ,ERROR_LIMIT
    ,STATUS
    ,TABLE_CATALOG_NAME
    ,TABLE_SCHEMA_NAME
    ,TABLE_NAME
    ,PIPE_CATALOG_NAME
    ,PIPE_SCHEMA_NAME
    ,PIPE_NAME
    ,PIPE_RECEIVED_TIME
    ,INTEGRATION_ID
    ,EXECUTION_DATE
FROM {copy_stream_name} S
WHERE S.METADATA$ACTION = 'INSERT';
"""

    logger.info({
        'execute_string': query_insert,
    })

    exec_status = snowflake_connection.execute_string(sql_text=query_insert)

    table = read_snowflake(
        spark = spark,
        table_name = copy_stream_dump_name,
        schema = sf_schema,
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        is_query = False,
        )

    table_collect = table_to_list_dict(table)

    logger.info({
        'row_count': len(table_collect),
    })

    for log_data in table_collect:
        try:
            post_log_data(log_data=log_data, log_type='SnowflakeCopyHistory', logger=logger)
        except Exception as e:
            logger.error(str(e))


# %% Post Snowflake Copy History Log to Azure Monitor

@catch_error(logger)
def post_merge_history(sf_database):
    """
    Post Snowflake Merge History Log to Azure Monitor
    """
    query_insert = f"""
USE ROLE {sf_role};
USE WAREHOUSE {sf_warehouse};
USE DATABASE {sf_database};

INSERT OVERWRITE INTO {merge_stream_dump_name}
    (
     QUERY_ID
    ,NAME
    ,DATABASE_NAME
    ,SCHEMA_NAME
    ,QUERY_TEXT
    ,CONDITION_TEXT
    ,STATE
    ,ERROR_CODE
    ,ERROR_MESSAGE
    ,SCHEDULED_TIME
    ,QUERY_START_TIME
    ,NEXT_SCHEDULED_TIME
    ,COMPLETED_TIME
    ,ROOT_TASK_ID
    ,GRAPH_VERSION
    ,RUN_ID
    ,RETURN_VALUE
    )
SELECT  
     QUERY_ID
    ,NAME
    ,DATABASE_NAME
    ,SCHEMA_NAME
    ,QUERY_TEXT
    ,CONDITION_TEXT
    ,STATE
    ,ERROR_CODE
    ,ERROR_MESSAGE
    ,SCHEDULED_TIME
    ,QUERY_START_TIME
    ,NEXT_SCHEDULED_TIME
    ,COMPLETED_TIME
    ,ROOT_TASK_ID
    ,GRAPH_VERSION
    ,RUN_ID
    ,RETURN_VALUE
FROM {merge_stream_name} S
WHERE S.METADATA$ACTION = 'INSERT';
"""

    logger.info({
        'execute_string': query_insert,
    })

    exec_status = snowflake_connection.execute_string(sql_text=query_insert)

    table = read_snowflake(
        spark = spark,
        table_name = merge_stream_dump_name,
        schema = sf_schema,
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        is_query = False,
        )

    table_collect = table_to_list_dict(table)

    logger.info({
        'row_count': len(table_collect),
    })

    for log_data in table_collect:
        try:
            post_log_data(log_data=log_data, log_type='SnowflakeMergeHistory', logger=logger)
        except Exception as e:
            logger.error(str(e))



# %% Iterate over given databases

@catch_error(logger)
def iterate_over_given_databases(sf_databases):
    for sf_database in sf_databases:
        post_copy_history(sf_database=sf_database)
        post_merge_history(sf_database=sf_database)



iterate_over_given_databases(sf_databases=sf_databases)



# %% Mark Execution End

mark_execution_end()


# %%


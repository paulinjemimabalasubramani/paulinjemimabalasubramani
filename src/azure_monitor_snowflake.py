"""
Move Snowflake information_schema history data to Azure Monitor

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


https://docs.snowflake.com/en/user-guide/spark-connector.html
https://docs.databricks.com/_static/notebooks/snowflake-python.html

"""

# %% Import Libraries

import logging
import os, sys, json
sys.parent_name = os.path.basename(__file__)

from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import logger, catch_error, get_secrets, mark_execution_end, post_log_data
from modules.spark_functions import create_spark, read_snowflake
from modules.snowflake_ddl import snowflake_ddl_params


from pyspark.sql.functions import col, lit



# %% Parameters

sf_account = snowflake_ddl_params.snowflake_account
sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse

sf_databases = ['QA_RAW_FP']


# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark



# %% Read Key Vault Data

_, sf_id, sf_pass = get_secrets(snowflake_ddl_params.sf_key_vault_account.lower(), logger=logger)



# %% Get List of Schemas for a given database from Information Schema

@catch_error(logger)
def get_sf_schema_list(sf_database:str):
    logger.info('Get List of Tables from Information Schema...')
    tables = read_snowflake(
        spark = spark,
        table_name = 'TABLES',
        schema = 'INFORMATION_SCHEMA',
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        )

    tables = tables.where(
        (col('TABLE_TYPE')==lit('BASE TABLE')) &
        (col('IS_TRANSIENT')==lit('NO'))
    )

    sf_schemas = tables.select('TABLE_SCHEMA').distinct().rdd.flatMap(lambda x: x).collect()

    logger.info({
        'database': f'{sf_database}',
        'count_schemas': len(sf_schemas),
        'schemas': sf_schemas,
        })
    return sf_schemas, tables



# %%

def get_snowflake_copy_history(spark, sf_database:str, sf_schema:str, table_name:str, start_time:str=None):
    if not start_time:
        start_timex = f"DATEADD(DAYS, -14, CURRENT_TIMESTAMP())"
    else:
        start_timex = f"DATEADD(SECONDS, 1, TO_TIMESTAMP_LTZ('{start_time}'))"

    sqlstr = f"SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'{table_name}', START_TIME=>{start_timex}));"

    logger.info({
        'sqlstr': sqlstr,
        'schema': sf_schema,
        'database': sf_database,
        'warehouse': sf_warehouse,
        'role': sf_role,
        })

    table = read_snowflake(
        spark = spark,
        table_name = sqlstr,
        schema = sf_schema,
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        is_query = True,
        )
    return table



# %%

@catch_error(logger)
def post_all_snowflake_copy_history_log():
    for sf_database in sf_databases:
        sf_schemas, tables = get_sf_schema_list(sf_database=sf_database)

        for sf_schema in sf_schemas:
            if (sf_schema.upper() in ['INFORMATION_SCHEMA']) or (sf_schema.upper() not in ['LR_RAW']):
                continue
            tables_per_schema = tables.where(col('TABLE_SCHEMA')==lit(sf_schema)).select('TABLE_NAME').distinct().rdd.flatMap(lambda x: x).collect()

            for table_name in tables_per_schema:
                if table_name.upper() in ['CICD_CHANGE_HISTORY']:
                    continue
                logging.info(f'Getting Log Data from {sf_database}.{sf_schema}.{table_name}')

                table = get_snowflake_copy_history(spark=spark, sf_database=sf_database, sf_schema=sf_schema, table_name=table_name, start_time=None)
                table_collect = table.toJSON().map(lambda j: json.loads(j)).collect()

                for log_data in table_collect:
                    post_log_data(log_data=log_data, log_type='SnowflakeCopyHistory')



post_all_snowflake_copy_history_log()



# %% Mark Execution End

mark_execution_end()


# %%


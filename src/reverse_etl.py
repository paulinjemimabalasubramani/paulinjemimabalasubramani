"""
Move curated data in Snowflake back to on prem SQL Server

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


from modules.common_functions import logger, catch_error, get_secrets, mark_execution_end, data_settings
from modules.spark_functions import collect_column, create_spark, write_sql, read_snowflake
from modules.azure_functions import default_storage_account_name, setup_spark_adls_gen2_connection
from modules.migrate_files import get_DataTypeTranslation_table
from modules.snowflake_ddl import snowflake_ddl_params


from pyspark.sql.functions import col, lit



# %% Parameters

reverse_etl_map = data_settings.reverse_etl_map

sql_server = data_settings.reverse_etl_sql_server
sql_key_vault_account = data_settings.reverse_etl_sql_key_vault_account

sf_account = snowflake_ddl_params.snowflake_account
sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse

data_type_translation_id = 'snowflake_sqlserver'



# %% Create Session

spark = create_spark()
snowflake_ddl_params.spark = spark



# %% Read Key Vault Data

_, sql_id, sql_pass = get_secrets(sql_key_vault_account.lower(), logger=logger)
_, sf_id, sf_pass = get_secrets(snowflake_ddl_params.sf_key_vault_account.lower(), logger=logger)



# %% Get Translation Table
storage_account_name = default_storage_account_name
setup_spark_adls_gen2_connection(spark, storage_account_name)

translation = get_DataTypeTranslation_table(spark=spark, data_type_translation_id=data_type_translation_id)



# %% Get List of Tables and Columns from Information Schema

@catch_error(logger)
def get_sf_table_list(sf_database:str, sf_schema:str):
    """
    Get List of Tables and Columns from Snowflake database Information Schema
    """
    logger.info('Get List of Tables and Columns from Information Schema...')
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

    columns = read_snowflake(
        spark = spark,
        table_name = 'COLUMNS',
        schema = 'INFORMATION_SCHEMA',
        database = sf_database,
        warehouse = sf_warehouse,
        role = sf_role,
        account = sf_account,
        user = sf_id,
        password = sf_pass,
        )

    tables = tables.where(
        (col('TABLE_SCHEMA')==lit(sf_schema)) & 
        (col('TABLE_TYPE')==lit('BASE TABLE')) &
        (col('IS_TRANSIENT')==lit('NO'))
        )

    columns = columns.where(col('TABLE_SCHEMA')==lit(sf_schema))
    columns = columns.alias('c').join(tables.alias('t'), columns['TABLE_NAME']==tables['TABLE_NAME'], how='inner').select('c.*')

    table_names = collect_column(table=columns, column_name='TABLE_NAME', distinct=True)

    logger.info({
        'schema': f'{sf_database}.{sf_schema}',
        'count_tables': len(table_names),
        'tables': table_names,
        })
    return table_names, columns



# %% Loop over all tables

@catch_error(logger)
def reverse_etl_all_tables(table_names, sf_database:str, sf_schema:str, sql_database:str, sql_schema:str):
    """
    Loop over all tables to read from Snowflake and write to SQL Server
    """
    for table_name in table_names:
        if table_name in ['CICD_CHANGE_HISTORY']: continue

        try:
            table = read_snowflake(
                spark = spark,
                table_name = f'SELECT * FROM {table_name} WHERE SCD_IS_CURRENT=1;',
                schema = sf_schema,
                database = sf_database,
                warehouse = sf_warehouse,
                role = sf_role,
                account = sf_account,
                user = sf_id,
                password = sf_pass,
                is_query = True,
                )

            write_sql(
                table = table,
                table_name = table_name.lower(),
                schema = sql_schema,
                database = sql_database,
                server = sql_server,
                user = sql_id,
                password = sql_pass,
                mode = 'overwrite',
            )

        except Exception as e:
            logger.error(str(e))

    logger.info(f'Finished Reverse ETL for all tables for {sf_database}.{sf_schema}')



# %% Loop over all databases

@catch_error(logger)
def reverse_etl_all_databases():
    """
    Loop over all databases and tables to read from Snowflake and write to SQL Server
    """
    for sf_database, val in reverse_etl_map.items():
        sf_schema = val['snowflake_schema']
        logger.info(f'Reverse ETL {sf_database}.{sf_schema}')
        table_names, columns = get_sf_table_list(sf_database=sf_database, sf_schema=sf_schema)
        reverse_etl_all_tables(table_names=table_names, sf_database=sf_database, sf_schema=sf_schema, sql_database=val['sql_database'], sql_schema=val['sql_schema'])



reverse_etl_all_databases()



# %% Mark Execution End

mark_execution_end()


# %%


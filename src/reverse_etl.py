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

import os, sys, pymssql
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'financial_professional'
sys.domain_abbr = 'FP'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import logger, catch_error, get_secrets, mark_execution_end, data_settings, execution_date, is_pc
from modules.spark_functions import collect_column, create_spark, write_sql, read_snowflake, get_columns_list_from_columns_table
from modules.azure_functions import default_storage_account_name, default_storage_account_abbr, setup_spark_adls_gen2_connection
from modules.migrate_files import get_DataTypeTranslation_table, add_TargetDataType, rename_columns, add_precision
from modules.snowflake_ddl import snowflake_ddl_params


from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit



# %% Parameters

reverse_etl_map = data_settings.reverse_etl_map

sql_server = data_settings.reverse_etl_sql_server
sql_key_vault_account = data_settings.reverse_etl_sql_key_vault_account

sf_account = snowflake_ddl_params.snowflake_account
sf_role = snowflake_ddl_params.snowflake_role
sf_warehouse = snowflake_ddl_params.snowflake_raw_warehouse

data_type_translation_id = 'snowflake_sqlserver'

sql_merge_schema = 'EDIP'



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



# %% Re-create SQL Table with the latest schema

@catch_error(logger)
def recreate_sql_table(columns, table_name:str, sf_schema:str, sql_schema:str, sql_database:str, sql_server:str, sql_id:str, sql_pass:str):
    """
    Re-create SQL Table with the latest schema
    """
    logger.info(f'Recreating SQL Server table: {sql_database}.{sql_schema}.{table_name}')

    columns = (columns
        .where(
            (F.upper(col('TABLE_SCHEMA'))==lit(sf_schema.upper())) & 
            (F.upper(col('TABLE_NAME'))==lit(table_name.upper()))
            )
        .distinct()
        .withColumn('KEY_COLUMN_NAME', lit(0))
        )

    columns = rename_columns(
        columns = columns,
        storage_account_name = default_storage_account_name,
        created_datetime = execution_date,
        modified_datetime = execution_date,
        storage_account_abbr = default_storage_account_abbr,
        )

    columns = add_TargetDataType(columns=columns, translation=translation)
    columns = add_precision(columns=columns, truncate_max_varchar=1000)

    column_list = get_columns_list_from_columns_table(
        columns = columns,
        column_names = ['TargetColumnName', 'TargetDataType', 'IsNullable', 'IS_IDENTITY'],
        OrdinalPosition = 'OrdinalPosition'
        )

    column_listx = [f"[{c['TargetColumnName']}] {c['TargetDataType']} {'NULL' if c['IsNullable']>0 else 'NOT NULL'}" for c in column_list]
    column_listx = '\n  ,'.join(column_listx)
    sqlstr = f'CREATE TABLE [{sql_schema}].[{table_name}] ({column_listx});'

    conn = pymssql.connect(sql_server, sql_id, sql_pass, sql_database)
    cursor = conn.cursor()
    sqlstr_drop = f'DROP TABLE IF EXISTS {sql_schema}.{table_name};'
    logger.info({'execute': sqlstr_drop})
    cursor.execute(sqlstr_drop)
    conn.commit()
    logger.info({'execute': sqlstr})
    cursor.execute(sqlstr)
    conn.commit()
    conn.close()

    return column_list



# %% Merge Raw SQL Table to final SQL Table

@catch_error(logger)
def merge_sql_table(column_list:list, table_name:str, sql_merge_schema:str, sql_schema:str, sql_database:str, sql_server:str, sql_id:str, sql_pass:str):
    id_columns = [c for c in column_list if c['IS_IDENTITY']=='YES']
    trim_coalesce = lambda x: f"LTRIM(RTRIM(COALESCE({x},'N/A')))"
    column_name = 'TargetColumnName'
    merge_on = '\n    AND '.join([f"{trim_coalesce('src.['+c[column_name]+']')} = {trim_coalesce('tgt.['+c[column_name]+']')}" for c in id_columns])
    check_na = '\n    '.join([f"AND {trim_coalesce('src.['+c[column_name]+']')} != 'N/A'" for c in id_columns])
    update_set = '\n   ,'.join([f"tgt.[{c[column_name]}]=src.[{c[column_name]}]" for c in column_list])
    insert_columns = '\n   ,'.join([f"[{c[column_name]}]" for c in column_list])
    insert_values = '\n   ,'.join([f"src.[{c[column_name]}]" for c in column_list])

    sqlstr = f"""
MERGE INTO [{sql_merge_schema}].[{table_name}] tgt
USING [{sql_schema}].[{table_name}] src
ON {merge_on}
WHEN MATCHED {check_na}
THEN UPDATE SET {update_set}
WHEN NOT MATCHED {check_na}
THEN
  INSERT ({insert_columns})
  VALUES ({insert_values})
;
"""

    return sqlstr



# %% Loop over all tables

@catch_error(logger)
def reverse_etl_all_tables(table_names, columns, sf_database:str, sf_schema:str, sql_database:str, sql_schema:str):
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

            column_list = recreate_sql_table(
                columns = columns,
                table_name = table_name.lower(),
                sf_schema = sf_schema,
                sql_schema = sql_schema,
                sql_database = sql_database,
                sql_server = sql_server,
                sql_id = sql_id,
                sql_pass = sql_pass,
            )

            write_sql(
                table = table,
                table_name = table_name.lower(),
                schema = sql_schema,
                database = sql_database,
                server = sql_server,
                user = sql_id,
                password = sql_pass,
                mode = 'append',
            )

            merge_sql_table(
                column_list = column_list,
                table_name = table_name.lower(),
                sql_merge_schema = sql_merge_schema,
                sql_schema = sql_schema,
                sql_database = sql_database,
                sql_server = sql_server,
                sql_id = sql_id,
                sql_pass = sql_pass,
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
        reverse_etl_all_tables(table_names=table_names, columns=columns, sf_database=sf_database, sf_schema=sf_schema, sql_database=val['sql_database'], sql_schema=val['sql_schema'])



reverse_etl_all_databases()



# %% Mark Execution End

mark_execution_end()


# %%


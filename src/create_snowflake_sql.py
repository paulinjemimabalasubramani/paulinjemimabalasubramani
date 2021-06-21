
# %% Import Libraries
import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.data_functions import elt_auto_columns
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

file_format = 'PARQUET'
wild_card = '.*.parquet'
stream_suffix = '_STREAM'

ddl_folder = f'metadata/{domain_name}/DDL'



# %% Create Session

spark = create_spark()

# %% Read metadata.TableInfo

tableinfo, table_rows = read_tableinfo(spark)


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% base sqlstr

@catch_error(logger)
def base_sqlstr(source_system):
    sqlstr = f"""USE ROLE AD_SNOWFLAKE_QA_DBA;
USE WAREHOUSE QA_INGEST_WH;
USE DATABASE QA_RAW_FP;
USE SCHEMA {source_system};


"""
    return sqlstr



# %% Create Step 1

@catch_error(logger)
def step1(base_sqlstr, source_system, schema_name, table_name, column_names):
    step = 1

    sqlstr = base_sqlstr
    sqlstr += f'CREATE OR REPLACE TABLE {source_system}.{table_name} \n(\n'
    cols = [f'{c} string \n' for c in column_names]
    cols[0] = '   '+cols[0]
    sqlstr += '  ,'.join(cols)
    sqlstr +=');'

    if is_pc: print(f'\n{sqlstr}\n')

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )


# %% Get Execution Date for a Table

@catch_error(logger)
def get_execution_date(source_system, schema_name, table_name):
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

    EXECUTION_DATE_LIST = df.limit(1).collect()
    if EXECUTION_DATE_LIST:
        EXECUTION_DATE = EXECUTION_DATE_LIST[0]['EXECUTION_DATE']
        return EXECUTION_DATE.replace(' ', '%20').replace(':', '%3A')
    else:
        print(f'{container_folder}/{table_name} is EMPTY - SKIPPING')
        return None



# %% Create Step 2

@catch_error(logger)
def step2(base_sqlstr, source_system, schema_name, table_name, column_names, EXECUTION_DATE):
    step = 2

    sqlstr = base_sqlstr
    sqlstr += f'COPY INTO {source_system}.{table_name} \nFROM (\nSELECT \n'
    cols = [f'$1:"{c}"::string AS {c} \n' for c in column_names]
    cols[0] = '   '+cols[0]
    sqlstr += '  ,'.join(cols)
    sqlstr += f"FROM @ELT_STAGE.AGGR_FP_DATALAKE/{source_system}/{schema_name}/{table_name}/EXECUTION_DATE={EXECUTION_DATE}/\n) \n"
    sqlstr += f"FILE_FORMAT = (type='{file_format}') \n"
    sqlstr += f"PATTERN = '{wild_card}' \n"
    sqlstr +='ON_ERROR = CONTINUE; \n'

    sqlstr +=f"""
SET SOURCE_SYSTEM = '{source_system}';
SET TARGET_TABLE = '{table_name}';
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
"""

    if is_pc: print(f'\n{sqlstr}\n')

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )




# %% Create Step 3

@catch_error(logger)
def step3(base_sqlstr, source_system, schema_name, table_name, column_names):
    step = 3

    sqlstr = base_sqlstr
    sqlstr += f"CREATE OR REPLACE STREAM {source_system}.{table_name}{stream_suffix} ON TABLE {source_system}.{table_name};"

    if is_pc: print(f'\n{sqlstr}\n')

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

        # Get Table Columns
        column_names = tableinfo.filter(
            (col('TableName') == table_name) &
            (col('SourceSchema') == schema_name) &
            (col('SourceDatabase') == source_system)
            ).select('SourceColumnName').rdd.flatMap(lambda x: x).collect()

        column_names += elt_auto_columns

        EXECUTION_DATE = get_execution_date(source_system, schema_name, table_name)
        if EXECUTION_DATE:
            base_sqlstr1 = base_sqlstr(source_system)
            step1(base_sqlstr1, source_system, schema_name, table_name, column_names)
            step2(base_sqlstr1, source_system, schema_name, table_name, column_names, EXECUTION_DATE)
            step3(base_sqlstr1, source_system, schema_name, table_name, column_names)

    print('Finished Iterating over all tables')


iterate_over_all_tables(tableinfo, table_rows)


# %%

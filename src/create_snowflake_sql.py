
# %% Import Libraries
import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_adls_gen2


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
tableinfo_folder = 'metadata'
tableinfo_name = 'metadata.TableInfo'
format = 'delta'
file_format = 'delta'
wild_card = '*.parquet'
stream_suffix = '_STREAM'

ddl_folder = 'DDL'

strftime = "%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = datetime.now()
created_datetime = execution_date.strftime(strftime)
modified_datetime = execution_date.strftime(strftime)



# %% Create Session

spark = create_spark()


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Read metadata.TableInfo

tableinfo = read_adls_gen2(
    spark = spark,
    storage_account_name = storage_account_name,
    container_name = container_name,
    container_folder = tableinfo_folder,
    table = tableinfo_name,
    format = format
)

tableinfo = tableinfo.filter(col('IsActive')==lit(1))
tableinfo.persist()


# %% Create unique list of tables

table_list = tableinfo.select(
    col('SourceDatabase'),
    col('SourceSchema'),
    col('TableName')
    ).distinct()

if is_pc: table_list.show(5)

table_rows = table_list.collect()
n_tables = len(table_rows)
print(f'Number of Tables in {tableinfo_name} is {n_tables}')


# %% Create Step 1

@catch_error(logger)
def step1(source_system, schema_name, table_name, column_names):
    step = 1

    sqlstr = f'CREATE OR REPLACE TABLE {schema_name}.{table_name} \n(\n'
    cols = [f'{c} string \n' for c in column_names]
    cols[0] = '  '+cols[0]
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



# %% Create Step 2

@catch_error(logger)
def step2(source_system, schema_name, table_name, column_names):
    step = 2

    sqlstr = f'COPY INTO {schema_name}.{table_name} \nFROM (\nSELECT \n'
    cols = [f'$1:"{c}"::string AS {c} \n' for c in column_names]
    cols[0] = '  '+cols[0]
    sqlstr += '  ,'.join(cols)
    sqlstr += f'FROM @ELT_STAGE.AGGR_FP_DATALAKE/{source_system}/{schema_name}/{table_name}/\n) \n'
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
   COLUMN_NAME
  ,ROW_NUMBER
  ,ROW_START_LINE
  ,REJECTED_RECORD

FROM TABLE(validate({schema_name}.{table_name}, job_id => '_last')); 
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
def step3(source_system, schema_name, table_name, column_names):
    step = 3

    sqlstr =f"CREATE OR REPLACE STREAM {schema_name}.{table_name}{stream_suffix} ON TABLE {schema_name}.{table_name};"

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
for i, table in enumerate(table_rows):
    table_name = table['TableName']
    schema_name = table['SourceSchema']
    source_system = table['SourceDatabase']
    print(f'\nProcessing table {i} of {n_tables}: {source_system}/{schema_name}/{table_name}')

    # Get Table Columns
    column_names = tableinfo.filter(
        (col('TableName') == table_name) &
        (col('SourceSchema') == schema_name) &
        (col('SourceDatabase') == source_system)
        ).select('SourceColumnName').rdd.flatMap(lambda x: x).collect()

    step1(source_system, schema_name, table_name, column_names)
    step2(source_system, schema_name, table_name, column_names)
    step3(source_system, schema_name, table_name, column_names)



print('Done')



# %% Import Libraries
import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.data_functions import elt_audit_columns, partitionBy
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

ddl_folder = f'metadata/{domain_name}/DDL'

# Steps 1-3 parameters:
file_format = 'PARQUET'
wild_card = '.*.parquet'
stream_suffix = '_STREAM'

# Steps 4-6 parameters:
environment = 'QA'
snowflake_raw_database = f'{environment}_RAW_FP'
snowflake_transformed_database = f'{environment}_PERSISTENT_FP'

src_alias = 'src' # Driver Table I.E. Non-Stream
tgt_alias = 'tgt'
hash_column_name = 'MD5_HASH'
stream_alias = 'src_strm' # Stream object
view_prefix = 'VW_'
execution_date_str = 'EXECUTION_DATE'


# TODO
column_list_with_business_key = 'This should be the column list with NO alias and it should include the PK too and ELT audit columns'
column_list_with_alias = 'This should be the column list with the {src_alias} --> alias and it should include the PK too and ELT audit columns'
columns_with_alias = 'This should be all of the columns with an alias # I.E. {stream_alias}.column1, {stream_alias}.column2'
columns_without_alias = 'This should be the columns without any alias'

integration_id = 'This should be the concatenation of all of the PK for a table.'



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
USE WAREHOUSE QA_RAW_WH;
USE DATABASE QA_RAW_FP;
USE SCHEMA {source_system};


"""
    return sqlstr



# %% Create Step 1

@catch_error(logger)
def step1(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list):
    step = 1

    sqlstr = base_sqlstr
    sqlstr += f'CREATE OR REPLACE TABLE {source_system.upper()}.{schema_name.upper()}_{table_name.upper()} \n(\n'
    cols = [f'{c} string \n' for c in column_names]
    cols[0] = '   '+cols[0]
    sqlstr += '  ,'.join(cols)
    sqlstr +=');'

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
def step2(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list):
    step = 2

    sqlstr = base_sqlstr
    sqlstr += f"CREATE OR REPLACE STREAM {source_system.upper()}.{schema_name.upper()}_{table_name.upper()}{stream_suffix} ON TABLE {source_system.upper()}.{schema_name.upper()}_{table_name.upper()};"

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
def get_partition(source_system:str, schema_name:str, table_name:str):
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

    PARTITION_LIST = df.select(F.max(col(partitionBy)).alias('part')).collect()
    PARTITION = PARTITION_LIST[0]['part']
    if PARTITION:
        return PARTITION.replace(' ', '%20').replace(':', '%3A')
    else:
        print(f'{container_folder}/{table_name} is EMPTY - SKIPPING')
        return None



# %% Create Step 3

@catch_error(logger)
def step3(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list, PARTITION:str):
    step = 3

    sqlstr = base_sqlstr
    sqlstr += f'COPY INTO {source_system.upper()}.{schema_name.upper()}_{table_name.upper()} \nFROM (\nSELECT \n'
    cols = [f'$1:"{c}"::string AS {c} \n' for c in column_names]
    cols[0] = '   '+cols[0]
    sqlstr += '  ,'.join(cols)
    sqlstr += f"FROM @ELT_STAGE.AGGR_FP_DATALAKE/{source_system}/{schema_name}/{table_name}/{partitionBy}={PARTITION}/\n) \n"
    sqlstr += f"FILE_FORMAT = (type='{file_format}') \n"
    sqlstr += f"PATTERN = '{wild_card}' \n"
    sqlstr +='ON_ERROR = CONTINUE; \n'

    sqlstr +=f"""
SET SOURCE_SYSTEM = '{source_system.upper()}';
SET TARGET_TABLE = '{schema_name.upper()}_{table_name.upper()}';
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

    save_adls_gen2(
        df = spark.createDataFrame([sqlstr], StringType()),
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = f'{ddl_folder}/{source_system}/step_{step}/{schema_name}',
        table = table_name,
        format = 'text'
    )


# %% Create Step 4 - Preparation

i = 0
n_tables = len(table_rows)

table = table_rows[i]
table_name = table['TableName']
schema_name = table['SourceSchema']
source_system = table['SourceDatabase']
print(f'\nProcessing table {i+1} of {n_tables}: {source_system}/{schema_name}/{table_name}')

column_names = tableinfo.filter(
    (col('TableName') == table_name) &
    (col('SourceSchema') == schema_name) &
    (col('SourceDatabase') == source_system)
    ).select('SourceColumnName').rdd.flatMap(lambda x: x).collect()

PARTITION = get_partition(source_system, schema_name, table_name)


# %% Create Step 4

@catch_error(logger)
def step4(base_sqlstr:str, source_system:str, schema_name:str, table_name:str, column_names:list):
    pass

step = 4

sqlstr = base_sqlstr(source_system)

sqlstr += f"""
CREATE OR REPLACE VIEW {schema_name}.{view_prefix}{table_name}
AS
SELECT  
       {integration_id}
       {columns_without_alias}
      ,{hash_column_name}
      ,{','.join(elt_audit_columns)}
FROM      
(
SELECT 
       -- Please include a CONCAT_BUSINESS_KEY, which is the concatenation of ALL the PK from the source system, you can use the KeyIndicator = 1 and concatenate all the keys into this column
       -- Please wrap each PK column with a COALESCE(pk_column,'N/A'). 
       -- Please have the concatenate seperator as a tilda (~) between each columns.
       -- src.IntegrationID should be the column name
       {columns_with_alias}	   
	  -- Need to ask Jared if we are brining over any ELT audit columns, if so, then they should be included here!!
	  ,{hash_column_name} --> {hash_column_name} --> See Steps4to5 output view example --> --BEGIN build of the MD5 Hash
      -- Include the MD5 for ALL of the Non-PK columns and the Non-ELT Audit Columns
      -- You should concatenate all of the columns and wrap them with a COALESCE(column,'N/A')
      -- Please have the concatenate seperator as a tilda (~) between each columns.
      -- Once you have all of the columns concatenated together, then you can wrap the string with the Snowflake MD5('concanated colunns') AS MD5Hash
	  ,ROW_NUMBER() OVER (PARTITION BY {src_alias}.{integration_id} ORDER BY {src_alias}.{integration_id},{src_alias}.{execution_date} DESC) AS top_slice
FROM    {snowflake_raw_database}.{schema_name}.{table_name}{stream_suffix} {stream_alias}
LEFT OUTER JOIN {snowflake_raw_database}.{schema_name}.{table_name} {src_alias}
      ON {src_alias}.{integration_id} = {stream_alias}.{integration_id}                            
WHERE
    TRIM({stream_alias}.{integration_id}) IS NOT NULL
)
WHERE top_slice = 1 ;
"""




print(sqlstr)


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

        PARTITION = get_partition(source_system, schema_name, table_name)
        if PARTITION:
            base_sqlstr1 = base_sqlstr(source_system)
            step1(base_sqlstr1, source_system, schema_name, table_name, column_names+elt_audit_columns)
            step2(base_sqlstr1, source_system, schema_name, table_name, column_names+elt_audit_columns)
            step3(base_sqlstr1, source_system, schema_name, table_name, column_names+elt_audit_columns, PARTITION)

    print('Finished Iterating over all tables')


iterate_over_all_tables(tableinfo, table_rows)


# %%


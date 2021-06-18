
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


# %% Create unique list of tables

table_list = tableinfo.select(
    col('SourceDatabase'),
    col('SourceSchema'),
    col('TableName')
    ).distinct()

if is_pc: table_list.show(5)

table_rows = table_list.collect()
print(f'Number of Tables in {tableinfo_name} is {len(table_rows)}')

# %% Iterate over tables

table = table_rows[0]


# %%




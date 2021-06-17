# %% Import Libraries
import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.config import is_pc
from modules.spark_functions import create_spark, read_csv
from modules.azure_functions import get_azure_storage_key_vault, save_adls_gen2_sp


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters
data_type_translation_path = os.path.realpath(os.path.dirname(__file__)+'/../metadata_source_files/DataTypeTranslation.csv')
assert os.path.isfile(data_type_translation_path)

data_type_translation_id = 'sqlserver_snowflake'


# %% Create Session

spark = create_spark()



# %% Get DataTypeTranslation table

@catch_error(logger)
def get_translation(data_type_translation_id:str):
    translation = read_csv(spark, data_type_translation_path)
    translation.printSchema()
    translation = translation.filter(
                        (col('DataTypeTranslationID') == lit(data_type_translation_id).cast("string")) & 
                        (col('IsActive') == lit(1))
                        )

    translation.createOrReplaceTempView('translation')
    return translation



translation = get_translation(data_type_translation_id)

# %%






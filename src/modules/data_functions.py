"""
Library for common data functions

"""

# %% Import Libraries

from datetime import datetime
import re

from .common_functions import make_logging, catch_error
from .config import is_pc

from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# %% Logging
logger = make_logging(__name__)


# %% Parameters

strftime = r"%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = datetime.now().strftime(strftime)

column_regex = r'[\W]+'

metadata_DataTypeTranslation = 'metadata.DataTypeTranslation'
metadata_MasterIngestList = 'metadata.MasterIngestList'
metadata_FirmSourceMap = 'metadata.FirmSourceMap'



# %% Remove Column Spaces

@catch_error(logger)
def remove_column_spaces(table_to_remove):
    """
    Removes spaces from column names
    """
    new_table_to_remove = table_to_remove.select([col(c).alias(re.sub(column_regex, '_', c)) for c in table_to_remove.columns])
    return new_table_to_remove


# %% Convert timestamp's or other types to string

@catch_error(logger)
def to_string(table_to_convert_columns, col_types=['timestamp']):
    """
    Convert timestamp's or other types to string - as it cause errors otherwise.
    """
    string_type = 'string'
    for col_name, col_type in table_to_convert_columns.dtypes:
        if (not col_types or col_type in col_types) and (col_type != string_type):
            print(f"Converting {col_name} from '{col_type}' to 'string' type")
            table_to_convert_columns = table_to_convert_columns.withColumn(col_name, col(col_name).cast(string_type))
    
    return table_to_convert_columns



# %% Add ELT Audit Columns

elt_audit_columns = ['RECEPTION_DATE', 'EXECUTION_DATE', 'SOURCE', 'ELT_LOAD_TYPE', 'ELT_DELETE_IND', 'DML_TYPE']
partitionBy = 'PARTITION_DATE'
partitionBy_value = re.sub(column_regex, '_', execution_date)


@catch_error(logger)
def add_elt_columns(table_to_add, reception_date:str, source:str, is_full_load:bool, dml_type:str=None):
    """
    Add ELT Audit Columns
    """
    table_to_add = table_to_add.withColumn('RECEPTION_DATE', lit(str(reception_date)))
    table_to_add = table_to_add.withColumn('EXECUTION_DATE', lit(str(execution_date)))
    table_to_add = table_to_add.withColumn('SOURCE', lit(str(source)))
    table_to_add = table_to_add.withColumn('ELT_LOAD_TYPE', lit(str("FULL" if is_full_load else "INCREMENTAL")))
    table_to_add = table_to_add.withColumn('ELT_DELETE_IND', lit(0).cast(IntegerType()))

    if is_full_load:
        DML_TYPE = 'I'
    else:
        DML_TYPE = dml_type.upper()

    if DML_TYPE not in ['U', 'I', 'D']:
        raise ValueError(f'DML_TYPE = {DML_TYPE}')

    table_to_add = table_to_add.withColumn('DML_TYPE', lit(str(DML_TYPE)))

    table_to_add = table_to_add.withColumn(partitionBy, lit(str(partitionBy_value)))

    return table_to_add



# %%


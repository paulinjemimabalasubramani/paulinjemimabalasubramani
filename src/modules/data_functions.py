# %% Import Libraries

from datetime import datetime
import re

from .common_functions import make_logging, catch_error
from .config import is_pc

from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F


# %% Logging
logger = make_logging(__name__)


# %% Parameters

strftime = "%Y-%m-%d %H:%M:%S"  # http://strftime.org/
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



# %% Add ETL Temporary Columns

elt_audit_columns = ['RECEPTION_DATE', 'EXECUTION_DATE', 'SOURCE']
partitionBy = 'PARTITION_DATE'


@catch_error(logger)
def add_elt_columns(table_to_add, reception_date:str=None, execution_date:str=None, source:str=None):
    """
    Add ELT Temporary Columns
    """
    if reception_date:
        table_to_add = table_to_add.withColumn('RECEPTION_DATE', lit(str(reception_date)))
    
    if execution_date:
        partition_date = execution_date.replace(' ', '_').replace(':', '-')
        table_to_add = table_to_add.withColumn('EXECUTION_DATE', lit(str(execution_date)))
        table_to_add = table_to_add.withColumn(partitionBy, lit(str(partition_date)))
    
    if source:
        table_to_add = table_to_add.withColumn('SOURCE', lit(str(source)))

    return table_to_add



# %%


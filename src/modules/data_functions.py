# %% Import Libraries

from .common_functions import make_logging, catch_error

from pyspark.sql.functions import col, lit


# %% Logging
logger = make_logging(__name__)


# %% Remove Column Spaces

@catch_error(logger)
def remove_column_spaces(df):
    """
    Removes spaces from column names
    """
    new_df = df.select([col(c).alias(c.replace(' ', '_')) for c in df.columns])
    return new_df



# %% Convert timestamp's or other types to string

@catch_error(logger)
def to_string(df, col_types=['timestamp']):
    """
    Convert timestamp's or other types to string - as it cause errors otherwise.
    """
    for col_name, col_type in df.dtypes:
        if not col_types or col_type in col_types:
            print(f"Converting {col_name} from '{col_type}' to 'string' type")
            df = df.withColumn(col_name, col(col_name).cast('string'))
    
    return df



# %% Add ETL Temporary Columns

@catch_error(logger)
def add_etl_columns(df, reception_date=None, execution_date=None, source:str=None):
    """
    Add ETL Temporary Columns
    """
    if reception_date:
        df = df.withColumn('RECEPTION_DATE', lit(str(reception_date)))
    
    if execution_date:
        df = df.withColumn('EXECUTION_DATE', lit(str(execution_date)))
    
    if source:
        df = df.withColumn('SOURCE', lit(str(source)))

    return df

# %%


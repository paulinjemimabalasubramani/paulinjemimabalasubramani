"""
Upload config CSV files to Azure

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries
import os, sys

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error, is_pc, config_path
from modules.spark_functions import create_spark, read_csv
from modules.data_functions import remove_column_spaces, execution_date, metadata_DataTypeTranslation, metadata_MasterIngestList, \
    metadata_FirmSourceMap, partitionBy, partitionBy_value
from modules.azure_functions import setup_spark_adls_gen2_connection, to_storage_account_name, file_format, save_adls_gen2, \
    tableinfo_container_name


from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType



# %% Logging
logger = make_logging(__name__)



# %% Parameters

storage_account_name = to_storage_account_name()

created_datetime = execution_date
modified_datetime = execution_date

lookup_files_path = os.path.join(config_path, 'lookup_files')

data_type_translation_path = os.path.join(lookup_files_path, 'DataTypeTranslation.csv')
assert os.path.isfile(data_type_translation_path), f"File not found: {data_type_translation_path}"

master_ingest_list_path = os.path.join(lookup_files_path, 'LNR_Tables.csv')
assert os.path.isfile(master_ingest_list_path), f"File not found: {master_ingest_list_path}"

firm_source_map_path = os.path.join(lookup_files_path, 'Firm_Source_Map.csv')
assert os.path.isfile(firm_source_map_path), f"File not found: {firm_source_map_path}"




# %% Create Session

spark = create_spark()


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Add ELT Audit Columns for Config Tables

def add_config_elt_columns(config_table):
    config_table = config_table.withColumn('IsActive', lit(1).cast(IntegerType()))
    config_table = config_table.withColumn('CreatedDateTime', lit(created_datetime).cast(StringType()))
    config_table = config_table.withColumn('ModifiedDateTime', lit(modified_datetime).cast(StringType()))
    config_table = config_table.withColumn(partitionBy, lit(partitionBy_value).cast(StringType()))

    return config_table



# %% Get Master Ingest List

@catch_error(logger)
def get_master_ingest_list_csv(master_ingest_list_path:str, tableinfo_source:str=None):
    """
    Get List of Tables of interest
    """
    master_ingest_list = read_csv(spark, master_ingest_list_path)
    if is_pc: master_ingest_list.printSchema()

    master_ingest_list = remove_column_spaces(master_ingest_list)

    master_ingest_list = add_config_elt_columns(config_table=master_ingest_list)
    master_ingest_list = master_ingest_list.withColumn('IsActive', F.when(F.upper(col('Table_of_Interest'))=='YES', lit(1)).otherwise(lit(0)).cast(IntegerType()))

    column_map = {
        'TableName': 'TABLE_NAME',
        'SchemaName' : 'TABLE_SCHEMA',
    }

    for key, val in column_map.items():
        master_ingest_list = master_ingest_list.withColumnRenamed(key, val)

    save_adls_gen2(
            table_to_save = master_ingest_list,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = tableinfo_source,
            table_name = metadata_MasterIngestList,
            partitionBy = partitionBy,
            file_format = file_format,
        )

    return master_ingest_list



master_ingest_list = get_master_ingest_list_csv(
    master_ingest_list_path = master_ingest_list_path,
    tableinfo_source = 'LR',
    )

if is_pc: master_ingest_list.show(5)



# %% Get DataTypeTranslation table

@catch_error(logger)
def get_translation(data_type_translation_path:str):
    """
    Get DataTypeTranslation table
    """
    translation = read_csv(spark, data_type_translation_path)
    if is_pc: translation.printSchema()

    translation = add_config_elt_columns(config_table=translation)

    save_adls_gen2(
            table_to_save = translation,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = '',
            table_name = metadata_DataTypeTranslation,
            partitionBy = partitionBy,
            file_format = file_format,
        )

    return translation



translation = get_translation(
    data_type_translation_path = data_type_translation_path
    )

if is_pc: translation.show(5)


# %% Get FirmSourceMap


def get_firm_source_map(firm_source_map_path:str):
    """
    Get Firm_Source_Map table
    """

    firm_source_map = read_csv(spark, firm_source_map_path)
    if is_pc: firm_source_map.printSchema()

    firm_source_map = add_config_elt_columns(firm_source_map)

    save_adls_gen2(
            table_to_save = firm_source_map,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = '',
            table_name = metadata_FirmSourceMap,
            partitionBy = partitionBy,
            file_format = file_format,
        )

    return firm_source_map


firm_source_map = get_firm_source_map(
    firm_source_map_path = firm_source_map_path
    )

if is_pc: firm_source_map.show(5)


# %%



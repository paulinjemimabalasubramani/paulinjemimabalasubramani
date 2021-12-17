"""
Common Library for translating data types from source database to target database, and creating metadata.TableInfo table in Azure

"""

# %% Import Libraries

import os, sys, re, tempfile, shutil, copy, json, pymssql
from pprint import pprint
from collections import defaultdict
from datetime import datetime

from .common_functions import logger, catch_error, is_pc, execution_date, data_settings, pymssql_execute_non_query, \
    execution_date_start, EXECUTION_DATE_str, cloud_file_hist_conf
from .azure_functions import read_adls_gen2, \
    default_storage_account_name, file_format, save_adls_gen2, setup_spark_adls_gen2_connection, container_name, \
    default_storage_account_abbr, metadata_folder, azure_container_folder_path, data_folder, add_table_to_tableinfo, \
    storage_account_abbr_to_full_name
from .spark_functions import read_csv, IDKeyIndicator, add_md5_key, read_sql, column_regex, partitionBy, \
    to_string, remove_column_spaces, add_elt_columns, partitionBy_value, \
    write_sql, elt_process_id, ELT_PROCESS_ID_str

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, when, to_timestamp
from pyspark.sql.types import IntegerType, StringType, BooleanType
from pyspark.sql.window import Window



# %% Parameters

to_cloud_file_history_name = lambda tableinfo_source: (sys.domain_abbr + '_' + tableinfo_source + '_file_history3').lower()
to_cloud_row_history_name = lambda tableinfo_source: (sys.domain_abbr + '_' + tableinfo_source + '_row_history3').lower()



# %% Get Key Column Names

@catch_error(logger)
def get_key_column_names():
    """
    Get Key Column Names for sorting the data files for uniqueness
    """
    key_column_names = dict() 
    key_column_names['base'] = ['table_name']
    key_column_names['with_load'] = key_column_names['base'] + ['is_full_load']
    key_column_names['with_load_n_date'] = key_column_names['with_load'] + ['key_datetime']
    return key_column_names



key_column_names = get_key_column_names()


# %% Migrate All Files

@catch_error(logger)
def migrate_all_files(spark, snowflake_connection, fn_extract_file_meta, additional_ingest_columns, fn_process_file):
    """
    Migrate All Files
    """











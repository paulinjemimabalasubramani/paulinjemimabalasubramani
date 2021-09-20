"""
Read Pershing "CUSTOMER ACCOUNT INFORMATION" - ACCT (Update File) / ACCF (Refresh File)

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_and_account'
sys.domain_abbr = 'CA'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import data_settings, get_secrets, logger, mark_execution_end
from modules.spark_functions import create_spark, read_csv
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name




# %% Parameters

ingest_from_files_flag = True

sql_server = ''
sql_key_vault_account = sql_server

storage_account_name = default_storage_account_name

tableinfo_source = 'ACCF'
sql_database = tableinfo_source # TABLE_CATALOG

data_type_translation_id = 'sqlserver_snowflake'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))

# https://stackoverflow.com/questions/41944689/pyspark-parse-fixed-width-text-file


# %% Create Session

spark = create_spark()


# %%







# %% Mark Execution End

mark_execution_end()


# %%







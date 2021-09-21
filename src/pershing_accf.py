"""
Read Pershing "CUSTOMER ACCOUNT INFORMATION" - ACCT (Update File) / ACCF (Refresh File)

https://standardfiles.pershing.com/


Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

__file__ = r'C:\Users\smammadov\packages\EDIP-Code\src\pershing_accf.py'


import os, sys
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_and_account'
sys.domain_abbr = 'CA'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import data_settings, get_secrets, logger, mark_execution_end, config_path
from modules.spark_functions import create_spark, read_csv, read_text
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name


from pyspark.sql.functions import col, lit



# %% Parameters

ingest_from_files_flag = True

sql_server = ''
sql_key_vault_account = sql_server

storage_account_name = default_storage_account_name

tableinfo_source = 'ACCF'
sql_database = tableinfo_source # TABLE_CATALOG

data_type_translation_id = 'sqlserver_snowflake'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
})


# https://stackoverflow.com/questions/41944689/pyspark-parse-fixed-width-text-file


# %% Create Session

spark = create_spark()


# %% Read Schema
schema_folder_path = os.path.join(config_path, 'pershing_schema')
schema_file_path_accf = os.path.join(schema_folder_path, 'customer_account_information_acct_accf.csv')

schema_accf = read_csv(spark=spark, file_path=schema_file_path_accf)


# %%

schema_accf.show(20, False)




# %% Read Text File

file_name = '4CCF.4CCF'

text_file = read_text(spark=spark, file_path=os.path.join(data_path_folder, file_name))

cv = col('value')



bof_pershing = 'BOF      PERSHING '
eof_pershing = 'EOF      PERSHING '

header = text_file.where(cv.substr(1, len(bof_pershing))==lit(bof_pershing))
trailer = text_file.where(cv.substr(1, len(eof_pershing))==lit(eof_pershing))

text_file = text_file.where(
    (cv.substr(1, len(bof_pershing))!=lit(bof_pershing)) &
    (cv.substr(1, len(eof_pershing))!=lit(eof_pershing))
    )

header.show()
trailer.show()
text_file.show()




# %% Mark Execution End

mark_execution_end()


# %%







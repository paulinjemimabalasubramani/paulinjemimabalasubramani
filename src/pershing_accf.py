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

from modules.common_functions import data_settings, get_secrets, logger, mark_execution_end, config_path, is_pc
from modules.spark_functions import create_spark, read_csv, read_text, column_regex
from modules.azure_functions import setup_spark_adls_gen2_connection, read_tableinfo_rows, default_storage_account_name, tableinfo_name

from pprint import pprint

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType



# %% Parameters

storage_account_name = default_storage_account_name
tableinfo_source = 'ACCF'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
})

header_str = 'BOF      PERSHING '
trailer_str = 'EOF      PERSHING '
transaction_code = 'CI'


# https://stackoverflow.com/questions/41944689/pyspark-parse-fixed-width-text-file



# %% Create Session

spark = create_spark()


# %% Read Schema
schema_folder_path = os.path.join(config_path, 'pershing_schema')
schema_file_path_accf = os.path.join(schema_folder_path, 'customer_account_information_acct_accf.csv')

schema = read_csv(spark=spark, file_path=schema_file_path_accf)


# %% Preprocess Schema

schema = schema.withColumn('field_name', F.lower(F.regexp_replace(F.trim(col('field_name')), column_regex, '_')))
schema = schema.withColumn('record_name', F.upper(col('record_name')))
schema = schema.withColumn('conditional_changes', F.upper(col('conditional_changes')))
schema = schema.withColumn('position_start', F.split(col('position'), pattern='-').getItem(0).cast(IntegerType()))
schema = schema.withColumn('position_end', F.split(col('position'), pattern='-').getItem(1).cast(IntegerType()))
schema = schema.withColumn('length', col('position_end') - col('position_start') + lit(1))
schema = schema.where(col('field_name')!='not_used')


if is_pc: schema.show(20, True)



# %% Read Text File

file_name = '4CCF.4CCF'

text_file = read_text(spark=spark, file_path=os.path.join(data_path_folder, file_name))


# %%

tables = dict()

cv = col('value').substr

record_names = schema.select('record_name').distinct().collect()
record_names = [x['record_name'] for x in record_names]
if is_pc: pprint(record_names)

for record_name in record_names:
    if is_pc: pprint(f'Record Name: {record_name}')
    if record_name == 'HEADER':
        table = text_file.where(cv(1, len(header_str))==lit(header_str))
    elif record_name == 'TRAILER':
        table = text_file.where(cv(1, len(trailer_str))==lit(trailer_str))
    else:
        table = text_file.where(
            (cv(1, len(transaction_code))==lit(transaction_code)) &
            (cv(3, 1)==lit(record_name))
            )

    for schema_row in schema.where(col('record_name')==lit(record_name)).rdd.toLocalIterator():
        schema_dict = schema_row.asDict()
        table = table.withColumn(schema_dict['field_name'], cv(schema_dict['position_start'], schema_dict['length']))

    if is_pc: table.show(5)
    tables[record_name] = table







# %% Mark Execution End

mark_execution_end()


# %%







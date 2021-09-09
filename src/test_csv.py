# %% Import Libraries

import os, sys, tempfile, shutil, json, copy
sys.parent_name = os.path.basename('test_csv.py')

from collections import defaultdict
from datetime import datetime
from pprint import pprint


from modules.common_functions import logger, catch_error, is_pc, data_path, config_path, execution_date, strftime, get_secrets, \
    mark_execution_end
from modules.spark_functions import create_spark, read_sql, write_sql, read_csv, read_xml, add_id_key, add_md5_key, \
    IDKeyIndicator, MD5KeyIndicator, get_sql_table_names, remove_column_spaces, add_elt_columns, partitionBy
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, get_firms_with_crd, add_table_to_tableinfo
from modules.build_finra_tables import base_to_schema, build_branch_table, build_individual_table, flatten_df, flatten_n_divide_df


import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, to_date, to_json, to_timestamp, when, row_number



# %% Initiate Spark
spark = create_spark()



# %%

table = read_csv(spark=spark, file_path=r'C:\Shared\LR\oltp_individual.txt')

# %%

table.select('individualid').where(F.length(col('individualid'))<lit(5)).show(5)

# %%
table.select('individualid').where(col('individualid')<lit('')).show(5)


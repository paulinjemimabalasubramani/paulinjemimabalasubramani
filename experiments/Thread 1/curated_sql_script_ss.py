"""
Dynamically create SQL statements for CTASing curated layer tables.

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging, catch_error
# from modules.config import is_pc
from modules.spark_functions import create_spark, read_sql
# from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, read_tableinfo, get_azure_sp
# from modules.data_functions import to_string, remove_column_spaces, add_elt_columns, execution_date, partitionBy


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window



# %% Logging
logger = make_logging(__name__)


# %% Parameters

# server = 'TSQLOLTP01'

# storage_account_name = "agaggrlakescd"
# container_name = "ingress"
# domain_name = 'financial_professional'
# format = 'delta'

# reception_date = execution_date
# source = 'LR'


# %% Create Session

spark = create_spark()

# %%
type(spark)
# %%
curated_tables_masterinfo = spark.read.csv('../config/curated_lr_tables_masterinfo.csv', header=True)
curated_tables_masterinfo = spark.read.csv('../config/curated_lr_tables_masterinfo2.csv', header=True)

# TODO: either make OrdinalPosition/KeyIndicator int to start with or make sure this casting is present
curated_tables_masterinfo = curated_tables_masterinfo.withColumn('OrdinalPosition', col('OrdinalPosition').cast(IntegerType())).withColumn('KeyIndicator', col('IsPrimaryKey').cast(IntegerType()))

print(curated_tables_masterinfo.count(), 'rows')
print(curated_tables_masterinfo.select('TargetTableName').distinct().count(), 'tables')
curated_tables_masterinfo.printSchema()
# curated_tables_masterinfo.show(5)

tables_to_curate = [i.TargetTableName for i in curated_tables_masterinfo.select('TargetTableName').distinct().collect()]
tables_to_curate = [t for t in tables_to_curate if type(t) != type(None)]
tables_to_curate

# %%

for target_tablename in tables_to_curate:
    try:
        print(target_tablename)
        current_masterinfo = curated_tables_masterinfo.where(col('TargetTableName') == target_tablename)
        # current_masterinfo.show()
        target_database, target_schema = [(i.TargetDataBase, i.TargetSchema) for i in current_masterinfo.select('TargetDataBase', 'TargetSchema').distinct().collect()][0]
        source_database, source_schema, source_table = [(i.SourceDatabase, i.SourceSchema, i.SourceTableName) for i in current_masterinfo.select('SourceDatabase', 'SourceSchema', 'SourceTableName').distinct().collect()][0]

        sql = f'CREATE OR REPLACE TABLE {target_database}.{target_schema}.{target_tablename} (\n'

        target_columns = [(i.TargetColumnName, i.TargetDataType, i.KeyIndicator) for i in current_masterinfo.orderBy('OrdinalPosition').select('TargetColumnName', 'TargetDataType', 'KeyIndicator').collect()]

        target_columns_sql = ""
        for tgtcol in target_columns:
            if type(tgtcol[1]) != type(None):
                target_columns_sql += f'\t{tgtcol[0]} {tgtcol[1].upper()}'
            else:
                target_columns_sql += f'\t{tgtcol[0]} N/A'
            if tgtcol[2] == 1:
                target_columns_sql += ' PRIMARY KEY'
            target_columns_sql += ',\n'

        target_columns_sql = target_columns_sql[:-2] + '\n)'
        sql += target_columns_sql + '\nAS\nSELECT\n'

        source_columns = [i.SourceColumnName for i in current_masterinfo.select('SourceColumnName').collect()]

        sql_sourcecolumns = ""
        for srccol in source_columns:
            sql_sourcecolumns += f'\t{srccol},\n'

        sql_sourcecolumns = sql_sourcecolumns[:-2] + '\n'
        sql += sql_sourcecolumns + f'FROM {source_database}.{source_schema}.{source_table}'

        print(sql)
        print()
        print('##########################################')
        print()
    except Exception as e:
        print(target_tablename, 'failed with this error:', e)


# %%

# TODO: add in a check somewhere that there's only one target db/schema pair??
# (target_database, target_schema) = [(i.TargetDataBase, i.TargetSchema) for i in current_masterinfo.select('TargetDataBase', 'TargetSchema').distinct().collect()][0]

# TODO: same with source db/schema/table combo??
# source_database, source_schema, source_table = [(i.SourceDatabase, i.SourceSchema, i.SourceTableName) for i in current_masterinfo.select('SourceDatabase', 'SourceSchema', 'SourceTableName').distinct().collect()][0]

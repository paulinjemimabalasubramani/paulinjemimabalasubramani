"""
Test Spark - SQL connection

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

from modules.common import make_logging, catch_error
from modules.mysession import MySession
from modules.config import get_azure_storage_key_valut


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window



# %% Logging
logger = make_logging(__name__)


# %% Parameters

schema = 'OLTP'
table = 'Individual'
database='LR'
server='TSQLOLTP01'

storage_account_name = "agaggrlakescd"
container_name = "ingress"

data_type = 'data'
firm_name='financial_professional'
container_folder = f"{data_type}/{firm_name}/{database}/{schema}"
print(container_folder)


# %% Main Body
if __name__ == '__main__':
    ss = MySession()

    df = ss.read_sql(schema=schema, table=table, database=database, server=server)

    df.printSchema()
    
    df = ss.to_string(df, col_types = ['timestamp']) # Convert timestamp's to string - as it cause errors otherwise.

    azure_tenant_id, sp_id, sp_pass = get_azure_storage_key_valut(storage_name=storage_account_name)

    ss.save_adls_gen2_oauth2(df=df,
        storage_account_name = storage_account_name,
        azure_tenant_id = azure_tenant_id,
        sp_id = sp_id,
        sp_pass = sp_pass,
        container_name = container_name,
        container_folder = container_folder,
        table = table,
        format = 'delta'
    )

    #ss.spark.stop()
    print('Done')



# %%


"""
Create and Execute (if required) Snowflake DDL Steps and ingest_data for FINRA

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

from modules.common_functions import make_logging
from modules.spark_functions import create_spark
from modules.azure_functions import tableinfo_name, to_storage_account_name, setup_spark_adls_gen2_connection
from modules.data_functions import partitionBy

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit


# %% Logging
logger = make_logging(__name__)


# %% Parameters

tableinfo_source = 'FINRA'


storage_account_name = to_storage_account_name() # Default Storage Account Name
tableinfo_container_name = "tables"
container_name = "ingress" # Default Container Name
file_format = 'delta' # Default File Format



# %% Create Session

spark = create_spark()


# %%
setup_spark_adls_gen2_connection(spark, storage_account_name)


# %%

container_name = tableinfo_container_name
container_folder = tableinfo_source
table_name = tableinfo_name


data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table_name}"


# %%

table_read = (spark.read
    .format(file_format)
    .load(data_path)
    )

# %%


table_read.show(5)



# %%

vacuum = spark.sql(f"VACUUM delta.`{data_path}` RETAIN 240 HOURS")


# %%

maxpartitiondate = spark.sql(f"SELECT MAX(PARTITION_DATE) FROM delta.`{data_path}`").collect()[0][0]


# %%

hist = spark.sql(f"DESCRIBE HISTORY delta.`{data_path}`")
maxversion = hist.select(F.max(col('version'))).collect()[0][0]
userMetadata = hist.where(col('version')==lit(maxversion)).collect()[0]['userMetadata']

print(userMetadata)

if userMetadata:
    print('yes')
else:
    print('No')


# %%



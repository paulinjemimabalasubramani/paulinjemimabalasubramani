
# %% Azure Libraries

import os

from .common_functions import make_logging, catch_error
from .data_functions import remove_column_spaces
from .spark_functions import read_csv
from .config import is_pc

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F

# %% Logging
logger = make_logging(__name__)


# %% Parameters

storage_account_name = "agaggrlakescd"
container_name = "tables"
tableinfo_name = 'metadata.TableInfo'
format = 'delta'


# %% Get Azure Key Vault

@catch_error(logger)
def get_azure_key_vault():
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
    azure_client_id = os.environ.get("AZURE_KV_ID")
    azure_client_secret = os.environ.get("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"

    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    client = SecretClient(vault_endpoint, credential)
    return azure_tenant_id, client



# %% Get Azure Storage Key Valut

@catch_error(logger)
def get_azure_storage_key_vault(storage_account_name:str):
    azure_tenant_id, client = get_azure_key_vault()

    sp_id = client.get_secret(f"qa-{storage_account_name}-id").value
    sp_pass = client.get_secret(f"qa-{storage_account_name}-pass").value
    return azure_tenant_id, sp_id, sp_pass



# %% Set up Spark to ADLS Connection

@catch_error(logger)
def setup_spark_adls_gen2_connection(spark, storage_account_name):
    azure_tenant_id, sp_id, sp_pass = get_azure_storage_key_vault(storage_account_name)

    #spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_pass)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")




# %% Save DF to ADLS Gen 2 using 'Service Principals'

@catch_error(logger)
def save_adls_gen2(
        df,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table:str,
        partitionBy:str=None,
        format:str='delta'):
    """
    Save DF to ADLS Gen 2
    """
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table}"
    print(f"Write {format} -> {data_path}")

    if format == 'text':
        df.coalesce(1).write.save(path=data_path, format=format, mode='overwrite', header='false')
    else:
        df.write.save(path=data_path, format=format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true")

    print(f'Finished Writing {container_folder}/{table}')



# %% Read table from ADLS Gen 2

@catch_error(logger)
def read_adls_gen2(spark,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table:str,
        format:str='delta'):
    """
    Read table from ADLS Gen 2
    """
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table}"

    print(f'Reading -> {data_path}')

    df = (spark.read
        .format(format)
        .load(data_path)
        )
    
    if is_pc: df.printSchema()
    if is_pc: df.show(5)

    return df



# %% Get Master Ingest List

@catch_error(logger)
def get_master_ingest_list_csv(spark, table_list_path:str, created_datetime:str=None, modified_datetime:str=None, save_to_adls:bool=False):
    """
    Get List of Tables of interest
    """
    table_list = read_csv(spark, table_list_path)
    if is_pc: table_list.printSchema()

    table_list = remove_column_spaces(table_list)

    table_list = table_list.withColumn('IsActive', F.when(F.upper(col('Table_of_Interest'))=='YES', lit(1)).otherwise(lit(0)))
    if created_datetime:
        table_list = table_list.withColumn('CreatedDateTime', lit(created_datetime))
    if modified_datetime:
        table_list = table_list.withColumn('ModifiedDateTime', lit(modified_datetime))

    column_map = {
        'TableName': 'TABLE_NAME',
        'SchemaName' : 'TABLE_SCHEMA',
    }

    for key, val in column_map.items():
        table_list = table_list.withColumnRenamed(key, val)

    if save_to_adls: # Save Master Ingest List to ADLS Gen 2 - before filtering
        setup_spark_adls_gen2_connection(spark, storage_account_name)

        partitionBy = 'ModifiedDateTime'
        save_adls_gen2(
                df=table_list,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = '',
                table = 'metadata.MasterIngestList',
                partitionBy = partitionBy,
                format = format,
            )

    table_list = table_list.filter(col('IsActive')==lit(1))

    return table_list



# %% Read metadata.TableInfo

catch_error(logger)
def read_tableinfo(spark):
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    tableinfo = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = container_name,
        container_folder = '',
        table = tableinfo_name,
        format = format
    )

    tableinfo = tableinfo.filter(col('IsActive')==lit(1))
    tableinfo.persist()

    # Create unique list of tables
    table_list = tableinfo.select(
        col('SourceDatabase'),
        col('SourceSchema'),
        col('TableName')
        ).distinct()

    if is_pc: table_list.show(5)

    table_rows = table_list.collect()
    print(f'\nNumber of Tables in {tableinfo_name} is {len(table_rows)}')

    return tableinfo, table_rows



# %%




# %% Azure Libraries

import os

from .common_functions import make_logging, catch_error
from .config import is_pc

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


# %% Logging
logger = make_logging(__name__)


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
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"
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
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"

    print(f'Reading -> {data_path}')

    df = (spark.read
        .format(format)
        .load(data_path)
        )
    
    if is_pc: df.printSchema()
    if is_pc: df.show(5)

    return df
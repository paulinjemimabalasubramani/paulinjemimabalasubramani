
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
def get_azure_key_vault(storage_name:str):
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
    azure_client_id = os.environ.get("AZURE_KV_ID")
    azure_client_secret = os.environ.get("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"

    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    client = SecretClient(vault_endpoint, credential)
    return azure_tenant_id, client



# %% Get Azure Storage Key Valut

@catch_error(logger)
def get_azure_storage_key_vault(storage_name:str):
    azure_tenant_id, client = get_azure_key_vault(storage_name)

    sp_id = client.get_secret(f"qa-{storage_name}-id").value
    sp_pass = client.get_secret(f"qa-{storage_name}-pass").value
    return azure_tenant_id, sp_id, sp_pass


# %% Save DF to ADLS Gen 2 using 'Access Key'

"""
@catch_error(logger)
def save_adls_gen2(spark, 
        df,
        storage_account_name:str,
        storage_account_access_key:str,
        container_name:str,
        container_folder:str,
        table:str,
        partitionBy:str=None,
        format:str='delta'):

    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"
    print(f"Write {format} -> {data_path}")

    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

    df.write.save(path=data_path, format=format, mode='overwrite', partitionBy=partitionBy)
    print(f'Finished Writing {container_folder}/{table}')
"""



# %% Save DF to ADLS Gen 2 using 'Service Principals'

@catch_error(logger)
def save_adls_gen2_sp(spark,
        df,
        storage_account_name:str,
        azure_tenant_id:str,
        sp_id:str,
        sp_pass:str,
        container_name:str,
        container_folder:str,
        table:str,
        partitionBy:str=None,
        format:str='delta',
        truncate_table:bool=None):
    """
    Save DF to ADLS Gen 2 using 'Service Principals'
    """

    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder}/{table}"
    print(f"Write {format} -> {data_path}")

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_pass)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

    df.write.save(path=data_path, format=format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true", truncate = 'true' if truncate_table else 'false')
    print(f'Finished Writing {container_folder}/{table}')



# %%


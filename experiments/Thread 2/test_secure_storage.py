from pyspark.sql.functions import lit, col
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
import shutil
import subprocess
import os
import getpass
from os import listdir
from azure.storage.blob import BlobClient
from io import BytesIO
from pyspark.sql import functions as F
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient




def connect_to_storage(storage_name):
    ##grabbing env variables
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
    #print(azure_tenant_id)
    azure_client_id = os.environ.get("AZURE_KV_ID")
    #print(azure_client_id)
    azure_client_secret = os.environ.get("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"
    #set azure credentials
    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    #set keyvault credentials
    client = SecretClient(vault_endpoint, credential)

    sp_id = client.get_secret(f"qa-{storage_name}-id").value
    sp_pass = client.get_secret(f"qa-{storage_name}-pass").value
    spark.conf.set(f"fs.azure.account.auth.type.{storage_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_name}.dfs.core.windows.net",sp_id )
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_name}.dfs.core.windows.net",sp_pass)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
    print(f"{storage_name} set as storage endpoint")


#grabbing env variables
azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
print(azure_tenant_id)
azure_client_id = os.environ.get("AZURE_KV_ID")
print(azure_client_id)
azure_client_secret = os.environ.get("AZURE_KV_SECRET")


connect_to_storage(agaggrlakescd)
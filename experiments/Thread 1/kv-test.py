#spark.conf.set("fs.azure.account.auth.type." + storage_name + ".dfs.core.windows.net", "OAuth")
#spark.conf.set("fs.azure.account.oauth.provider.type." + storage_name + ".dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenPro>
#spark.conf.set("fs.azure.account.oauth2.client.id." + storage_name + ".dfs.core.windows.net", agg_id)
#spark.conf.set("fs.azure.account.oauth2.client.secret." + storage_name + ".dfs.core.windows.net",agg_pass)
#spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storage_name + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + azure_tenant_id >
#spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")

####Import Parameters

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




##storage function
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



##Set Spark Context
conf = SparkConf()
#conf.set('spark.jars', '/usr/local/spar/resources/jars/spark-mssql-connector_2.11-1.0.0.jar')
spark = SparkSession.builder.config("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem").config("fs.azure.account.key.agaggrlakescd.dfs.core.windows.net","22SMBMDagReIFofaEClqhyHzK/lT7GZLlpSc2COP3DZqwXzfJ56KCWXWFcBWUg4vpyt/R6No2o/2jZBn7o5xcQ==").appName("get-table").getOrCreate()


os.environ["AZURE_TENANT_ID"] = str("c1ef4e97-eeff-48b2-b720-0c8480a08061")
os.environ["AZURE_KV_ID"] = str("d23f5bf8-15a0-4ba7-8ddc-88d131a550e0")
os.environ["AZURE_KV_SECRET"] = str("n.sruiBdlT9xu7-kg4_3rG22cc_5-Jpq43")

#grabbing env variables
azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
print(azure_tenant_id)
azure_client_id = os.environ.get("AZURE_KV_ID")
print(azure_client_id)
azure_client_secret = os.environ.get("AZURE_KV_SECRET")

## set endpoint
vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"

#set azure credentials
credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
#set keyvault credentials
client = SecretClient(vault_endpoint, credential)
client.set_secret("qa-agraalakescd-id","f52f424a-126f-4220-9eba-9ba3b12ad79e")
client.set_secret("qa-agraalakescd-pass","_c4k.93xoy3P.~_0kg-Pkf0h-JVD-EALMC")

client.set_secret("qa-agaggrlakescd-id","38bf93c0-48be-4dec-863a-f3ebd320b93c")
client.set_secret("qa-agaggrlakescd-pass","WeRJhzVsSO-63q.vQF0-7.3nniD-I-M7q6")

client.set_secret("qa-agfsclakescd-id","342c7757-d7b6-4670-9dd8-3043d4bc02b7")
client.set_secret("qa-agaggrlakescd-pass","8lD-hvb--0brCT-~8hO54~f6FsalI1gty7")

client.set_secret("qa-agsailakescd-id","59b41940-9e57-433b-b39f-821d4e465c19")
client.set_secret("qa-agsailakescd-pass","LjNaX4wGl6A0d~6_MZXHxo63RVI44Y~-_S")

client.set_secret("qa-agspflakescd-id","8638f8aa-ea62-4c11-896e-45f59a83aff0")
client.set_secret("qa-agspflakescd-pass","qa~F14Bo5fnumxds1.JnhljO3~PrdLY_YT")

client.set_secret("qa-agtrdlakescd-id","695848ee-cccc-41fb-b328-f4ff39764d66")
client.set_secret("qa-agtrdlakescd-pass","t985-i36~9X3AG1-Ww9I5OP9xP6geS-ou~")

client.set_secret("qa-agwfslakescd-id","a78d98f5-143a-499d-a26b-50cfc6e390f2")
client.set_secret("qa-agwfslakescd-pass","~Qk-AW.y_QcDDCOC6fVSP8B0SCC8105WRU")



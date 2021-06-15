""" 
Library Class for retrieving and storing configuration data

"""

# %% libraries

import os, sys, platform

from .common import make_logging, catch_error

import yaml

# %% Azure Libraries

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


# %% Logging
logger = make_logging(__name__)


# %% App and Environment Info
app_info = f'Running python on {platform.system()}'

print(app_info)
logger.info(app_info)

is_pc = platform.system().lower() == 'windows'

# %% Config Paths

if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    drivers_path = os.path.realpath(os.path.dirname(__file__)+'/../../drivers')
    config_path = os.path.realpath(os.path.dirname(__file__)+'/../../config')
    joinstr = ';' # for extraClassPath

else:
    drivers_path = '/usr/local/spark/resources/fileshare/EDIP-Code/drivers'
    config_path = '/usr/local/spark/resources/fileshare/EDIP-Code/config'
    joinstr = ':' # for extraClassPath

drivers = []
for file in os.listdir(drivers_path):
    if file.endswith('.jar'):
        drivers.append(os.path.join(drivers_path, file))
extraClassPath = joinstr.join(drivers)
print(f'extraClassPath: {extraClassPath}')

secrets_file = os.path.join(config_path, "secrets.yaml")


# %% Main Class
class Config:
    """
    Class for retrieving and storing configuration data
    """
    @catch_error(logger)
    def __init__(self, file_path:str, defaults:dict={}):
        for name, value in defaults.items():
            setattr(self, name, value) # Write defaults

        try:
            with open(file_path, 'r') as f:
                contents = yaml.load(f, Loader=yaml.FullLoader)
        except Exception as e:
            except_str = f'Error File was not read: {file_path}'
            print(except_str)
            logger.error(except_str, exc_info=True)
            return

        for name, value in contents.items():
            setattr(self, name, value) # Overwrite defaults from file



# %% Azure Key Vault

os.environ["AZURE_TENANT_ID"] = str("c1ef4e97-eeff-48b2-b720-0c8480a08061")
os.environ["AZURE_KV_ID"] = str("d23f5bf8-15a0-4ba7-8ddc-88d131a550e0")
os.environ["AZURE_KV_SECRET"] = str("n.sruiBdlT9xu7-kg4_3rG22cc_5-Jpq43")



@catch_error(logger)
def get_azure_key_vault(storage_name:str):
    azure_tenant_id = os.environ.get("AZURE_TENANT_ID")
    azure_client_id = os.environ.get("AZURE_KV_ID")
    azure_client_secret = os.environ.get("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"

    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    client = SecretClient(vault_endpoint, credential)
    return azure_tenant_id, client



@catch_error(logger)
def get_azure_storage_key_valut(storage_name:str):
    azure_tenant_id, client = get_azure_key_vault(storage_name)

    sp_id = client.get_secret(f"qa-{storage_name}-id").value
    sp_pass = client.get_secret(f"qa-{storage_name}-pass").value

    return azure_tenant_id, sp_id, sp_pass


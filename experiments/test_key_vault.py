import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient


environment = os.environ.get(key='ENVIRONMENT')
azure_tenant_id = os.environ.get(key='AZURE_TENANT_ID')
azure_client_id = os.environ.get(key='AZURE_KV_ID')
azure_client_secret = os.environ.get(key='AZURE_KV_SECRET')
vault_endpoint = os.environ.get(key='KEYVAULTURL')


if environment == 'DEV': # fix compatiblity issue with running code in laptop with DEV environment
    environment = 'QA'


credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
client = SecretClient(vault_endpoint, credential, logging_enable=True)


account_name = 'snowflake'
sp_id_name = f'{environment}-{account_name}-id'.lower()
sp_pass_name = f'{environment}-{account_name}-pass'.lower()

sp_id = client.get_secret(sp_id_name).value
sp_pass = client.get_secret(sp_pass_name).value


print(f'{sp_id_name}: {sp_id}')
print(f'{sp_pass_name}: {sp_pass}')


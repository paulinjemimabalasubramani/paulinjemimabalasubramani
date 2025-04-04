import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

# Function to retrieve secrets from Azure Key Vault
def get_secret_from_key_vault(vault_url, secret_name, credential):
    """Fetches a secret from Azure Key Vault"""
    secret_client = SecretClient(vault_url=vault_url, credential=credential)
    return secret_client.get_secret(secret_name).value

# Function to get Azure credentials (Service Principal & Key Vault)
def get_azure_credentials():
    """Retrieves Service Principal credentials and Key Vault details"""
   
    # Fetch from environment variables
    key_vault_name = os.getenv('KEY_VAULT_NAME')
    tenant_id = os.getenv('AZURE_TENANT_ID')
    client_id = os.getenv('AZURE_CLIENT_ID')
    client_secret = os.getenv('AZURE_CLIENT_SECRET')

    # Key Vault URI
    key_vault_uri = f"https://{key_vault_name}.vault.azure.net"

    # Authenticate using Service Principal
    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    return {
        "key_vault_uri": key_vault_uri,
        "credential": credential
    }

# Function to get Storage Account configuration
def get_storage_config(azure_creds):
    """Retrieves Storage Account details from Key Vault and initializes Blob Storage Client"""
   
    # Retrieve Storage Account Name
    storage_account = os.getenv('STORAGE_ACCOUNT')

    # Get Storage Container Name from environment
    container = os.getenv('CONTAINER')

    # Initialize Blob Service Client
    blob_service_client = BlobServiceClient(
        account_url=f"https://{storage_account}.blob.core.windows.net",
        credential=azure_creds["credential"]
    )

    return {
        "storage_account": storage_account,
        "container": container,
        "blob_service_client": blob_service_client,
        "storage_upload_location": f'https://{storage_account}.blob.core.windows.net/{container}/'
    }

import os
import sys
import requests
import snowflake.connector
from Azure_utils import get_secret_from_key_vault

class SnowflakeCustomException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


def get_oauth_snowflake_token(azure_creds):
    # Azure OAuth token request URL
    token_url = f'https://login.microsoftonline.com/' + os.getenv('AZURE_TENANT_ID') +'/oauth2/v2.0/token'
    
    payload = {'grant_type': 'client_credentials',
               'client_id': get_secret_from_key_vault(azure_creds["key_vault_uri"], os.getenv('VAULT_KEY_NAME')+"-client-id", azure_creds["credential"]),
               'client_secret': get_secret_from_key_vault(azure_creds["key_vault_uri"], os.getenv('VAULT_KEY_NAME')+"-client-secret", azure_creds["credential"]),
               'scope':get_secret_from_key_vault(azure_creds["key_vault_uri"], os.getenv('VAULT_KEY_NAME')+"-role-scope", azure_creds["credential"])}
    
    # Request headers
    headers = { 'Content-Type': 'application/x-www-form-urlencoded'}
    
    # Make the request
    response = requests.post(token_url, data=payload, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data['access_token']                   
    else:
        print(f'Failed to retrieve access token: {response.status_code}')
        print(response.text)
        raise SnowflakeCustomException(f'Failed to get snowflake oauth token from azure, token_url : {token_url}, response status Code : {response.status_code}, response details : {response.text}')
        
    return access_token


# Function to get Snowflake connection
def get_snowflake_connection(azure_creds):
    """Establish Snowflake connection using credentials from Key Vault"""
    # Retrieve Snowflake credentials from Key Vault
    #snowflake_user = get_secret_from_key_vault(azure_creds["key_vault_uri"], "snowflake-loader-service-account-login-name", azure_creds["credential"])
    snowflake_user = '2c762c52-3f09-4f2f-9d20-aee14ab57d86'
    try:
        conn = snowflake.connector.connect(
            user=snowflake_user,
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            authenticator='oauth',
            token=get_oauth_snowflake_token(azure_creds),
        )
        return conn
    except Exception as e:
        print(f"Snowflake Connection Error: {e}")
        raise e

def get_processing_info_from_snowflake(azure_creds, source):
    """Fetch all active records from DATA_FEED_FILE_INVENTORY and store them in a dictionary."""
    conn = get_snowflake_connection(azure_creds)
    cursor = conn.cursor()

    try:
        query = """
        SELECT FILE_NAME_PREFIX, FILE_STORAGE_LOCATION_TEXT
        FROM DATA_FEED_FILE_INVENTORY
        WHERE SOURCE_SYS_CODE=%s and ACTIVE_FLG = 'Y'
        """
        cursor.execute(query, (source,))

        print(f"Printing the query processed {query % (source,)}")

        # Store all data in a dictionary
        processing_info = {}
        for row in cursor.fetchall():
            file_name_prefix, file_storage_location_text = row
            if file_name_prefix in processing_info:
                print (f'Found 2 entries for source {source} and file_name_prefix {file_name_prefix} from DATA_FEED_FILE_INVENTORY table')
                raise SnowflakeCustomException(f'Found 2 entries for source {source} and file_name_prefix {file_name_prefix} from DATA_FEED_FILE_INVENTORY table')
            processing_info[(file_name_prefix)] = {
                "file_storage_location": file_storage_location_text,
            }
        return processing_info
    
    except Exception as e:
        print(f"Error fetching processing info: {e}")
        raise e
    
    finally:
        cursor.close()
        conn.close()
        print("Snowflake connection closed.")

"""
Module to handle connections to various resources

"""

# %%

import pyodbc
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from contextlib import contextmanager

from .logger import catch_error, get_env, environment, logger



# %%

@catch_error()
def get_azure_key_vault_client(config):
    """
    Get Azure Key Vault Handler
    """
    client_id = get_env('AZURE_KV_ID')
    client_secret = get_env('AZURE_KV_SECRET')
    vault_url = f'https://{config.key_vault_account}.vault.azure.net/'

    credential = ClientSecretCredential(tenant_id=config.azure_tenant_id, client_id=client_id, client_secret=client_secret)
    client = SecretClient(vault_url=vault_url, credential=credential, logging_enable=True)
    return client



# %%

class Connection:
    """
    Store connection data
    """
    @catch_error()
    def __init__(
            self,
            driver:str,
            server:str,
            database:str,
            username:str = None,
            password:str = None,
            trusted_connection:bool = False,
            key_vault_name:str = None,
            config = None,
            ):
        """
        Initiate single connection
        """
        self.driver = driver
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        self.key_vault_name = key_vault_name
        self.config = config


    @catch_error()
    def property_kv_name(self, kv_suffix:str, spacing:str='_'):
        """
        Key Vault name
        e.g. QA_SAVIYNT_ELT_ID
        """
        ENV = environment.QA if environment.environment<environment.qa else environment.ENVIRONMENT # because we don't have dedicated DEV credentials in Azure KV
        return spacing.join([ENV, self.config.key_vault_prefix, self.key_vault_name, kv_suffix]).upper()


    @catch_error()
    def get_kv_secret(self, name:str):
        """
        Get secret from Azure KV
        """
        if not hasattr(self, '__key_vault_client'):
            self.__key_vault_client = get_azure_key_vault_client(config=self.config)
        return self.__key_vault_client.get_secret(name.upper()).value


    @catch_error()
    def secret_property(self, property_name:str, kv_suffix:str):
        """
        Fetch secret property. To be used inside class property
        Try various sources to get the property
        """
        hidden_property_name = f'__{property_name}'
        property_value = getattr(self, hidden_property_name, None) # check if the property already exists in `connection` object
        if property_value: return property_value

        env_name = self.property_kv_name(kv_suffix=kv_suffix, spacing='_')
        kv_name = self.property_kv_name(kv_suffix=kv_suffix, spacing='-')

        if hasattr(self, 'config'): property_value = getattr(self.config, env_name.lower(), None) # check if the property is available in `config` object
        if not property_value: property_value = get_env(variable_name=env_name.upper(), default=None, raise_error_if_no_value=False) # check if the property is available in environment
        if not property_value: property_value = self.get_kv_secret(kv_name.upper()) # check if the property is available in Key Vault

        if not property_value:
            logger.warning(f'Secret property is not found: {kv_name}')
            return None

        setattr(self, hidden_property_name, property_value)
        return property_value


    @property
    def username(self):
        """
        Fetch username
        """
        return self.secret_property(property_name='username', kv_suffix='ID')


    @username.setter
    def username(self, val:str):
        """
        Set username
        """
        self.__username = val


    @property
    def password(self):
        """
        Fetch password
        """
        return self.secret_property(property_name='password', kv_suffix='PASS')


    @password.setter
    def password(self, val:str):
        """
        Set password
        """
        self.__password = val


    @classmethod
    @catch_error()
    def from_config(cls, config, prefix:str=None):
        """
        Populate connection details from config object
        """
        pre = prefix+'_' if prefix else ''

        return cls(
            driver = getattr(config, f'{pre}driver').strip(),
            server = getattr(config, f'{pre}server').strip(),
            database = getattr(config, f'{pre}database').strip(),
            username = getattr(config, f'{pre}username', '').strip(),
            password = getattr(config, f'{pre}password', ''),
            trusted_connection = getattr(config, f'{pre}trusted_connection', '').lower().strip() in ['yes', 'y', 't', 'true'],
            key_vault_name = getattr(config, f'{pre}key_vault_name', '').strip(),
            config = config,
        )


    @catch_error()
    def conn_str_sql_server(self):
        """
        Generate connection string for SQL Server
        """
        authentication_str = 'Trusted_Connection=yes;' if self.trusted_connection else f'UID={self.username};PWD={self.password}'
        return f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};{authentication_str}'


    @contextmanager
    @catch_error()
    def to_sql_server(self):
        """
        Get SQL Server ODBC connection
        """
        try:
            conn = pyodbc.connect(self.conn_str_sql_server())
            yield conn
        finally:
            conn.close()



# %%



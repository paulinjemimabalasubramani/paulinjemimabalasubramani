"""
Module to handle connections to various resources

"""

# %%

import pyodbc
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from contextlib import contextmanager

from .logger import catch_error, get_env, environment



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
    def get_azure_key_vault_client(self):
        """
        Get Azure Key Vault Handler
        """
        client_id = get_env('AZURE_KV_ID')
        client_secret = get_env('AZURE_KV_SECRET')
        vault_url = f'https://{self.config.key_vault_account}.vault.azure.net/'

        credential = ClientSecretCredential(tenant_id=self.config.azure_tenant_id, client_id=client_id, client_secret=client_secret)
        client = SecretClient(vault_url=vault_url, credential=credential, logging_enable=True)
        return client


    @catch_error()
    def kv_str(self, kv_suffix:str):
        """
        Key Vault String wihout last suffix
        e.g. DEV_SAVIYNT_ELT_ID
        """
        return f'{environment.ENVIRONMENT}_{self.config.key_vault_prefix}_{self.key_vault_name}_{kv_suffix}'.upper()


    @property
    def username(self):
        """
        fetch username
        """
        if self.__username: return self.__username

        kv_name = self.kv_str(kv_suffix='ID')

        self.__username = get_env(variable_name=kv_name.upper(), default=None, raise_error_if_no_value=False)
        if self.__username: return self.__username

        if not hasattr(self, '__key_vault_client'):
            self.__key_vault_client = self.get_azure_key_vault_client()
        self.__username = self.__key_vault_client.get_secret(kv_name.lower()).value
        if self.__username: return self.__username

        return None


    @username.setter
    def username(self, val:str):
        """
        Set username
        """
        self.__username = val


    @property
    def password(self):
        """
        fetch password
        """
        if self.__password: return self.__password

        kv_name = self.kv_str(kv_suffix='PASS')

        self.__password = get_env(variable_name=kv_name.upper(), default=None, raise_error_if_no_value=False)
        if self.__password: return self.__password

        if not hasattr(self, '__key_vault_client'):
            self.__key_vault_client = self.get_azure_key_vault_client()
        self.__password = self.__key_vault_client.get_secret(kv_name.lower()).value
        if self.__password: return self.__password

        return None


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

        username = getattr(config, f'{pre}username', None)
        username = username.strip() if username else username

        return cls(
            driver = getattr(config, f'{pre}driver').strip(),
            server = getattr(config, f'{pre}server').strip(),
            database = getattr(config, f'{pre}database').strip(),
            username = username,
            password = getattr(config, f'{pre}password', None),
            trusted_connection = getattr(config, f'{pre}trusted_connection', '').lower().strip() in ['yes', 'y', 't', 'true'],
            key_vault_name = getattr(config, f'{pre}key_vault_name').strip(),
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



"""
Module to handle connections to various resources

"""

# %%
from dataclasses import dataclass
from .settings import Config


# %%

@dataclass
class Connection:
    """
    Store connection data
    """
    driver:str
    server:str
    database:str
    username:str = None # set to None for trusted connections
    password:str = None

    def is_trusted_connection(self):
        """
        Check if authentication method is trusted connection
        """
        return not self.username or not self.password

    def conn_str_sql_server(self):
        """
        Generate connection string for SQL Server
        """
        authentication_str = 'Trusted_Connection=yes;' if self.is_trusted_connection() else 'UID={username};PWD={password}'
        return f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};{authentication_str}'

    @classmethod
    def from_config(cls, config:Config, prefix:str=None):
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
        )




# %%




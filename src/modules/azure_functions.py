"""
Library for Azure Functions

"""

# %% Import Libraries

import os

from .common_functions import make_logging, catch_error
from .config import is_pc
from .data_functions import partitionBy

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

from pyspark.sql.functions import col, lit


# %% Logging
logger = make_logging(__name__)


# %% firm_name to storage_account_name

@catch_error(logger)
def to_storage_account_name(firm_name:str=None, source_system:str=''):
    """
    Converts firm_name to storage_account_name
    """
    if firm_name and source_system.upper() not in ['LR']:
        account = firm_name
    else:
        account = 'aggr' # Default Aggregate Account
    
    return f"ag{account}lakescd".lower()



# %% Parameters

storage_account_name = to_storage_account_name() # Default Storage Account Name
tableinfo_container_name = "tables"
container_name = "ingress" # Default Container Name
tableinfo_name = 'metadata.TableInfo'
file_format = 'delta' # Default File Format



# %% Select TableInfo Columns

@catch_error(logger)
def select_tableinfo_columns(tableinfo):
    column_names = [
        'SourceDatabase',
        'SourceSchema',
        'TableName',
        'SourceColumnName',
        'SourceDataType',
        'SourceDataLength',
        'SourceDataPrecision',
        'SourceDataScale',
        'OrdinalPosition',
        'CleanType',
        'TargetColumnName',
        'TargetDataType',
        'IsNullable',
        'KeyIndicator',
        'IsActive',
        'CreatedDateTime',
        'ModifiedDateTime',
        partitionBy,
        ]

    column_orderby = [
        'SourceDatabase',
        'SourceSchema',
        'TableName',
    ]

    selected_tableinfo = tableinfo.select(*column_names).orderBy(*column_orderby)

    if is_pc: selected_tableinfo.printSchema()
    return selected_tableinfo



# %% Get Environment Variable

@catch_error(logger)
def get_env(variable_name:str):
    value = os.environ.get(variable_name)
    if not value:
        raise ValueError(f'Environment variable does not exist: {variable_name}')
    return value



# %% Get Azure Key Vault

@catch_error(logger)
def get_azure_key_vault():
    azure_tenant_id = get_env("AZURE_TENANT_ID")
    azure_client_id = get_env("AZURE_KV_ID")
    azure_client_secret = get_env("AZURE_KV_SECRET")
    vault_endpoint = "https://ag-kv-west2-secondary.vault.azure.net/"

    credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
    client = SecretClient(vault_endpoint, credential)
    return azure_tenant_id, client



# %% Get Azure Service Principals

@catch_error(logger)
def get_azure_sp(storage_account_name:str):
    azure_tenant_id, client = get_azure_key_vault()

    sp_id = client.get_secret(f"qa-{storage_account_name}-id").value
    sp_pass = client.get_secret(f"qa-{storage_account_name}-pass").value
    return azure_tenant_id, sp_id, sp_pass



# %% Set up Spark to ADLS Connection

@catch_error(logger)
def setup_spark_adls_gen2_connection(spark, storage_account_name):
    azure_tenant_id, sp_id, sp_pass = get_azure_sp(storage_account_name)

    #spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", sp_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", sp_pass)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")



# %% Save table_to_save to ADLS Gen 2 using 'Service Principals'

@catch_error(logger)
def save_adls_gen2(
        table_to_save,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table:str,
        partitionBy:str=None,
        file_format:str=file_format):
    """
    Save table_to_save to ADLS Gen 2
    """
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table}"
    print(f"Write {file_format} -> {data_path}")

    if file_format == 'text':
        table_to_save.coalesce(1).write.save(path=data_path, format=file_format, mode='overwrite', header='false')
    elif file_format == 'json':
        table_to_save.coalesce(1).write.json(path=data_path, mode='overwrite')
    elif file_format == 'csv':
        table_to_save.coalesce(1).write.csv(path=data_path, header='true', mode='overwrite')
    else:
        table_to_save.write.save(path=data_path, format=file_format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true")

    print(f'Finished Writing {container_folder}/{table}')



# %% Read table from ADLS Gen 2

@catch_error(logger)
def read_adls_gen2(spark,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table:str,
        file_format:str=file_format):
    """
    Read table from ADLS Gen 2
    """
    data_path = f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table}"

    print(f'Reading -> {data_path}')

    table_read = (spark.read
        .format(file_format)
        .load(data_path)
        )
    
    if is_pc: table_read.printSchema()
    if is_pc: table_read.show(5)

    return table_read



# %% Read metadata.TableInfo

catch_error(logger)
def read_tableinfo(spark, tableinfo_name:str, tableinfo_source:str):
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    tableinfo = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = tableinfo_source,
        table = tableinfo_name,
        file_format = file_format
    )

    tableinfo = tableinfo.filter(col('IsActive')==lit(1))
    tableinfo.persist()

    # Create unique list of tables
    table_list = tableinfo.select(
        col('SourceDatabase'),
        col('SourceSchema'),
        col('TableName')
        ).distinct()

    if is_pc: table_list.show(5)

    table_rows = table_list.collect()
    print(f'\nNumber of Tables in {tableinfo_source}/{tableinfo_name} is {len(table_rows)}')

    return tableinfo, table_rows



# %%



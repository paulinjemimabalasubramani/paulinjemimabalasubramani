"""
Library for Azure Functions

"""

# %% Import Libraries

from collections import defaultdict
import os, json, re

from .common_functions import make_logging, catch_error
from .config import is_pc
from .data_functions import partitionBy, metadata_FirmSourceMap, elt_audit_columns, column_regex, execution_date, partitionBy_value
from .spark_functions import IDKeyIndicator, MD5KeyIndicator

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

from pyspark.sql import functions as F
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
        'StorageAccount',
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

    selected_tableinfo = tableinfo.select(*column_names).distinct().orderBy(*column_orderby)

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



# %% Azure Data Path Creator

@catch_error(logger)
def azure_data_path_create(container_name:str, storage_account_name:str, container_folder:str, table_name:str):
    return f"abfs://{container_name}@{storage_account_name}.dfs.core.windows.net/{container_folder+'/' if container_folder else ''}{table_name}"



# %% Save table_to_save to ADLS Gen 2 using 'Service Principals'

@catch_error(logger)
def save_adls_gen2(
        table_to_save,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table_name:str,
        partitionBy:str=None,
        file_format:str=file_format):
    """
    Save table_to_save to ADLS Gen 2
    """
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)
    print(f"Write {file_format} -> {data_path}")

    if file_format == 'text':
        table_to_save.coalesce(1).write.save(path=data_path, format=file_format, mode='overwrite', header='false')
    elif file_format == 'json':
        table_to_save.coalesce(1).write.json(path=data_path, mode='overwrite')
    elif file_format == 'csv':
        table_to_save.coalesce(1).write.csv(path=data_path, header='true', mode='overwrite')
    elif file_format == 'delta':
        # spark.sql(f"VACUUM delta.`{data_path}` RETAIN 240 HOURS")
        partitionBy_value = table_to_save.select(F.max(col(partitionBy))).collect()[0][0]
        if partitionBy_value:
            userMetadata = f'{partitionBy}={partitionBy_value}'
        else:
            userMetadata = None
        table_to_save.write.save(path=data_path, format=file_format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true", userMetadata=userMetadata)
    else:
        table_to_save.write.save(path=data_path, format=file_format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true")

    print(f'Finished Writing {container_folder}/{table_name}')




# %% Get partition string for a Table

@catch_error(logger)
def get_partition(spark, domain_name:str, source_system:str, schema_name:str, table_name:str, storage_account_name:str):
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    data_type = 'data'
    container_folder = f"{data_type}/{domain_name}/{source_system}/{schema_name}"
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)
    print(f'Reading partition data for {data_path}')

    hist = spark.sql(f"DESCRIBE HISTORY delta.`{data_path}`")
    maxversion = hist.select(F.max(col('version'))).collect()[0][0]
    userMetadata = hist.where(col('version')==lit(maxversion)).collect()[0]['userMetadata']

    if userMetadata and ('=' in userMetadata):
        print(f'Taking userMetadata {userMetadata}')
        return userMetadata
    else:
        partitionBy_value = spark.sql(f"SELECT MAX({partitionBy}) FROM delta.`{data_path}`").collect()[0][0]
        if not partitionBy_value:
            print(f'{data_path} is EMPTY -> SKIPPING')
            return

        PARTITION = f'{partitionBy}={partitionBy_value}'
        print(f'No userMetadata found, using MAX({partitionBy}): {partitionBy_value}')
        return PARTITION




# %% Read table from ADLS Gen 2

@catch_error(logger)
def read_adls_gen2(spark,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table_name:str,
        file_format:str=file_format):
    """
    Read table from ADLS Gen 2
    """
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)

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
        table_name = tableinfo_name,
        file_format = file_format
    )

    tableinfo = tableinfo.filter(col('IsActive')==lit(1)).distinct()
    tableinfo.persist()

    # Create unique list of tables
    table_list = tableinfo.select(
        col('SourceDatabase'),
        col('SourceSchema'),
        col('TableName'),
        col('StorageAccount'),
        ).distinct()

    if is_pc: table_list.show(5)

    table_rows = table_list.collect()
    print(f'\nNumber of Tables in {tableinfo_source}/{tableinfo_name} is {len(table_rows)}')

    return tableinfo, table_rows



# %% Get Firms with CRD Number

@catch_error(logger)
def get_firms_with_crd(spark, tableinfo_source):
    storage_account_name = to_storage_account_name()
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    firms_table = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = '',
        table_name = metadata_FirmSourceMap,
        file_format = file_format
    )

    firms_table = firms_table.filter(
        (col('Source') == lit(tableinfo_source.upper()).cast("string")) & 
        (col('IsActive') == lit(1))
    )

    firms_table = firms_table.select('Firm', 'SourceKey', 'StorageAccount') \
        .withColumnRenamed('Firm', 'firm_name') \
        .withColumnRenamed('SourceKey', 'crd_number') \
        .withColumnRenamed('StorageAccount', 'storage_account_name')

    firms = firms_table.toJSON().map(lambda j: json.loads(j)).collect()

    assert firms, 'No Firms Found!'
    return firms




# %% Add Table to tableinfo

@catch_error(logger)
def add_table_to_tableinfo(tableinfo:defaultdict, table, firm_name:str, table_name:str, tableinfo_source:str, storage_account_name:str):
    for ix, (col_name, col_type) in enumerate(table.dtypes):
        if col_name in elt_audit_columns or col_name == partitionBy:
            continue

        var_col_type = 'variant' if ':' in col_type else col_type

        tableinfo['SourceDatabase'].append(tableinfo_source)
        tableinfo['SourceSchema'].append(firm_name)
        tableinfo['TableName'].append(table_name)
        tableinfo['SourceColumnName'].append(col_name)
        tableinfo['SourceDataType'].append(var_col_type)
        tableinfo['SourceDataLength'].append(0)
        tableinfo['SourceDataPrecision'].append(0)
        tableinfo['SourceDataScale'].append(0)
        tableinfo['OrdinalPosition'].append(ix+1)
        tableinfo['CleanType'].append(var_col_type)
        tableinfo['StorageAccount'].append(storage_account_name)
        tableinfo['TargetColumnName'].append(re.sub(column_regex, '_', col_name.strip()))
        tableinfo['TargetDataType'].append(var_col_type)
        tableinfo['IsNullable'].append(0 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 1)
        tableinfo['KeyIndicator'].append(1 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 0)
        tableinfo['IsActive'].append(1)
        tableinfo['CreatedDateTime'].append(execution_date)
        tableinfo['ModifiedDateTime'].append(execution_date)
        tableinfo[partitionBy].append(partitionBy_value)




# %%



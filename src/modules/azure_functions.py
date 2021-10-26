"""
Library for Azure Functions

"""

# %% Import Libraries

import re, sys
from collections import defaultdict

from .common_functions import logger, catch_error, is_pc, execution_date, get_secrets, post_log_data, data_settings
from .spark_functions import IDKeyIndicator, MD5KeyIndicator, partitionBy, metadata_FirmSourceMap, elt_audit_columns, \
    column_regex, partitionBy_value, table_to_list_dict

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from pyspark import StorageLevel


from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential



# %% Parameters

azure_filesystem_uri = 'dfs.core.windows.net'

tableinfo_container_name = "tables"
container_name = "ingress" # Default Container Name
tableinfo_name = 'TableInfo'
metadata_folder = 'metadata'
data_folder = 'data'

file_format = 'delta' # Default File Format
default_storage_account_abbr = data_settings.azure_storage_accounts_default_mid.upper()



# %% firm_name to storage_account_name

@catch_error(logger)
def to_storage_account_name(firm_name:str=None):
    """
    Converts firm_name to storage_account_name
    """
    if firm_name:
        account = firm_name
    else:
        account = default_storage_account_abbr # Default Aggregate Account

    return f"{data_settings.azure_storage_accounts_prefix}{account}{data_settings.azure_storage_accounts_suffix}".lower()



default_storage_account_name = to_storage_account_name() # Default Storage Account Name



# %% Select TableInfo Columns

@catch_error(logger)
def select_tableinfo_columns(tableinfo):
    """
    Selects the right columns and orders the rows for the TableInfo -> ready to write to Azure
    """
    tableinfo = (tableinfo
        .withColumn('SourceSchema', F.lower(col('SourceSchema')))
        .withColumn('TableName', F.lower(col('TableName')))
        .withColumn('SourceDatabase', F.upper(col('SourceDatabase')))
        )

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
        'StorageAccountAbbr',
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
        'OrdinalPosition',
    ]

    selected_tableinfo = tableinfo.select(*column_names).distinct().orderBy(*column_orderby)

    if is_pc: selected_tableinfo.show(5)
    return selected_tableinfo




# %% Set up Spark to ADLS Connection

@catch_error(logger)
def setup_spark_adls_gen2_connection(spark, storage_account_name):
    """
    Set up Spark to ADLS Gen 2 Connection using Service Principals
    """
    azure_tenant_id, sp_id, sp_pass = get_secrets(storage_account_name)

    #spark.conf.set(f"fs.azure.account.key.{storage_account_name}.{azure_filesystem_uri}", storage_account_access_key)

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.{azure_filesystem_uri}", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.{azure_filesystem_uri}",  "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.{azure_filesystem_uri}", sp_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.{azure_filesystem_uri}", sp_pass)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.{azure_filesystem_uri}", f"https://login.microsoftonline.com/{azure_tenant_id}/oauth2/token")
    spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")



# %% Get ADLS Gen 2 Service Client

@catch_error(logger)
def get_adls_gen2_service_client(storage_account_name):
    """
    Get ADLS Gen 2 Service Client Handler
    """
    azure_tenant_id, sp_id, sp_pass = get_secrets(storage_account_name)

    credential = ClientSecretCredential(tenant_id=azure_tenant_id, client_id=sp_id, client_secret=sp_pass)
    service_client = DataLakeServiceClient(account_url=f'https://{storage_account_name}.{azure_filesystem_uri}', credential=credential)
    return service_client



# %% Construct path for Azure Tables

@catch_error(logger)
def azure_data_path_create(container_name:str, storage_account_name:str, container_folder:str, table_name:str):
    """
    Construct path for Azure Tables
    """
    return f"abfs://{container_name}@{storage_account_name}.{azure_filesystem_uri}/{container_folder+'/' if container_folder else ''}{table_name}"



# %% Construct container folder path for Azure Tables

@catch_error(logger)
def azure_container_folder_path(data_type:str, domain_name:str='', source_or_database:str='', firm_or_schema:str=''):
    """
    Construct container folder path for Azure Tables
    """
    sl = lambda x: '/'+x if x else ''
    return f"{data_type}{sl(domain_name)}{sl(source_or_database)}{sl(firm_or_schema)}"



# %% Save table to ADLS Gen 2 using 'Service Principals'

@catch_error(logger)
def save_adls_gen2(
        table,
        storage_account_name:str,
        container_name:str,
        container_folder:str,
        table_name:str,
        partitionBy:str=None,
        file_format:str=file_format):
    """
    Save table to Azure ADLS Gen 2
    """
    file_format = file_format.lower()
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)
    logger.info(f"Write {file_format} -> {data_path}")

    userMetadata = None
    if file_format == 'text':
        table.coalesce(1).write.save(path=data_path, format=file_format, mode='overwrite', header='false')
    elif file_format == 'json':
        table.coalesce(1).write.json(path=data_path, mode='overwrite')
    elif file_format == 'csv':
        table.coalesce(1).write.csv(path=data_path, header='true', mode='overwrite')
    elif file_format == 'delta':
        #partitionBy_value = table.select(F.max(col(partitionBy))).collect()[0][0]
        if partitionBy_value:
            userMetadata = f'{partitionBy}={partitionBy_value}'
        table.write.save(path=data_path, format=file_format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true", userMetadata=userMetadata)
    else:
        table.write.save(path=data_path, format=file_format, mode='overwrite', partitionBy=partitionBy, overwriteSchema="true")

    log_data = {
        "Storage_Account": storage_account_name,
        "Container": container_name,
        "Folder": container_folder,
        "Table": table_name,
        "Partitioning": partitionBy,
        "Format": file_format,
        "Row_Count": table.count(),
        "Number_of_Columns": len(table.columns),
        "Table_Size": sys.getsizeof(table),
        }

    post_log_data(log_data=log_data, log_type='AirlfowSavedTables', logger=logger)

    logger.info(f'Finished Writing {container_folder}/{table_name}')
    return userMetadata




# %% Get partition string for a Delta Table

@catch_error(logger)
def get_partition(spark, domain_name:str, source_system:str, schema_name:str, table_name:str, storage_account_name:str, PARTITION_list=None):
    """
    Get partition string for a Delta Table. The partition string should be same as partition by folder name for Delta Tables
    """
    data_type = data_folder
    container_folder = f"{data_type}/{domain_name}/{source_system}/{schema_name}"
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)
    logger.info(f'Reading partition data for {data_path}')

    if PARTITION_list:
        PARTITION = PARTITION_list[(domain_name, source_system, schema_name, table_name, storage_account_name)]
        if not PARTITION:
            logger.warning(f'{data_path} is EMPTY -> SKIPPING')
        return PARTITION

    logger.warning('No Partition List, taking partition info from Azure...')
    setup_spark_adls_gen2_connection(spark, storage_account_name)
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{data_path}`")
    maxversion = hist.select(F.max(col('version'))).collect()[0][0]
    userMetadata = hist.where(col('version')==lit(maxversion)).collect()[0]['userMetadata']

    if userMetadata and ('=' in userMetadata):
        logger.info(f'Taking userMetadata {userMetadata}')
        return userMetadata
    else:
        partitionBy_value = spark.sql(f"SELECT MAX({partitionBy}) FROM delta.`{data_path}`").collect()[0][0]
        if not partitionBy_value:
            logger.warning(f'{data_path} is EMPTY -> SKIPPING')
            return

        PARTITION = f'{partitionBy}={partitionBy_value}'
        logger.warning(f'No userMetadata found, using MAX({partitionBy}): {partitionBy_value}')
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
    Read table from Azure ADLS Gen 2
    """
    data_path = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder=container_folder, table_name=table_name)

    logger.info(f'Reading -> {data_path}')

    table = (spark.read
        .format(file_format)
        .load(data_path)
        )

    if is_pc: table.show(5)

    table.persist(StorageLevel.MEMORY_AND_DISK)
    return table



# %% Read metadata.TableInfo

catch_error(logger)
def read_tableinfo_rows(tableinfo_name:str, tableinfo_source:str, tableinfo):
    """
    Convert tableinfo table metadata to list
    """
    if not tableinfo:
        logger.warning('No TableInfo to read -> skipping')
        return

    tableinfo = tableinfo.filter(col('IsActive')==lit(1)).distinct()
    tableinfo.persist(StorageLevel.MEMORY_AND_DISK)

    # Create unique list of tables
    table_list = tableinfo.select(
        col('SourceDatabase'),
        col('SourceSchema'),
        col('TableName'),
        col('StorageAccount'),
        col('StorageAccountAbbr'),
        ).distinct()

    if is_pc: table_list.show(5)

    table_rows = table_list.collect()
    logger.info(f'Number of Tables in {tableinfo_source}/{tableinfo_name} is {len(table_rows)}')

    logger.info('Check if there is a table with no primary key')
    nopk = tableinfo.groupBy(['SourceDatabase', 'SourceSchema', 'TableName']).agg(F.sum('KeyIndicator').alias('key_count')).where(F.col('key_count')==F.lit(0))
    assert nopk.rdd.isEmpty(), f'Found tables with no primary keys: {nopk.collect()}'

    return table_rows



# %% Get Firms with CRD Number

@catch_error(logger)
def get_firms_with_crd(spark, tableinfo_source):
    """
    Get Firms with CRD Number from Azure
    """
    storage_account_name = default_storage_account_name
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    firms_table = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = metadata_folder,
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
        .withColumnRenamed('StorageAccount', 'storage_account_abbr')

    firms_table = firms_table.withColumn('firm_name', F.lower(col('firm_name')))

    firms = table_to_list_dict(firms_table)

    assert firms, 'No Firms Found!'
    return firms




# %% Add Table to tableinfo

@catch_error(logger)
def add_table_to_tableinfo(tableinfo:defaultdict, table, schema_name:str, table_name:str, tableinfo_source:str, storage_account_name:str, storage_account_abbr:str):
    """
    Add Table Metadata to tableinfo
    """
    for ix, (col_name, col_type) in enumerate(table.dtypes):
        if col_name in elt_audit_columns or col_name == partitionBy:
            continue

        var_col_type = 'variant' if ':' in col_type else col_type

        tableinfo['SourceDatabase'].append(tableinfo_source)
        tableinfo['SourceSchema'].append(schema_name)
        tableinfo['TableName'].append(table_name)
        tableinfo['SourceColumnName'].append(col_name)
        tableinfo['SourceDataType'].append(var_col_type)
        tableinfo['SourceDataLength'].append(0)
        tableinfo['SourceDataPrecision'].append(0)
        tableinfo['SourceDataScale'].append(0)
        tableinfo['OrdinalPosition'].append(ix+1)
        tableinfo['CleanType'].append(var_col_type)
        tableinfo['StorageAccount'].append(storage_account_name)
        tableinfo['StorageAccountAbbr'].append(storage_account_abbr)
        tableinfo['TargetColumnName'].append(re.sub(column_regex, '_', col_name.strip()))
        tableinfo['TargetDataType'].append(var_col_type)
        tableinfo['IsNullable'].append(0 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 1)
        tableinfo['KeyIndicator'].append(1 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 0)
        tableinfo['IsActive'].append(1)
        tableinfo['CreatedDateTime'].append(execution_date)
        tableinfo['ModifiedDateTime'].append(execution_date)
        tableinfo[partitionBy].append(partitionBy_value)



# %%



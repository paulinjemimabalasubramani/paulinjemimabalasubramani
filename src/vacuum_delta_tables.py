"""
Vacuum all delta tables in ADLS Gen 2

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys
sys.parent_name = os.path.basename(__file__)


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import logger, mark_execution_end, get_secrets, catch_error
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, default_storage_account_abbr, get_firms_with_crd, \
    to_storage_account_name, azure_data_path_create


from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient



# %% Parameters

days_to_retain = 7 # How many days to retain for vacuuming

firms_source = 'FINRA'



# %% Create Session

spark = create_spark()



# %% Get Container Names

@catch_error(logger)
def get_container_names(storage_account_name:str):
    azure_tenant_id, sp_id, sp_pass = get_secrets(storage_account_name)

    credential = ClientSecretCredential(tenant_id = azure_tenant_id, client_id = sp_id, client_secret = sp_pass)
    service_client = DataLakeServiceClient(account_url=f'https://{storage_account_name}.dfs.core.windows.net', credential=credential)

    file_systems = service_client.list_file_systems()
    container_names = [file_system.name for file_system in file_systems]
    logger.info({
        'storage_account_name': storage_account_name,
        'container_names': container_names,
        })
    return container_names



# %% Get ADLS Gen 2 paths

@catch_error(logger)
def get_adls_gen2_paths(storage_account_name:str, container_name:str):
    account_url = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder='', table_name='')
    logger.info(f'Getting paths from {account_url}')

    azure_tenant_id, sp_id, sp_pass = get_secrets(storage_account_name)

    credential = ClientSecretCredential(tenant_id = azure_tenant_id, client_id = sp_id, client_secret = sp_pass)
    service_client = DataLakeServiceClient(account_url=f'https://{storage_account_name}.dfs.core.windows.net', credential=credential)

    file_system_client = service_client.get_file_system_client(file_system=container_name)
    paths = file_system_client.get_paths(path=None, recursive=True, max_results=None)
    return paths



# %% Get Delta Table paths

@catch_error(logger)
def get_delta_table_paths(storage_account_name:str, container_name:str):
    delta_log_folder = '/_delta_log'
    rm_suffix = len(delta_log_folder)

    paths = get_adls_gen2_paths(storage_account_name=storage_account_name, container_name=container_name)
    delta_paths = [path.name[:-rm_suffix] for path in paths if path.name.lower().endswith(delta_log_folder)]
    return delta_paths




# %% Vacuum All Delta Tables in a Container

@catch_error(logger)
def vacuum_container(spark, storage_account_name:str, container_name:str, days_to_retain:int=7):
    logger.info(f'Start Vacuuming {container_name}@{storage_account_name}')
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    delta_paths = get_delta_table_paths(storage_account_name=storage_account_name, container_name=container_name)

    for delta_path in delta_paths:
        delta_path_full = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder='', table_name=delta_path)
        logger.info(f'Vacuuming {delta_path_full}')

        try:
            result = spark.sql(f"VACUUM delta.`{delta_path_full}` RETAIN {days_to_retain * 24} HOURS")
        except Exception as e:
            logger.error(str(e))

    logger.info(f'End Vacuuming {container_name}@{storage_account_name}')




# %% Vacuum All Delta Tables in a Storage Account

@catch_error(logger)
def vacuum_storage_account(spark, storage_account_name:str, days_to_retain:int=7):
    logger.info(f'Vacuuming {storage_account_name}')
    container_names = get_container_names(storage_account_name=storage_account_name)
    for container_name in container_names:
        vacuum_container(spark, storage_account_name=storage_account_name, container_name=container_name, days_to_retain=days_to_retain)



# %% Get All Storage Accounts

@catch_error(logger)
def get_all_storage_accounts(spark, firms_source:str, include_default:bool=True):
    firms = get_firms_with_crd(spark=spark, tableinfo_source=firms_source)

    storage_account_abbrs = {x['storage_account_abbr'] for x in firms}
    if include_default: storage_account_abbrs.add(default_storage_account_abbr)
    storage_account_abbrs = sorted(list(storage_account_abbrs))
    storage_account_names = [to_storage_account_name(firm_name=storage_account_abbr) for storage_account_abbr in storage_account_abbrs]

    logger.info({'storage_account_names': storage_account_names})
    return storage_account_names



# %% Vacuum Delata Tables in All Storage Accounts

@catch_error(logger)
def vacuum_all_storage_accounts(spark, firms_source:str, days_to_retain:int=7):
    storage_account_names = get_all_storage_accounts(spark=spark, firms_source=firms_source, include_default=True)

    for storage_account_name in storage_account_names:
        vacuum_storage_account(spark=spark, storage_account_name=storage_account_name, days_to_retain=days_to_retain)



vacuum_all_storage_accounts(spark=spark, firms_source=firms_source, days_to_retain=days_to_retain)



# %% Mark Execution End

mark_execution_end()


# %%


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

from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import is_pc, logger, mark_execution_end, get_secrets, catch_error
from modules.spark_functions import create_spark
from modules.azure_functions import setup_spark_adls_gen2_connection, default_storage_account_abbr, get_firms_with_crd, \
    to_storage_account_name, azure_data_path_create, container_name


from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient



# %% Parameters

firms_source = 'FINRA'



# %% Create Session

spark = create_spark()



# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=firms_source)

if is_pc: pprint(firms)



# %%

storage_account_abbrs = {x['storage_account_abbr'] for x in firms}
storage_account_abbrs.add(default_storage_account_abbr)
storage_account_abbrs = sorted(list(storage_account_abbrs))


pprint(storage_account_abbrs)


for storage_account_abbr in storage_account_abbrs:
    pass

storage_account_abbr = storage_account_abbrs[1]
storage_account_name = to_storage_account_name(firm_name=storage_account_abbr)
pprint(f'Vacuuming {storage_account_name}')



# %% Get ADLS Gen 2 paths

@catch_error(logger)
def get_adls_gen2_paths(spark, storage_account_name, container_name):
    account_url = azure_data_path_create(container_name=container_name, storage_account_name=storage_account_name, container_folder='', table_name='')
    pprint(f'Getting paths from {account_url}')

    setup_spark_adls_gen2_connection(spark, storage_account_name)
    azure_tenant_id, sp_id, sp_pass = get_secrets(storage_account_name)

    credential = ClientSecretCredential(tenant_id = azure_tenant_id, client_id = sp_id, client_secret = sp_pass)
    service_client = DataLakeServiceClient(account_url=f'https://{storage_account_name}.dfs.core.windows.net', credential=credential)

    file_system_client = service_client.get_file_system_client(file_system=container_name)
    paths = file_system_client.get_paths(path=None, recursive=True, max_results=None, timeout=120)
    return paths



paths = get_adls_gen2_paths(spark=spark, storage_account_name=storage_account_name, container_name=container_name)


# %%

delta_log_folder = '/_delta_log'

delta_paths = [path.name for path in paths if path.name.lower().endswith(delta_log_folder)]



for path in delta_paths:
    print(path + '\n')





# %% Mark Execution End

mark_execution_end()


# %%


__description__ = """
Load data to SQL Server using bcp tool

bcp <databse_name>.<schema_name>.<table_name> in "<file_path>" -S <server_name>.<dns_suffix> -U <username> -P <password> -c -t "|" -F 2

bcp SaviyntIntegration.dbo.envestnet_hierarchy_firm in "C:/myworkdir/data/envestnet_v35_processed/hierarchy_firm_20231009.txt" -S DW1SQLDATA01.ibddomain.net -T -c -t "|" -F 2

"""


# %% Start Logging

import os
from saviynt_modules.logger import catch_error, environment, logger
logger.set_logger(app_name=os.path.basename(__file__))



# %% Parse Arguments

if environment.is_prod:
    import argparse

    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--pipeline_key', '--pk', help='pipeline_key value for getting pipeline settings', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipeline_key': 'saviynt_envestnet',
        }



# %% Import Libraries

from saviynt_modules.logger import logger, catch_error
from saviynt_modules.connections import Connection
from saviynt_modules.migration import recursive_migrate_all_files, migrate_csv_file_to_sql_server, get_config



# %% Parameters

args |=  {
    }



# %% Get Config

config = get_config(args=args)



# %%

from saviynt_modules.migration import migrate_file_to_sql_table
from saviynt_modules.filemeta import get_file_meta_csv
from saviynt_modules.sqlserver import create_or_truncate_sql_table, execute_sql_queries, get_elt_values_sql



# %%


file_path = r'C:\myworkdir\data\envestnet_v35_processed\codes_account_status_20231009.txt'
zip_file_path = None

file_meta = get_file_meta_csv(file_path=file_path, config=config, zip_file_path=zip_file_path)



# %%






# %%

recursive_migrate_all_files(source_path=config.source_path, config=config, fn_migrate_file=migrate_csv_file_to_sql_server)



# %% Close Connections / End Program

logger.mark_run_end()



# %%



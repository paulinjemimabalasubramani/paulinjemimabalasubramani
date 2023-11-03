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
from saviynt_modules.settings import Config
from saviynt_modules.migration import recursive_migrate_all_files, migrate_csv_file_to_sql_server



# %% Parameters

args |=  {
    }



# %% Get Config

config = Config(args=args)
config.add_target_connection(Connection=Connection)



# %%

recursive_migrate_all_files(source_path=config.source_path, config=config, fn_migrate_file=migrate_csv_file_to_sql_server)



# %% Close Connections / End Program

logger.mark_run_end()



# %%



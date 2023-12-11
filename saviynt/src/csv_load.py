__description__ = """
Load data to SQL Server using bcp tool

bcp <databse_name>.<schema_name>.<table_name> in "<file_path>" -S <server_name>.<dns_suffix> -U <username> -P <password> -c -t "|" -F 2

bcp SaviyntIntegration.dbo.envestnet_hierarchy_firm in "C:/myworkdir/data/envestnet_v35_processed/hierarchy_firm_20231009.txt" -S DW1SQLDATA01.ibddomain.net -T -c -t "|" -F 2

"""


# %% Start Logging

import os
from saviynt_modules.logger import environment, logger
logger.set_logger(app_name=os.path.basename(__file__))



# %% Parse Arguments

if environment.environment >= environment.qa:
    import argparse

    parser = argparse.ArgumentParser(description=__description__)

    parser.add_argument('--pipeline_key', '--pk', help='pipeline_key value for getting pipeline settings', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipeline_key': 'saviynt_mips',
        }



# %% Import Libraries

from saviynt_modules.migration import recursive_migrate_all_files, get_config



# %% Parameters

args |=  {
    }



# %% Get Config

config = get_config(args=args)



# %%

recursive_migrate_all_files(file_type='csv', file_paths=config.source_path, config=config)



# %% Close Connections / End Program

logger.mark_run_end()



# %%



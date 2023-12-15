__description__ = """
Load CSV data to SQL Server

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
        'pipeline_key': 'saviynt_investalink',
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



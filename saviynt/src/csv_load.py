# %% Description

__description__ = """

Load CSV data to SQL Server

"""


# %% Import Libraries

from saviynt_modules.settings import init_app
from saviynt_modules.logger import logger, catch_error
from saviynt_modules.migration import recursive_migrate_all_files



# %% Parameters

test_pipeline_key = 'saviynt_sabos'

args = {}



# %% Get Config

config = init_app(
    __file__ = __file__,
    __description__ = __description__,
    args = args,
    test_pipeline_key = test_pipeline_key,
)



# %%

recursive_migrate_all_files(file_type='csv', file_paths=config.source_path, config=config)



# %% Close Connections / End Program

logger.mark_ending()



# %%



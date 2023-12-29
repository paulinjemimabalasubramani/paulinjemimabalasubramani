# %% Init App

__description__ = """

Load CSV data to SQL Server

"""

from saviynt_modules.logger import init_app, logger, catch_error

args = init_app(
    __file__ = __file__,
    __description__ = __description__,
    test_pipeline_key = 'test01',
)



# %% Import Libraries

from saviynt_modules.migration import recursive_migrate_all_files, get_config



# %% Parameters

args |=  {
    }



# %% Get Config

config = get_config(**args)



# %%

recursive_migrate_all_files(file_type='csv', file_paths=config.source_path, config=config)



# %% Close Connections / End Program

logger.mark_ending()



# %%



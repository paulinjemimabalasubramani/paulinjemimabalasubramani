"""
Library for common generic functions

"""

# %% Import Libraries
import os, logging

from functools import wraps



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            try:
                response = fn(*args, **kwargs)
            except Exception as e:
                exception_message = f"Exception occurred inside '{fn.__name__}' --> Exception Message: {e}"
                print(exception_message)
                if logger:
                    logger.error(exception_message)
                raise e
            return response
        return inner
    return outer


# %% Create file with associated directory tree

@catch_error()
def write_file(file_path:str, contents, mode = 'w'):
    """
    Create file with associated directory tree
    if directory does not exist, then create the directory as well.
    """
    dirname = os.path.dirname(file_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    with open(file_path, mode) as f:
        f.write(contents)


# %% Create Logger with custom configuration

@catch_error()
def make_logging(module_name:str):
    logger = logging.getLogger(module_name)

    log_file = f'./logs/data_eng.log'

    write_file(file_path=log_file, contents='', mode='a')

    logging.basicConfig(
        filename= log_file, 
        filemode='a',
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        datefmt='%d-%b-%y %H:%M:%S',
        level=logging.INFO
        )

    return logger



# %%


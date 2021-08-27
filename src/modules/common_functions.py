"""
Library for common generic functions

"""

# %% Import Libraries
import os, logging, platform, psutil

from functools import wraps


# %% Get System Info in String

def system_info():
    uname = platform.uname()

    sysinfo  = f"System:    {uname.system}\n"
    sysinfo += f"Node:      {uname.node}\n"
    sysinfo += f"Release:   {uname.release}\n"
    sysinfo += f"Version:   {uname.version}\n"
    sysinfo += f"Machine:   {uname.machine}\n"
    sysinfo += f"Processor: {uname.processor}\n"
    sysinfo += f"RAM:       "+str(round(psutil.virtual_memory().total / (1024.0 **3)))+" GB\n"

    return sysinfo



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            try:
                response = fn(*args, **kwargs)
            except Exception as e:
                exception_message  = f"\n"
                exception_message += f"\nException occurred inside '{fn.__name__}'"
                exception_message += f"\nException Message: {e}"
                exception_message += f"\n\n"
                exception_message += system_info()
                exception_message += f"\n"
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


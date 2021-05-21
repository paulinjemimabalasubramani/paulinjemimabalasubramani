"""
Library for common generic functions

"""

# %% Import Libraries
import os, logging



# %% Create file with associated directory tree

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



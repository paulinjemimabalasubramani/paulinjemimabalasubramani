"""
Library for common generic functions

"""

# %% Import Libraries
import os, sys, logging, platform, psutil, yaml

from datetime import datetime

from functools import wraps


# %% Parameters

strftime = r"%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = datetime.now().strftime(strftime)

is_pc = platform.system().lower() == 'windows'

fileshare = '/usr/local/spark/resources/fileshare'
drivers_path = fileshare + '/EDIP-Code/drivers'
config_path = fileshare + '/EDIP-Code/config'
data_path = fileshare + '/Shared'

joinstr = ':' # for extraClassPath

if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'
    #os.environ["PYSPARK_PYTHON"] = r'C:\Users\smammadov\AppData\Local\Programs\Python\Python38\python.exe' # add this line as necessary

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    python_dirname = os.path.dirname(__file__)
    drivers_path = os.path.realpath(python_dirname + '/../../drivers')
    config_path = os.path.realpath(python_dirname + '/../../config')
    data_path = os.path.realpath(python_dirname + '/../../../Shared')

    joinstr = ';' # for extraClassPath




# %% Get System Info in String

def system_info():
    uname = platform.uname()

    sysinfo  = f"OS         {platform.system()}\n"
    sysinfo += f"System:    {uname.system}\n"
    sysinfo += f"Node:      {uname.node}\n"
    sysinfo += f"Release:   {uname.release}\n"
    sysinfo += f"Version:   {uname.version}\n"
    sysinfo += f"Machine:   {uname.machine}\n"
    sysinfo += f"Processor: {uname.processor}\n"
    sysinfo += f"RAM:       " + str(round(psutil.virtual_memory().total / (1024.0 **3))) + " GB\n"

    return sysinfo



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            try:
                response = fn(*args, **kwargs)
            except Exception as e:
                exception_message  = f"\n\nException occurred inside '{fn.__name__}'"
                exception_message += f"\nException Message: {e}\n"
                print(exception_message)

                if logger:
                    #logger.error(exception_message)
                    pass
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
        filename = log_file, 
        filemode = 'a',
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
        datefmt = '%d-%b-%y %H:%M:%S',
        level = logging.INFO,
        )

    return logger



logger = make_logging(__name__)

#logger.info(system_info())


# %% get extraClassPath:

@catch_error(logger)
def get_extraClassPath():
    drivers = []

    for file in os.listdir(drivers_path):
        if file.endswith('.jar'):
            drivers.append(os.path.join(drivers_path, file))
    
    extraClassPath = joinstr.join(drivers)

    print(f'\nDrivers Path: {drivers_path}')
    print(f'Config Path: {config_path}')
    print(f'Data Path: {data_path}')
    print(f'extraClassPath: {extraClassPath}\n')
    return extraClassPath



extraClassPath = get_extraClassPath()



# %% Config Class
class Config:
    """
    Class for retrieving and storing configuration data
    """
    @catch_error(logger)
    def __init__(self, file_path:str, defaults:dict={}):
        for name, value in defaults.items():
            setattr(self, name, value) # Write defaults

        try:
            with open(file_path, 'r') as f:
                contents = yaml.load(f, Loader=yaml.FullLoader)
        except Exception as e:
            except_str = f'Error File was not read: {file_path}'
            print(except_str)
            #logger.error(except_str, exc_info=True)
            return

        for name, value in contents.items():
            setattr(self, name, value) # Overwrite defaults from file




# %%



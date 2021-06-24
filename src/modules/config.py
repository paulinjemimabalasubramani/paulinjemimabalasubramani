""" 
Library Class for retrieving and storing configuration data

"""

# %% libraries

import os, sys, platform

from .common_functions import make_logging, catch_error

import yaml



# %% Logging
logger = make_logging(__name__)


# %% App and Environment Info
app_info = f'Running python on {platform.system()}'

print(app_info)
logger.info(app_info)

is_pc = platform.system().lower() == 'windows'



# %% Config Paths

if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'
    #os.environ["PYSPARK_PYTHON"] = r'C:\Users\smammadov\AppData\Local\Programs\Python\Python38\python.exe' # add this line as necessary

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    drivers_path = os.path.realpath(os.path.dirname(__file__)+'/../../drivers')
    config_path = os.path.realpath(os.path.dirname(__file__)+'/../../config')
    joinstr = ';' # for extraClassPath

else:
    drivers_path = '/usr/local/spark/resources/fileshare/EDIP-Code/drivers'
    config_path = '/usr/local/spark/resources/fileshare/EDIP-Code/config'
    joinstr = ':' # for extraClassPath



# %% extraClassPath

drivers = []
for file in os.listdir(drivers_path):
    if file.endswith('.jar'):
        drivers.append(os.path.join(drivers_path, file))
extraClassPath = joinstr.join(drivers)
print(f'extraClassPath: {extraClassPath}')



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
            logger.error(except_str, exc_info=True)
            return

        for name, value in contents.items():
            setattr(self, name, value) # Overwrite defaults from file



# %%



"""
Test Spark - SQL connection

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys
import tempfile, shutil

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common import make_logging, catch_error
from modules.mysession import MySession


# %% Spark Libraries

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Main Body
if __name__ == '__main__':
    ss = MySession()

    print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

    if ss.is_pc:
        data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared')
        temp_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Temp')
    else:
        # /usr/local/spark/resources/fileshare/Shared
        data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared')
        temp_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Temp')

    os.makedirs(data_path_folder, exist_ok=True)
    os.makedirs(temp_path_folder, exist_ok=True)
    



# %%

def recursive_unzip(folder_path:str, temp_path_folder:str=None, parent:str='', walk:bool=False):
    zip_files = []

    if walk:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.zip'):
                    zip_files.append(os.path.join(root, file))
    else:
        for file in os.listdir(folder_path):
            if file.endswith('.zip'):
                zip_files.append(os.path.join(folder_path, file))

    for zip_file in zip_files:
        with tempfile.TemporaryDirectory(dir=temp_path_folder) as tmpdir:
            print(f'Extracting {zip_file} to {tmpdir}')
            shutil.unpack_archive(filename=zip_file, extract_dir=tmpdir)
            recursive_unzip(tmpdir, temp_path_folder=temp_path_folder, walk=True)


# %%
recursive_unzip(data_path_folder, temp_path_folder)


# %%

file_path = os.path.join(temp_path_folder, r'2021-05-16\7461_PostAppointmentsReport_2021-05-16.xml')
print(file_path)

df = (ss.spark.read
    .format("com.databricks.spark.xml")
#    .option("rootTag", "PostAppointmentsReport")
    .option("rowTag", "Criteria")
    .load(file_path)
)


# %%

df.show()
# %%

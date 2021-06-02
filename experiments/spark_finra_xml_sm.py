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

import pyspark
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
                    pass # Placeholder
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

file_path = os.path.join(temp_path_folder, r'2021-05-16\7461_IndividualInformationReport_2021-05-16\7461_IndividualInformationReport_2021-05-16.iid')
print(file_path)

df = ss.read_xml(file_path, rowTag="?xml")

df.printSchema()

# %% Get Criteria

def get_xml_criteria(df, criteria_str:str='Criteria'):
    if df.count()==1 and criteria_str in df.columns:
        dtype:str = [x[1] for x in df.dtypes if x[0] == criteria_str][0]
        if dtype.startswith('struct'):
            return df.select(col(criteria_str)).collect()[0][0].asDict()
    return {}

get_xml_criteria(df)


# %% Critera-less DF


dfc = df.select([col(c) for c in df.columns if c!='Criteria'])


# %% Flatten XML


def flatten_df(df) -> pyspark.sql.dataframe.DataFrame:
    cols = []
    nested = False

    # to ensure not to have more than 1 explosion in a table
    expolode_flag = len([c[0] for c in df.dtypes if c[1].startswith('array')]) <= 1

    for c in df.dtypes:
        if c[1].startswith('struct'):
            nested = True
            if len(df.columns)>1:
                struct_cols = df.select(c[0]+'.*').columns
                cols.extend([col(c[0]+'.'+cc).alias(c[0]+'_'+cc) for cc in struct_cols])
            else:
                cols.append(c[0]+'.*')
        elif c[1].startswith('array') and expolode_flag:
            nested = True
            cols.append(explode(c[0]).alias(c[0]))
        else:
            cols.append(c[0])

    dfx = df.select(cols)
    if nested:
        print('\n' ,dfx.columns)
        dfx = flatten_df(dfx)

    return dfx


dfy = flatten_df(dfc)

# %%

print(len(dfy.columns))
dfy.printSchema()


# %%

dfy.limit(1).collect()


# %%

df.show(n=10, truncate=True)
# %%

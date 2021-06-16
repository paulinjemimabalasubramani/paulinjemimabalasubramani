"""
Test Spark - SQL connection

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys, tempfile, shutil, json

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc


# %% Spark Libraries

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType, ArrayType, LongType

from pyspark.sql import functions as F
from pyspark.sql.functions import array, col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Initiate Spark
spark = create_spark()


# %% Get Paths

print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared')
    temp_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Temp')
else:
    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared')
    temp_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Temp')

os.makedirs(data_path_folder, exist_ok=True)
os.makedirs(temp_path_folder, exist_ok=True)

schema_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../config/finra')


# %% Recursive Unzip

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


# %% Load Schema

def base_to_schema(base:dict):
    st = []
    for key, val in base.items():
        if val is None:
            v = StringType()
        elif isinstance(val, dict):
            v = base_to_schema(val)
        elif isinstance(val, list):
            v = ArrayType(base_to_schema(val[0]), True)
        else:
            v = val
        
        st.append(StructField(key, v, True))
    return StructType(st)


with open(os.path.join(schema_path_folder, 'IndividualInformationReport.json'), 'r') as f:
    base = json.load(f)

schema = base_to_schema(base)


# %% Read XML File

file_name = r'7461_IndividualInformationReport_2021-05-16.iid'

file_path = os.path.join(temp_path_folder, r'2021-05-16\7461_IndividualInformationReport_2021-05-16\7461_IndividualInformationReport_2021-05-16.iid')
#file_path = os.path.join(data_path_folder, r'test2.xml')

print(file_path)

#df = read_xml(file_path, rowTag="?xml")
df = read_xml(spark, file_path, rowTag="Indvls", schema=schema)


df.printSchema()

df.show(1)

df.schema

# %% Get XML Criteria

def get_xml_criteria(df, criteria_str:str='Criteria'):
    if df.count()==1 and criteria_str in df.columns:
        dtype:str = [x[1] for x in df.dtypes if x[0] == criteria_str][0]
        if dtype.startswith('struct'):
            return df.select(col(criteria_str)).collect()[0][0].asDict()
    return {}

criteria = get_xml_criteria(df)

print(criteria)
# TODO: Track Criteria in metadata

df = df.select([col(c) for c in df.columns if c!='Criteria'])



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
                cols.extend([col(c[0]+'.'+cc).alias(c[0]+('' if cc[0]=='_' else '_')+cc) for cc in struct_cols])
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


#df = flatten_df(df)


# %% flatten and divide

def flatten_n_divide_df(df, name:str='main'):
    df = flatten_df(df)

    string_cols = [c[0] for c in df.dtypes if c[1]=='string']
    array_cols = [c[0] for c in df.dtypes if c[1].startswith('array')]

    if len(array_cols)==0:
        return {name:df}

    df_list = dict()
    for array_col in array_cols:
        colx = string_cols + [array_col]
        dfx = df.select(*colx)
        df_list={**df_list, **flatten_n_divide_df(dfx, name=name+'_'+array_col)}

    return df_list





df_list = flatten_n_divide_df(df, name='IndividualInformationReport')

# %% Count of DF's

print(len(df_list))

# %% Print all schemas

for df_name, df_flat in df_list.items():
    print('\n'+df_name)
    df_flat.printSchema()




# %% Print Number of Columns and Schema

print(len(df.columns))
df.printSchema()


# %% Wrtie to Azure




# %% Write to JSON
"""
data_path = os.path.join(data_path_folder, file_name+'.json')
df.coalesce(1).write.json(path = data_path, mode='overwrite')


""";


# %% Show sample row(s)

#df.show(n=5, truncate=True)

df.limit(1).write.json(r"C:\Users\smammadov\packages\Temp\t.json", mode='overwrite')


# %%

#df.select('Comp_Nm__first').where(lit(14)==F.length(col("Comp_Nm__first"))).collect()


#df.select(F.max(F.length(col("Comp_Nm__first"))).alias('maxx')).collect()


# %%


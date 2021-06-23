"""
Flatten all XML files and migrate them to ADLS Gen 2 

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys, tempfile, shutil, json

from datetime import datetime

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2
from modules.data_functions import  add_elt_columns


# %% Spark Libraries

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType, ArrayType, LongType

from pyspark.sql import functions as F
from pyspark.sql.functions import array, col, lit, split, explode, udf
from pyspark.sql import Row, Window


# %% Logging
logger = make_logging(__name__)


# %% Parameters

storage_account_name = "agaggrlakescd"
#storage_account_name = "agfsclakescd"

container_name = "ingress"
domain_name = 'financial_professional'
database = 'FINRA'
format = 'delta'

partitionBy = 'RECEPTION_DATE'
execution_date = datetime.now()


# %% Initiate Spark
spark = create_spark()


# %% Setup spark to ADLS Gen2 connection

setup_spark_adls_gen2_connection(spark, storage_account_name)


# %% Get Paths

print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared')
else:
    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared')

schema_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../config/finra')


# %% Load Schema

@catch_error(logger)
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


#with open(os.path.join(schema_path_folder, 'IndividualInformationReport.json'), 'r') as f:
#    base = json.load(f)

#schema = base_to_schema(base)



# %% Name extract

@catch_error(logger)
def extract_data_from_finra_file_name(file_name):
    basename = os.path.basename(file_name)
    sp = basename.split("_")
    ans = {
        'firm': sp[0],
        'name': sp[1],
        'date': sp[2].rsplit('.', 1)[0]
    }
    return ans

# %% Find rowTags

@catch_error(logger)
def find_rowTags(file_path):
    df = read_xml(spark, file_path, rowTag="?xml")
    return [c for c in df.columns if c not in ['Criteria']]


# %% Flatten XML

@catch_error(logger)
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
        if is_pc: print('\n' ,dfx.columns)
        dfx = flatten_df(dfx)

    return dfx



# %% flatten and divide

@catch_error(logger)
def flatten_n_divide_df(df, name:str='main'):
    if not df.rdd.flatMap(lambda x: x).collect(): # check if df is empty
        return dict()

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



# %% Write df list to Azure

@catch_error(logger)
def write_df_list_to_azure(df_list, df_file_name, reception_date):

    for df_name, dfx in df_list.items():
        print(f'\n Writing {df_name} to Azure...')
        if is_pc: dfx.printSchema()

        data_type = 'data'
        container_folder = f"{data_type}/{domain_name}/{database}/{df_file_name}"

        dfx = add_elt_columns(df=dfx, execution_date=execution_date, reception_date=reception_date)

        save_adls_gen2(
            df=dfx,
            storage_account_name = storage_account_name,
            container_name = container_name,
            container_folder = container_folder,
            table = df_name,
            partitionBy = partitionBy,
            format = format
        )

        # Metadata
        data_type = 'metadata'
        container_folder = f"{data_type}/{domain_name}/{database}/{df_file_name}"

        meta_columns = ["column_name", "data_type"]
        meta_data = dfx.dtypes
        meta_df = spark.createDataFrame(data=meta_data, schema=meta_columns)
        meta_df = add_elt_columns(df=meta_df, execution_date=execution_date, reception_date=reception_date)
        
        save_adls_gen2(
            df=meta_df,
            storage_account_name = storage_account_name,
            container_name = container_name,
            container_folder = container_folder,
            table = df_name,
            partitionBy = partitionBy,
            format = format
        )

        dfx.unpersist()
        meta_df.unpersist()

    print('Done writing to Azure')


# %% Main Processing of Finra file

@catch_error(logger)
def process_finra(root, file):
    name_data = extract_data_from_finra_file_name(file)
    print('\n', name_data)

    file_path = os.path.join(root, file)

    rowTags = find_rowTags(file_path)
    print(f'rowTags: {rowTags}')
    rowTag = rowTags[0]

    schema_file = name_data['name']+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if os.path.isfile(schema_path):
        print(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        df = read_xml(spark, file_path, rowTag=rowTag, schema=schema)
    else:
        print(f"No manual schema defined for {name_data['name']}. Using default schema.")
        df = read_xml(spark, file_path, rowTag=rowTag)

    df_list = flatten_n_divide_df(df, name=name_data['name'])
    if not df_list:
        print(f"No data to write -> {name_data['name']}")
        return

    write_df_list_to_azure(
        df_list = df_list,
        df_file_name = name_data['name'],
        reception_date = name_data['date']
        )

    df.unpersist()



# %% Recursive Unzip

@catch_error(logger)
def recursive_unzip(folder_path:str, temp_path_folder:str=None, parent:str='', walk:bool=False):
    zip_files = []

    if walk:
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith('.zip'):
                    zip_files.append(os.path.join(root, file))
                else:
                    process_finra(root, file)
    else:
        for file in os.listdir(folder_path):
            if file.endswith('.zip'):
                zip_files.append(os.path.join(folder_path, file))

    for zip_file in zip_files:
        with tempfile.TemporaryDirectory(dir=temp_path_folder) as tmpdir:
            print(f'\nExtracting {zip_file} to {tmpdir}')
            shutil.unpack_archive(filename=zip_file, extract_dir=tmpdir)
            recursive_unzip(tmpdir, temp_path_folder=temp_path_folder, walk=True)



recursive_unzip(data_path_folder)



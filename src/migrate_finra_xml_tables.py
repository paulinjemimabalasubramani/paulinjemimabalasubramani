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

from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc


# %% Spark Libraries

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType, ArrayType, LongType

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, split, explode, udf
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


# %% Create Schema - Test


base = {
    'person': [{
        '_age':None, 
        '_height':None, 
        '_name':None, 
        'locations':{
            'loc':[{
                '_address':None, 
                '_id':None
            }]
        }
    }]
}


# %% Create Schema

base = {
    'Indvl': [{
        'Comp': {
            'Nm': {
                '_VALUE': None,
                '_first': None,
                '_last': None,
                '_mid': None,
                '_suf': None,
            },
            '_actvMltry': None,
            '_bllngCd': None,
            '_indvlPK': None,
            '_indvlSSN': None,
            '_matDiff': None,
            '_rgstdMult': None,
            '_rptblDisc': None,
            '_statDisq': None,
        },
        'ContEds': {
            'ContEd': [{
                'Appts': None,
                '_begDt': None,
                '_endDt': None,
                '_eventDt': None,
                '_frgnDfrdFl': None,
                '_mltryDfrdFl': None,
                '_nrlmtID': None,
                '_rslt': None,
                '_sssn': None,
                '_sssnDt': None,
                '_sssnReqSt': None,
                '_sssnSt': None,
            }],
            '_VALUE': None,
            '_baseDt': None,
            '_st': None,
        },
        'CrntRgstns': { # Manual Input
            'CrntRgstn':[{
                'CrntDfcnys': None,
                '_actvReg': None,
                '_aprvlDt': None,
                '_crtnDt': None,
                '_empStDt': None,
                '_regAuth': None,
                '_regCat': None,
                '_st': None,
                '_stDt': None,
                '_trmnnDt': None,
                '_updateTS': None,
            }],
        },
        'DRPs': None,
        'Dsgntns': None,
        'EmpHists': { # Manual Input
            'EmpHist': [{
                'DtRng': {
                    '_VALUE': None,
                    '_fromDt': None,
                    '_toDt': None,
                },
                '_city': None,
                '_cntryCd': None,
                '_invRel': None,
                '_orgNm': None,
                '_pstnHeld': None,
                '_seqNb': None,
                '_state': None,
            }]
        },
        'EventFlngHists': {
            'EventFlngHist': [{
                '_VALUE': None,
                '_dt': None,
                '_flngType': None,
                '_frmType': None,
                '_id': None,
                '_src': None,
                '_type': None,
            }]
        },
        'ExmWvrs': None,
        'Exms': {
            'Exm': [{
                'Appts': None,
                '_exmCd': None,
                '_exmDt': None,
                '_exmValidFl': None,
                '_grd': None,
                '_nrlmtID': None,
                '_st': None,
                '_stDt': None,
                '_updateTS': None,
                '_wndwBeginDt': None,
                '_wndwEndDt': None,
            }]
        },
        'FngprInfos': {
            'FngprInfo': [{
                '_VALUE': None,
                '_barCd': None,
                '_orgNm': None,
                '_pstnInFirm': None,
                '_st': None,
                '_stDt': None,
            }]
        },
        'IAAffltns': None,
        'IdentInfo': {
            'Ht': {
                '_VALUE': None,
                '_ft': None,
                '_in': None,
            },
            '_birthCntryCd': None,
            '_birthCntryOld': None,
            '_birthPrvnc': None,
            '_birthStCd': None,
            '_dob': None,
            '_eye': None,
            '_gender': None,
            '_hair': None,
            '_wt': None,
        },
        'OffHists': {
            'OffHist': [{
                'DtRng': {
                    '_VALUE': None,
                    '_fromDt': None,
                    '_toDt': None,
                },
                'EmpLocs': {
                    'EmpLoc': [{
                        'Addr': {
                            '_VALUE': None,
                            '_city': None,
                            '_cntryCd': None,
                            '_cntryOld': None,
                            '_postlCd': None,
                            '_state': None,
                            '_strt1': None,
                            '_strt2': None,
                        },
                        '_bllngCd': None,
                        '_brnchPK': None,
                        '_fromDt': None,
                        '_lctdFl': None,
                        '_mainOfcBDFl': None,
                        '_mainOfcIAFl': None,
                        '_oldSeqNb': None,
                        '_prvtRsdnc': None,
                        '_regdLocFl': None,
                        '_seqNb': None,
                        '_sprvdFl': None,
                        '_toDt': None,
                    }]
                },
                '_empCntxt': None,
                '_firmAsctnSt': None,
                '_ndpndCntrcrFl': None,
                '_orgNm': None,
                '_orgPK': None,
                '_termDt': None,
                '_termExpln': None,
                '_termRsn': None,
                '_termRsnAmndtExpln': None,
            }]
        },
        'OthrBuss': {
            'OthrBus': {
                '_VALUE': None,
                '_desc': None,
            }
        },
        'OthrNms': None,
        'ResHists': {
            'ResHist': [{
                'Addr': {
                    '_VALUE': None,
                    '_city': None,
                    '_cntryCd': None,
                    '_postlCd': None,
                    '_state': None,
                    '_strt1': None,
                },
                'DtRng': {
                    '_VALUE': None,
                    '_fromDt': None,
                    '_toDt': None,
                },
                '_seqNb': None,
            }]
        },
    }]
}




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


df = flatten_df(df)

# %% Print Number of Columns and Schema

print(len(df.columns))
df.printSchema()


# %% Wrtie to parquet format

print(os.path.realpath(os.path.dirname(__file__)))

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared/Finra')
else:
    # /usr/local/spark/resources/fileshare/Shared/Finra
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared/Finra')

#os.makedirs(data_path_folder, exist_ok=True)
data_path = os.path.join(data_path_folder, file_name+'.parquet')
print(f'Data path: {data_path}')

codec = spark.conf.get("spark.sql.parquet.compression.codec")
print(f"Write data in parquet format with '{codec}' compression")

df.write.parquet(path = data_path, mode='overwrite')


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


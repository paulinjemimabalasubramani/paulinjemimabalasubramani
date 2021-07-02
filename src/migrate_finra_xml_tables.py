"""
Flatten all XML files and migrate them to ADLS Gen 2 

https://www.finra.org/filing-reporting/web-crd/web-eft-schema-documentation-and-schema-files

Ingestion Path:
CRD Number > Files (2 of them are zip) get the latest dates

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys, tempfile, shutil, json, re
from collections import defaultdict

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name
from modules.data_functions import  add_elt_columns, execution_date, column_regex


# %% Spark Libraries

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, DoubleType, IntegerType, FloatType, ArrayType, LongType

from pyspark.sql import functions as F
from pyspark.sql.functions import array, col, lit, split, explode, udf
from pyspark.sql import Row, Window

from pyspark.sql.functions import md5, concat_ws


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

firms = [
    {'firm_name': 'FSC', 'firm_number': '7461' },
    {'firm_name': 'RAA', 'firm_number': '23131'},
]

firm_number_to_name = {firm['firm_number']:firm['firm_name'] for firm in firms}

KeyIndicator = 'MD5_KEY'


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



# %% Name extract

@catch_error(logger)
def extract_data_from_finra_file_name(file_name):
    basename = os.path.basename(file_name)
    sp = basename.split("_")
    ans = {
        'firm_number': sp[0],
        'table_name': sp[1],
        'date': sp[2].rsplit('.', 1)[0]
    }
    return ans



# %% Find rowTags

@catch_error(logger)
def find_rowTags(file_path):
    xml_table = read_xml(spark, file_path, rowTag="?xml")
    return [c for c in xml_table.columns if c not in ['Criteria']]



# %% Flatten XML

@catch_error(logger)
def flatten_df(xml_table) -> pyspark.sql.dataframe.DataFrame:
    cols = []
    nested = False

    # to ensure not to have more than 1 explosion in a table
    expolode_flag = len([c[0] for c in xml_table.dtypes if c[1].startswith('array')]) <= 1

    for c in xml_table.dtypes:
        if c[1].startswith('struct'):
            nested = True
            if len(xml_table.columns)>1:
                struct_cols = xml_table.select(c[0]+'.*').columns
                cols.extend([col(c[0]+'.'+cc).alias(c[0]+('' if cc[0]=='_' else '_')+cc) for cc in struct_cols])
            else:
                cols.append(c[0]+'.*')
        elif c[1].startswith('array') and expolode_flag:
            nested = True
            cols.append(explode(c[0]).alias(c[0]))
        else:
            cols.append(c[0])

    xml_table_select = xml_table.select(cols)
    if nested:
        if is_pc: print('\n' ,xml_table_select.columns)
        xml_table_select = flatten_df(xml_table_select)

    return xml_table_select



# %% flatten and divide

@catch_error(logger)
def flatten_n_divide_df(xml_table, name:str='main'):
    if not xml_table.rdd.flatMap(lambda x: x).collect(): # check if xml_table is empty
        return dict()

    xml_table = flatten_df(xml_table)

    string_cols = [c[0] for c in xml_table.dtypes if c[1]=='string']
    array_cols = [c[0] for c in xml_table.dtypes if c[1].startswith('array')]

    if len(array_cols)==0:
        return {name:xml_table}

    df_list = dict()
    for array_col in array_cols:
        colx = string_cols + [array_col]
        xml_table_select = xml_table.select(*colx)
        df_list={**df_list, **flatten_n_divide_df(xml_table_select, name=name+'_'+array_col)}

    return df_list


# %% Create tableinfo

tableinfo = defaultdict(list)

def add_table_to_tableinfo(xml_table, firm, table_name):
    for ix, (col_name, col_type) in enumerate(xml_table.dtypes):
        tableinfo['SourceDatabase'].append(database)
        tableinfo['SourceSchema'].append(firm)
        tableinfo['TableName'].append(table_name)
        tableinfo['SourceColumnName'].append(col_name)
        tableinfo['SourceDataType'].append(col_type)
        tableinfo['SourceDataLength'].append(0)
        tableinfo['SourceDataPrecision'].append(0)
        tableinfo['SourceDataScale'].append(0)
        tableinfo['OrdinalPosition'].append(ix+1)
        tableinfo['CleanType'].append(col_type)
        tableinfo['TargetColumnName'].append(re.sub(column_regex, '_', col_name))
        tableinfo['TargetDataType'].append('string')
        tableinfo['IsNullable'].append(0 if col_name==KeyIndicator else 1)
        tableinfo['KeyIndicator'].append(1 if col_name==KeyIndicator else 0)
        tableinfo['IsActive'].append(1)
        tableinfo['CreatedDateTime'].append(execution_date)
        tableinfo['ModifiedDateTime'].append(execution_date)




# %% Write xml table list to Azure

@catch_error(logger)
def write_xml_table_list_to_azure(xml_table_list, file_name, reception_date, firm):

    for df_name, xml_table in xml_table_list.items():
        print(f'\n Writing {df_name} to Azure...')
        if is_pc: xml_table.printSchema()

        data_type = 'data'
        container_folder = f"{data_type}/{domain_name}/{database}/{firm}"

        xml_table.withColumn(KeyIndicator, md5(concat_ws("||", *xml_table.columns))) # add HASH column for key indicator

        xml_table = add_elt_columns(table_to_add=xml_table, execution_date=execution_date, reception_date=reception_date)

        if is_pc and False:
            print(fr'Save to local {database}\{file_name}\{df_name}')
            temp_path = os.path.join(data_path_folder, 'temp')
            #xml_table.coalesce(1).write.csv( path = fr'{temp_path}\{database}\{file_name}\{df_name}.csv',  mode='overwrite', header='true')
            xml_table.coalesce(1).write.json(path = fr'{temp_path}\{database}\{file_name}\{df_name}.json', mode='overwrite')

        if False:
            save_adls_gen2(
                table_to_save = xml_table,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table = df_name,
                partitionBy = partitionBy,
                format = format
            )
        
        add_table_to_tableinfo(xml_table=xml_table, firm=firm, table_name = df_name)


    print('Done writing to Azure')


# %% Main Processing of Finra file

@catch_error(logger)
def process_finra(root, file, firm):
    name_data = extract_data_from_finra_file_name(file)
    print('\n', name_data)

    file_path = os.path.join(root, file)

    rowTags = find_rowTags(file_path)
    print(f'\nrowTags: {rowTags}\n')
    rowTag = rowTags[0]

    schema_file = name_data['table_name']+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if os.path.isfile(schema_path):
        print(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        xml_table = read_xml(spark, file_path, rowTag=rowTag, schema=schema)
    else:
        print(f"No manual schema defined for {name_data['table_name']}. Using default schema.")
        xml_table = read_xml(spark, file_path, rowTag=rowTag)

    if is_pc: xml_table.printSchema()

    xml_table_list = flatten_n_divide_df(xml_table, name=name_data['table_name'])
    if not xml_table_list:
        print(f"No data to write -> {name_data['table_name']}")
        return

    write_xml_table_list_to_azure(
        xml_table_list= xml_table_list,
        file_name = name_data['table_name'],
        reception_date = name_data['date'],
        firm = firm,
        )




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


#recursive_unzip(data_path_folder)



# %% Manual Iteration

manual_iteration = False

if manual_iteration:
    firm = firms[0]
    firm_folder = firm['folder']
    folder_path = os.path.join(data_path_folder, firm_folder)

    root = r"C:\Users\smammadov\packages\Shared\RAA_FINRA\2021-06-20"
    file = r"23131_PostExamsReport_2021-06-19.exm"
    process_finra(root=root, file=file)



# %% Process all files

@catch_error(logger)
def process_all_files():
    for firm in firms:
        firm_folder = firm['firm_number']
        folder_path = os.path.join(data_path_folder, firm_folder)
        print(f"Firm: {firm['firm_name']}\n")

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                print(os.path.join(root, file), '\n')

                if manual_iteration:
                    print(root, file, '\n', sep='\n')
                
                if not manual_iteration:
                    if file.endswith('.zip'):
                        print('PLACEHOLDER FOR ZIP FILES')
                        pass
                    else:
                        process_finra(root=root, file=file, firm=firm['firm_name'])
        break



process_all_files()


# %% Save Tableinfo

tableinfo_keys = list(tableinfo.keys())
tableinfo_values = list(tableinfo.values())

list_of_dict = []
for vi in range(len(tableinfo_values[0])):
    list_of_dict.append({k:v[vi] for k, v in tableinfo.items()})

meta_tableinfo = spark.createDataFrame(list_of_dict)

meta_tableinfo = meta_tableinfo.select(
    meta_tableinfo.SourceDatabase,
    meta_tableinfo.SourceSchema,
    meta_tableinfo.TableName,
    meta_tableinfo.SourceColumnName,
    meta_tableinfo.SourceDataType,
    meta_tableinfo.SourceDataLength,
    meta_tableinfo.SourceDataPrecision,
    meta_tableinfo.SourceDataScale,
    meta_tableinfo.OrdinalPosition,
    meta_tableinfo.CleanType,
    meta_tableinfo.TargetColumnName,
    meta_tableinfo.TargetDataType,
    meta_tableinfo.IsNullable,
    meta_tableinfo.KeyIndicator,
    meta_tableinfo.IsActive,
    meta_tableinfo.CreatedDateTime,
    meta_tableinfo.ModifiedDateTime,
).orderBy(
    meta_tableinfo.SourceDatabase,
    meta_tableinfo.SourceSchema,
    meta_tableinfo.TableName,
)

if is_pc: meta_tableinfo.printSchema()

save_adls_gen2(
        table_to_save = meta_tableinfo,
        storage_account_name = storage_account_name,
        container_name = 'tables',
        container_folder = '',
        table = f'{tableinfo_name}_{database}',
        partitionBy = 'ModifiedDateTime',
        format = format,
    )



# %%


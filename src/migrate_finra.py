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
from datetime import datetime


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, read_adls_gen2
from modules.data_functions import  to_string, remove_column_spaces, add_elt_columns, execution_date, column_regex, partitionBy, \
    metadata_FirmSourceMap, partitionBy_value


from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, lit, explode, md5, concat_ws



# %% Logging
logger = make_logging(__name__)


# %% Parameters

manual_iteration = False
save_xml_to_adls_flag = True

if not is_pc:
    manual_iteration = False
    save_xml_to_adls_flag = True

domain_name = 'financial_professional'
database = 'FINRA'
tableinfo_source = database

KeyIndicator = 'MD5_KEY'
FirmCRDNumber = 'FIRM_CRD_NUMBER'

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'


# %% Initiate Spark
spark = create_spark()


# %% Get Paths

print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../../Shared/FINRA')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../config/finra')
else:
    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/Shared/FINRA')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__)+'/../resources/fileshare/EDIP-Code/config/finra')



# %% Get Firms

@catch_error(logger)
def get_firms():
    storage_account_name = to_storage_account_name()
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    firms_table = read_adls_gen2(
        spark = spark,
        storage_account_name = storage_account_name,
        container_name = tableinfo_container_name,
        container_folder = '',
        table = metadata_FirmSourceMap,
        file_format = file_format
    )

    firms_table = firms_table.filter(
        (col('Source') == lit(database.upper()).cast("string")) & 
        (col('IsActive') == lit(1))
    )

    firms_table = firms_table.select('Firm', 'SourceKey') \
        .withColumnRenamed('Firm', 'firm_name') \
        .withColumnRenamed('SourceKey', 'crd_number')

    firms = firms_table.toJSON().map(lambda j: json.loads(j)).collect()

    return firms



firms = get_firms()

if is_pc: print(firms)


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

    try:
        ans = {
            'crd_number': sp[0],
            'table_name': sp[1],
            'date': sp[2].rsplit('.', 1)[0]
        }
        _ = datetime.strptime(ans['date'], r'%Y-%m-%d')
        assert len(sp)==3 or (len(sp)==4 and sp[1].upper()==finra_individual_delta_name.upper())
    except:
        print(f'Cannot parse file name: {file_name}')
        return

    return ans



# %% Find rowTags

@catch_error(logger)
def find_finra_xml_meta(file_path):
    xml_table = read_xml(spark, file_path, rowTag="?xml")
    criteria = xml_table.select('Criteria.*').toJSON().map(lambda j: json.loads(j)).collect()[0]
    if not criteria.get(reportDate_name):
        criteria[reportDate_name] = criteria['_postingDate']
    rowTags = [c for c in xml_table.columns if c not in ['Criteria']]
    return criteria, rowTags



# %% Flatten XML

@catch_error(logger)
def flatten_df(xml_table):
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
def flatten_n_divide_df(xml_table, table_name:str):
    if not xml_table.rdd.flatMap(lambda x: x).collect(): # check if xml_table is empty
        return dict()

    xml_table = flatten_df(xml_table=xml_table)

    string_cols = [c[0] for c in xml_table.dtypes if c[1]=='string']
    array_cols = [c[0] for c in xml_table.dtypes if c[1].startswith('array')]

    if len(array_cols)==0:
        return {table_name:xml_table}

    xml_table_list = dict()
    for array_col in array_cols:
        colx = string_cols + [array_col]
        xml_table_select = xml_table.select(*colx)
        xml_table_list={**xml_table_list, **flatten_n_divide_df(xml_table=xml_table_select, table_name=table_name+'_'+array_col)}

    return xml_table_list


# %% Create tableinfo

tableinfo = defaultdict(list)

@catch_error(logger)
def add_table_to_tableinfo(xml_table, firm_name, table_name):
    for ix, (col_name, col_type) in enumerate(xml_table.dtypes):
        tableinfo['SourceDatabase'].append(database)
        tableinfo['SourceSchema'].append(firm_name)
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
        tableinfo['IsNullable'].append(0 if col_name in [KeyIndicator] else 1)
        tableinfo['KeyIndicator'].append(1 if col_name in [KeyIndicator] else 0)
        tableinfo['IsActive'].append(1)
        tableinfo['CreatedDateTime'].append(execution_date)
        tableinfo['ModifiedDateTime'].append(execution_date)
        tableinfo[partitionBy].append(partitionBy_value)


# %% Add Business Key

@catch_error(logger)
def add_business_key(xml_table):
    cols = xml_table.columns

    cols_indvl = sorted([c for c in cols if ('individualCRDNumber'.upper() in c.upper() or 'indvlPK'.upper() in c.upper())], key=len)
    if is_pc: print(f'\nindvlPK: {cols_indvl}')
    if cols_indvl:
        xml_table = xml_table.withColumn('BUSINESS_KEY_INDVL', concat_ws('_', col(FirmCRDNumber), col(cols_indvl[0])))

    cols_brnch = sorted([c for c in cols if ('brnchPK'.upper() in c.upper())], key=len)
    if is_pc: print(f'\nbrnchPK: {cols_brnch}')
    if cols_brnch:
        xml_table = xml_table.withColumn('BUSINESS_KEY_BRNCH', concat_ws('_', col(FirmCRDNumber), col(cols_brnch[0])))

    return xml_table



# %% Write xml table list to Azure

@catch_error(logger)
def write_xml_table_list_to_azure(xml_table_list:dict, file_name:str, reception_date:str, firm_name:str, storage_account_name:str, is_full_load:bool, crd_number:str):

    for df_name, xml_table in xml_table_list.items():
        print(f'\nWriting {df_name} to Azure...')

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        xml_table1 = to_string(table_to_convert_columns = xml_table, col_types = []) # Convert all columns to string
        xml_table1 = remove_column_spaces(table_to_remove = xml_table1)
        xml_table1 = xml_table1.withColumn('FILE_DATE', lit(str(reception_date)))
        xml_table1 = xml_table1.withColumn(FirmCRDNumber, lit(str(crd_number)))
        xml_table1 = add_business_key(xml_table = xml_table1)
        xml_table1 = xml_table1.withColumn(KeyIndicator, md5(concat_ws('_', *xml_table1.columns))) # add HASH column for key indicator

        add_table_to_tableinfo(xml_table=xml_table1, firm_name=firm_name, table_name = df_name)

        xml_table1 = add_elt_columns(
            table_to_add = xml_table1,
            reception_date = reception_date,
            source = tableinfo_source,
            is_full_load = is_full_load,
            dml_type = 'I' if is_full_load or firm_name not in ['IndividualInformationReport'] else 'U',
            )

        if is_pc: xml_table1.printSchema()

        if is_pc: # and manual_iteration:
            print(fr'Save to local {database}\{file_name}\{df_name}')
            temp_path = os.path.join(data_path_folder, 'temp')
            xml_table1.coalesce(1).write.csv( path = fr'{temp_path}\{storage_account_name}\{container_folder}\{df_name}.csv',  mode='overwrite', header='true')
            xml_table1.coalesce(1).write.json(path = fr'{temp_path}\{storage_account_name}\{container_folder}\{df_name}.json', mode='overwrite')

        if save_xml_to_adls_flag:
            save_adls_gen2(
                table_to_save = xml_table1,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table = df_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

    print('Done writing to Azure')




# %% Main Processing of Finra file

@catch_error(logger)
def process_finra_file(root:str, file:str, firm_name:str, storage_account_name:str):
    file_path = os.path.join(root, file)

    if firm_name == 'IndividualInformationReportDelta':
        firm_name = 'IndividualInformationReport'

    name_data = extract_data_from_finra_file_name(file)
    if not name_data:
        print(f'Not valid file name format: {file_path}   -> SKIPPING')
        return

    print('\n', name_data)

    criteria, rowTags = find_finra_xml_meta(file_path)
    print(f'\nrowTags: {rowTags}\n')
    rowTag = rowTags[0]

    if criteria['_firmCRDNumber'] != name_data['crd_number']:
        print(f"\nFirm CRD Number in Criteria '{criteria['_firmCRDNumber']}' does not match to the CRD Number in the file name '{name_data['crd_number']}'\n")
        name_data['crd_number'] = criteria['_firmCRDNumber']

    if criteria[reportDate_name] != name_data['date']:
        print(f"\nReport/Posting Date in Criteria '{criteria[reportDate_name]}' does not match to the date in the file name '{name_data['date']}'\n")
        name_data['date'] = criteria[reportDate_name]
    
    is_full_load = criteria.get('_IIRType') == 'FULL' or firm_name in ['BranchInformationReport']

    schema_file = name_data['table_name']+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if os.path.isfile(schema_path):
        print(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        xml_table = read_xml(spark, file_path, rowTag=rowTag, schema=schema)
    else:
        print(f"Looking for Schema File Location: {schema_path}")
        print(f"No manual schema defined for {name_data['table_name']}. Using default schema.")
        xml_table = read_xml(spark, file_path, rowTag=rowTag)

    if is_pc: xml_table.printSchema()

    xml_table_list = flatten_n_divide_df(xml_table=xml_table, table_name=name_data['table_name'])
    if not xml_table_list:
        print(f"No data to write -> {name_data['table_name']}")
        return

    write_xml_table_list_to_azure(
        xml_table_list= xml_table_list,
        file_name = name_data['table_name'],
        reception_date = name_data['date'],
        firm_name = firm_name,
        storage_account_name = storage_account_name,
        is_full_load = is_full_load,
        crd_number = name_data['crd_number'],
        )



# %% Manual Iteration

if manual_iteration:
    firm = firms[0]
    firm_folder = firm['crd_number']
    folder_path = os.path.join(data_path_folder, firm_folder)
    firm_name = firm['firm_name']
    print(f"\n\nFirm: {firm_name}, Firm CRD Number: {firm['crd_number']}")

    root = r"C:\Users\smammadov\packages\Shared"
    file = r"7461_IndividualInformationReport_2021-05-16.iid"
    file_path = os.path.join(root, file)

    criteria, rowTags = find_finra_xml_meta(file_path)
    #process_finra_file(root=root, file=file, firm_name=firm_name)




# %% Get Maximum Date from file names:

@catch_error(logger)
def get_max_date(folder_path):
    max_date:str=None

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            name_data = extract_data_from_finra_file_name(file)
            if name_data and (not max_date or max_date<name_data['date']):
                max_date = name_data['date']

    print(f'Max Date: {max_date}')
    return max_date



# %% Get list of file names above certain date:

@catch_error(logger)
def get_files_list_date(folder_path, date_start:str, inclusive:bool=True):
    files_list = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            name_data = extract_data_from_finra_file_name(file)
            if name_data and (date_start<name_data['date'] or (date_start==name_data['date'] and inclusive)):
                files_list.append({
                    **name_data,
                    'root': root,
                    'file': file,
                })

    return files_list




# %% Process Single File

@catch_error(logger)
def process_one_file(root:str, file:str, firm_name:str, storage_account_name:str):
    file_path = os.path.join(root, file)
    print(f'\nProcessing {file_path}')

    name_data = extract_data_from_finra_file_name(file)
    if not name_data:
        print(f'Not valid file name format: {file_path}   -> SKIPPING')
        return

    if file.endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=root) as tmpdir:
            print(f'\nExtracting {file} to {tmpdir}')
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    process_one_file(root=root1, file=file1, firm_name=firm_name, storage_account_name=storage_account_name)
    else:
        process_finra_file(root=root, file=file, firm_name=firm_name, storage_account_name=storage_account_name)



# %% Process all files

@catch_error(logger)
def process_all_files():
    for firm in firms:
        firm_folder = firm['crd_number']
        folder_path = os.path.join(data_path_folder, firm_folder)
        firm_name = firm['firm_name']
        print(f"\n\nFirm: {firm_name}, Firm CRD Number: {firm['crd_number']}")

        if not os.path.isdir(folder_path):
            print(f'Path does not exist: {folder_path}   -> SKIPPING')
            continue

        storage_account_name = to_storage_account_name(firm_name=firm_name)
        setup_spark_adls_gen2_connection(spark, storage_account_name)

        max_date = get_max_date(folder_path=folder_path)

        if not max_date:
            print(f'Max Date not found, SKIPPING {folder_path}')
            continue

        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if max_date not in file:
                    print(f'Not Max Date {max_date}. Skipping file {os.path.join(root, file)}')
                    continue

                if is_pc and 'IndividualInformationReport' in file:
                    continue
                
                if not manual_iteration:
                    process_one_file(root=root, file=file, firm_name=firm_name, storage_account_name=storage_account_name)




process_all_files()


# %% Save Tableinfo

@catch_error(logger)
def save_tableinfo():
    tableinfo_values = list(tableinfo.values())

    list_of_dict = []
    for vi in range(len(tableinfo_values[0])):
        list_of_dict.append({k:v[vi] for k, v in tableinfo.items()})

    meta_tableinfo = spark.createDataFrame(list_of_dict)
    meta_tableinfo = select_tableinfo_columns(tableinfo=meta_tableinfo)

    storage_account_name = to_storage_account_name() # keep default storage account name for tableinfo
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    save_adls_gen2(
            table_to_save = meta_tableinfo,
            storage_account_name = storage_account_name,
            container_name = tableinfo_container_name,
            container_folder = tableinfo_source,
            table = tableinfo_name,
            partitionBy = partitionBy,
            file_format = file_format,
        )



save_tableinfo()


# %%


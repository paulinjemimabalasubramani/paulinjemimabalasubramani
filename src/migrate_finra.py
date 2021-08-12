"""
Flatten all Finra XML files and migrate them to ADLS Gen 2 

https://brokercheck.finra.org/individual/summary/3132991

Official Finra Schemas:
https://www.finra.org/filing-reporting/web-crd/web-eft-schema-documentation-and-schema-files

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""


# %% Import Libraries

import os, sys, tempfile, shutil, json, re
from collections import defaultdict
from datetime import datetime
from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_csv, read_xml, add_id_key, add_md5_key, IDKeyIndicator, MD5KeyIndicator
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, get_firms_with_crd, get_azure_sp
from modules.data_functions import  remove_column_spaces, add_elt_columns, execution_date, column_regex, partitionBy, \
    partitionBy_value, strftime
from modules.build_finra_tables import base_to_schema, build_branch_table, build_individual_table, flatten_df, flatten_n_divide_df


from pyspark.sql.functions import col, lit



# %% Logging
logger = make_logging(__name__)


# %% Parameters

save_xml_to_adls_flag = False
save_tableinfo_adls_flag = False
flatten_n_divide_flag = False

if not is_pc:
    save_xml_to_adls_flag = True
    save_tableinfo_adls_flag = True
    flatten_n_divide_flag = False

date_start = '2021-01-01'

domain_name = 'financial_professional'
database = 'FINRA'
tableinfo_source = database

sql_server = 'DSQLOLTP02'
sql_database = 'EDIPIngestion'
sql_schema = 'edip'
sql_key_vault_account = 'sqledipingestion'

FirmCRDNumber = 'Firm_CRD_Number'

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'
firmCRDNumber_name = '_firmCRDNumber'
DualRegistrations_name = 'DualRegistrations'



# %% Initiate Spark
spark = create_spark()


# %% Read Key Vault Data

# _, sql_id, sql_id = get_azure_sp(sql_key_vault_account.lower())

sql_id , sql_pass = 'svc_edipingestionapp', 'svc_edipingestionapp'  # TODO: Remove this once KV is ready.



# %% Get Paths

print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../../Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../config/{tableinfo_source}')
else:
    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../resources/fileshare/Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../resources/fileshare/EDIP-Code/config/{tableinfo_source}')



# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=tableinfo_source)

if is_pc: print(firms)



# %% Name extract

@catch_error(logger)
def extract_data_from_finra_file_name(file_name:str, crd_number:str):
    basename = os.path.basename(file_name)
    sp = basename.split("_")

    if basename.startswith("INDIVIDUAL_-_DUAL_REGISTRATIONS_-_FIRMS_DOWNLOAD_-_"):
        ans = {
            'crd_number': crd_number,
            'table_name': DualRegistrations_name,
            'date': datetime.strftime(datetime.strptime(execution_date, strftime), r'%Y-%m-%d')
        }
        return ans

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



# %% Get Meta Data from Finra XML File

@catch_error(logger)
def get_finra_file_xml_meta(file_path:str, crd_number:str):
    rowTag = '?xml'
    csv_flag = False

    file_meta = extract_data_from_finra_file_name(file_name=file_path, crd_number=crd_number)
    if not file_meta:
        return

    if file_meta['table_name'].upper() == DualRegistrations_name.upper():
        csv_flag = True
        xml_table = read_csv(spark=spark, file_path=file_path)
        criteria = {
            reportDate_name: file_meta['date'],
            firmCRDNumber_name: file_meta['crd_number'],
            }
    elif file_path.lower().endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            k = 0
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    file_path1 = os.path.join(root1, file1)
                    xml_table = read_xml(spark=spark, file_path=file_path1, rowTag=rowTag)
                    criteria = xml_table.select('Criteria.*').toJSON().map(lambda j: json.loads(j)).collect()[0]
                    k += 1
                    break
                if k>0: break
    else:
        xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag)
        criteria = xml_table.select('Criteria.*').toJSON().map(lambda j: json.loads(j)).collect()[0]

    if not criteria.get(reportDate_name):
        criteria[reportDate_name] = criteria['_postingDate']

    if criteria[firmCRDNumber_name] != file_meta['crd_number']:
        print(f"\n{file_path}\nFirm CRD Number in Criteria '{criteria[firmCRDNumber_name]}' does not match to the CRD Number in the file name '{file_meta['crd_number']}'\n")
        file_meta['crd_number'] = criteria[firmCRDNumber_name]

    if criteria[reportDate_name] != file_meta['date']:
        print(f"\n{file_path}\nReport/Posting Date in Criteria '{criteria[reportDate_name]}' does not match to the date in the file name '{file_meta['date']}'\n")
        file_meta['date'] = criteria[reportDate_name]

    rowTags = [c for c in xml_table.columns if c not in ['Criteria']]
    assert len(rowTags) == 1 or csv_flag, f"\n{file_path}\nXML File has rowTags {rowTags} is not valid\n"

    if file_meta['table_name'].upper() == 'IndividualInformationReportDelta'.upper():
        file_meta['table_name'] = 'IndividualInformationReport'

    file_meta['is_full_load'] = criteria.get('_IIRType') == 'FULL' or file_meta['table_name'].upper() in ['BranchInformationReport'.upper(), DualRegistrations_name.upper()]

    file_meta = {
        **file_meta,
        'root': os.path.dirname(file_path),
        'file': os.path.basename(file_path),
        'criteria': criteria,
        'rowTags': rowTags,
    }

    return file_meta




# %% Get list of file names above certain date:

@catch_error(logger)
def get_all_finra_file_xml_meta(folder_path, date_start:str, crd_number:str, inclusive:bool=True):
    print(f'\nGetting list of candidate files from {folder_path}')
    files_meta = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_meta = get_finra_file_xml_meta(file_path=file_path, crd_number=crd_number)

            if file_meta and (date_start<file_meta['date'] or (date_start==file_meta['date'] and inclusive)):
                files_meta.append(file_meta)

    print(f'Finished getting list of files. Total Files = {len(files_meta)}\n')
    return files_meta





# %% Create tableinfo

tableinfo = defaultdict(list)

@catch_error(logger)
def add_table_to_tableinfo(xml_table, firm_name, table_name):
    for ix, (col_name, col_type) in enumerate(xml_table.dtypes):
        var_col_type = 'variant' if ':' in col_type else col_type

        tableinfo['SourceDatabase'].append(database)
        tableinfo['SourceSchema'].append(firm_name)
        tableinfo['TableName'].append(table_name)
        tableinfo['SourceColumnName'].append(col_name)
        tableinfo['SourceDataType'].append(var_col_type)
        tableinfo['SourceDataLength'].append(0)
        tableinfo['SourceDataPrecision'].append(0)
        tableinfo['SourceDataScale'].append(0)
        tableinfo['OrdinalPosition'].append(ix+1)
        tableinfo['CleanType'].append(var_col_type)
        tableinfo['TargetColumnName'].append(re.sub(column_regex, '_', col_name))
        tableinfo['TargetDataType'].append(var_col_type)
        tableinfo['IsNullable'].append(0 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 1)
        tableinfo['KeyIndicator'].append(1 if col_name.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()] else 0)
        tableinfo['IsActive'].append(1)
        tableinfo['CreatedDateTime'].append(execution_date)
        tableinfo['ModifiedDateTime'].append(execution_date)
        tableinfo[partitionBy].append(partitionBy_value)



# %% Write xml table list to Azure

@catch_error(logger)
def write_xml_table_list_to_azure(xml_table_list:dict, file_name:str, reception_date:str, firm_name:str, storage_account_name:str, is_full_load:bool, crd_number:str):
    if not xml_table_list:
        print(f"No data to write -> {file_name}")
        return

    for table_name, xml_table in xml_table_list.items():
        print(f'\nWriting {table_name} to Azure...')

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        xml_table1 = xml_table
        xml_table1 = remove_column_spaces(table_to_remove = xml_table1)
        xml_table1 = xml_table1.withColumn('FILE_DATE', lit(str(reception_date)))
        xml_table1 = xml_table1.withColumn(FirmCRDNumber, lit(str(crd_number)))

        if table_name.upper() == 'IndividualInformationReport'.upper():
            xml_table1 = add_id_key(xml_table1, key_column_names=['FIRM_CRD_NUMBER', 'CRD_NUMBER'])
        elif table_name.upper() == 'BranchInformationReport'.upper():
            xml_table1 = add_id_key(xml_table1, key_column_names=['FIRM_CRD_NUMBER', 'BRANCH_CRD_NUMBER'])
        else:
            xml_table1 = add_md5_key(xml_table1)

        add_table_to_tableinfo(xml_table=xml_table1, firm_name=firm_name, table_name=table_name)

        xml_table1 = add_elt_columns(
            table_to_add = xml_table1,
            reception_date = reception_date,
            source = tableinfo_source,
            is_full_load = is_full_load,
            dml_type = 'I' if is_full_load or firm_name.upper() not in ['IndividualInformationReport'.upper()] else 'U',
            )

        if is_pc: xml_table1.printSchema()

        if is_pc: # and manual_iteration:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{table_name}'
            print(fr'Save to local {local_path}')
            #xml_table1.coalesce(1).write.csv( path = fr'{local_path}.csv',  mode='overwrite', header='true')
            xml_table1.coalesce(1).write.json(path = fr'{local_path}.json', mode='overwrite')

        if save_xml_to_adls_flag:
            save_adls_gen2(
                table_to_save = xml_table1,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table_name = table_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

    print('Done writing to Azure')






# %% Main Processing of Finra file

@catch_error(logger)
def process_finra_file(file_meta, firm_name:str, storage_account_name:str):
    file_path = os.path.join(file_meta['root'], file_meta['file'])

    rowTags = file_meta['rowTags']
    table_name = file_meta['table_name']
    crd_number = file_meta['crd_number']

    print('\n', crd_number, table_name, file_meta['date'])
    print(f'\nrowTags: {rowTags}\n')
    rowTag = rowTags[0]

    schema_file = table_name+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if table_name.upper() == DualRegistrations_name.upper():
        xml_table = read_csv(spark=spark, file_path=file_path)
    elif os.path.isfile(schema_path):
        print(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag, schema=schema)
    else:
        print(f"Looking for Schema File Location: {schema_path}")
        print(f"No manual schema defined for {table_name}. Using default schema.")
        xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag)

    if is_pc: xml_table.printSchema()
    xml_table_list = {}

    if flatten_n_divide_flag:
        xml_table_list={**xml_table_list, **flatten_n_divide_df(xml_table=xml_table, table_name=table_name)}

    semi_flat_table = flatten_df(xml_table=xml_table)

    if table_name.upper() == 'BranchInformationReport'.upper():
        xml_table_list={**xml_table_list, table_name: build_branch_table(semi_flat_table=semi_flat_table)}
    elif table_name.upper() == 'IndividualInformationReport'.upper():
        xml_table_list={**xml_table_list, table_name: build_individual_table(semi_flat_table=semi_flat_table, crd_number=crd_number)}
    else:
        xml_table_list={**xml_table_list, table_name: semi_flat_table}


    write_xml_table_list_to_azure(
        xml_table_list= xml_table_list,
        file_name = file_meta['file'],
        reception_date = file_meta['date'],
        firm_name = firm_name,
        storage_account_name = storage_account_name,
        is_full_load = file_meta['is_full_load'],
        crd_number = crd_number,
        )

    return semi_flat_table




# %% Process Single File

@catch_error(logger)
def process_one_file(file_meta, firm_name:str, storage_account_name:str):
    file_path = os.path.join(file_meta['root'], file_meta['file'])
    print(f'\nProcessing {file_path}')

    if file_path.lower().endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
            print(f'\nExtracting {file_path} to {tmpdir}')
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            k = 0
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    file_meta1 = json.loads(json.dumps(file_meta))
                    file_meta1['root'] = root1
                    file_meta1['file'] = file1
                    process_finra_file(file_meta=file_meta1, firm_name=firm_name, storage_account_name=storage_account_name)
                    k += 1
                    break
                if k>0: break
    else:
        process_finra_file(file_meta=file_meta, firm_name=firm_name, storage_account_name=storage_account_name)



# %% Testing

if is_pc and False:
    folder_path = r'C:\Users\smammadov\packages\Shared\test'
    files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start, crd_number='x')
    file_meta = files_meta[0]
    print(file_meta)
    semi_flat_table = process_finra_file(file_meta, firm_name='x', storage_account_name='x')


# %% Testing 2


# individual = build_individual_table(semi_flat_table=semi_flat_table, crd_number='7461')


# individual = individual.persist()
# pprint(individual.where('CRD_Number = 3044031').toJSON().map(lambda j: json.loads(j)).collect())




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

        files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start, crd_number=firm['crd_number'])

        for file_meta in files_meta:
            if is_pc and 'IndividualInformationReport'.upper() in file_meta['table_name'].upper():
                continue
            process_one_file(file_meta=file_meta, firm_name=firm_name, storage_account_name=storage_account_name)



process_all_files()


# %% Save Tableinfo

@catch_error(logger)
def save_tableinfo():
    if not tableinfo:
        print('No data in TableInfo --> Skipping write to Azure')
        return
        
    tableinfo_values = list(tableinfo.values())

    list_of_dict = []
    for vi in range(len(tableinfo_values[0])):
        list_of_dict.append({k:v[vi] for k, v in tableinfo.items()})

    meta_tableinfo = spark.createDataFrame(list_of_dict)
    meta_tableinfo = select_tableinfo_columns(tableinfo=meta_tableinfo)

    storage_account_name = to_storage_account_name() # keep default storage account name for tableinfo
    setup_spark_adls_gen2_connection(spark, storage_account_name)

    if save_tableinfo_adls_flag:
        save_adls_gen2(
                table_to_save = meta_tableinfo,
                storage_account_name = storage_account_name,
                container_name = tableinfo_container_name,
                container_folder = tableinfo_source,
                table_name = tableinfo_name,
                partitionBy = partitionBy,
                file_format = file_format,
            )



save_tableinfo()


# %%


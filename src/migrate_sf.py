"""
Read CSV files and migrate them to ADLS Gen 2


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
from modules.spark_functions import create_spark, read_csv
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, read_adls_gen2
from modules.data_functions import  to_string, remove_column_spaces, add_elt_columns, execution_date, column_regex, partitionBy, \
    metadata_FirmSourceMap, partitionBy_value, strftime


from pyspark.sql.functions import col, lit, md5, concat_ws, monotonically_increasing_id



# %% Logging
logger = make_logging(__name__)


# %% Parameters

save_csv_to_adls_flag = True
save_tableinfo_adls_flag = True


if not is_pc:
    save_csv_to_adls_flag = True
    save_tableinfo_adls_flag = True

date_start = '2021-01-01'

domain_name = 'financial_professional'
database = 'SF'
tableinfo_source = database

MD5KeyIndicator = 'MD5_KEY'
IDKeyIndicator = 'ID'
FirmCRDNumber = 'Firm_CRD_Number'




# %% Initiate Spark
spark = create_spark()


# %% Get Paths

print(f'Main Path: {os.path.realpath(os.path.dirname(__file__))}')

if is_pc:
    data_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../../Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../config/{tableinfo_source}')
else:
    # /usr/local/spark/resources/fileshare/Shared
    data_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../resources/fileshare/Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(os.path.dirname(__file__) + f'/../resources/fileshare/EDIP-Code/config/{tableinfo_source}')



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
        (col('Source') == lit('FINRA'.upper()).cast("string")) & 
        (col('IsActive') == lit(1))
    )

    firms_table = firms_table.select('Firm', 'SourceKey') \
        .withColumnRenamed('Firm', 'firm_name') \
        .withColumnRenamed('SourceKey', 'crd_number')

    firms = firms_table.toJSON().map(lambda j: json.loads(j)).collect()

    assert firms, 'No Firms Found!'
    return firms



firms = get_firms()

if is_pc: print(firms)



# %% Get Meta Data from CSV File

@catch_error(logger)
def get_csv_file_meta(file_path:str, crd_number:str):

    if not file_path.lower().endswith('.csv'):
        print(f'Not CSV file type: {file_path}')
        return

    file_name_with_ext = os.path.basename(file_path)

    file_meta = {
        'crd_number': crd_number,
        'table_name': os.path.splitext(file_name_with_ext)[0].lower(),
        'date': datetime.strftime(datetime.strptime(execution_date, strftime), r'%Y-%m-%d'),
        'is_full_load': True,
        'root': os.path.dirname(file_path),
        'file': file_name_with_ext,
    }

    return file_meta




# %% Get list of file names above certain date:

@catch_error(logger)
def get_all_csv_files_meta(folder_path, date_start:str, crd_number:str, inclusive:bool=True):
    print(f'\nGetting list of candidate files from {folder_path}')
    files_meta = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_meta = get_csv_file_meta(file_path=file_path, crd_number=crd_number)

            if file_meta and (date_start<file_meta['date'] or (date_start==file_meta['date'] and inclusive)):
                files_meta.append(file_meta)

    print(f'Finished getting list of files. Total Files = {len(files_meta)}\n')
    return files_meta




# %% Create tableinfo

tableinfo = defaultdict(list)

@catch_error(logger)
def add_table_to_tableinfo(csv_table, firm_name, table_name):
    for ix, (col_name, col_type) in enumerate(csv_table.dtypes):
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



# %% Write CSV table list to Azure

@catch_error(logger)
def write_csv_table_list_to_azure(csv_table_list:dict, file_name:str, reception_date:str, firm_name:str, storage_account_name:str, is_full_load:bool, crd_number:str):
    if not csv_table_list:
        print(f"No data to write -> {file_name}")
        return
    monid = 'monotonically_increasing_id'

    for table_name, csv_table in csv_table_list.items():
        print(f'\nWriting {table_name} to Azure...')

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        table1 = csv_table
        table1 = remove_column_spaces(table_to_remove = table1)
        table1 = table1.withColumn('FILE_DATE', lit(str(reception_date)))
        table1 = table1.withColumn(FirmCRDNumber, lit(str(crd_number)))

        id_columns = [c for c in table1.columns if c.upper() in [IDKeyIndicator.upper()]]
        if not id_columns:
            table1 = table1.withColumn(monid, monotonically_increasing_id().cast('string'))
            table2 = to_string(table_to_convert_columns = table1, col_types = []) # Convert all columns to string
            md5_columns = [c for c in table2.columns if c not in [monid]]
            table2 = table2.withColumn(MD5KeyIndicator, md5(concat_ws('_', *md5_columns))) # add HASH column for key indicator

            table1 = table1.alias('x1'
                ).join(table2.alias('x2'), table1[monid]==table2[monid], how='left'
                ).select('x1.*', 'x2.'+MD5KeyIndicator
                ).drop(monid)

        add_table_to_tableinfo(csv_table=table1, firm_name=firm_name, table_name=table_name)

        table1 = add_elt_columns(
            table_to_add = table1,
            reception_date = reception_date,
            source = tableinfo_source,
            is_full_load = is_full_load,
            dml_type = 'I' if is_full_load or firm_name.upper() not in ['IndividualInformationReport'.upper()] else 'U',
            )

        if is_pc: table1.printSchema()

        if is_pc and True:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{table_name}'
            print(fr'Save to local {local_path}')
            table1.coalesce(1).write.csv( path = fr'{local_path}.csv',  mode='overwrite', header='true')

        if save_csv_to_adls_flag:
            save_adls_gen2(
                table_to_save = table1,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table = table_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

    print('Done writing to Azure')




# %% Main Processing of CSV file

@catch_error(logger)
def process_csv_file(file_meta, firm_name:str, storage_account_name:str):
    file_path = os.path.join(file_meta['root'], file_meta['file'])

    table_name = file_meta['table_name']
    crd_number = file_meta['crd_number']

    print('\n', crd_number, table_name, file_meta['date'])

    csv_table = read_csv(spark=spark, file_path=file_path)

    if is_pc: csv_table.printSchema()

    table_list = {table_name: csv_table}

    write_csv_table_list_to_azure(
        csv_table_list= table_list,
        file_name = file_meta['file'],
        reception_date = file_meta['date'],
        firm_name = firm_name,
        storage_account_name = storage_account_name,
        is_full_load = file_meta['is_full_load'],
        crd_number = crd_number,
        )

    return csv_table




# %% Process Single File

@catch_error(logger)
def process_one_file(file_meta, firm_name:str, storage_account_name:str):
    file_path = os.path.join(file_meta['root'], file_meta['file'])
    print(f'\nProcessing {file_path}')

    if file_path.endswith('.zip'):
        with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
            print(f'\nExtracting {file_path} to {tmpdir}')
            shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
            k = 0
            for root1, dirs1, files1 in os.walk(tmpdir):
                for file1 in files1:
                    file_meta1 = json.loads(json.dumps(file_meta))
                    file_meta1['root'] = root1
                    file_meta1['file'] = file1
                    process_csv_file(file_meta=file_meta1, firm_name=firm_name, storage_account_name=storage_account_name)
                    k += 1
                    break
                if k>0: break
    else:
        process_csv_file(file_meta=file_meta, firm_name=firm_name, storage_account_name=storage_account_name)



# %% Testing

if is_pc and False:
    folder_path = r'C:\Users\smammadov\packages\Shared\test'
    files_meta = get_all_csv_files_meta(folder_path=folder_path, date_start=date_start, crd_number='x')
    file_meta = files_meta[0]
    print(file_meta)
    csv_table = process_csv_file(file_meta, firm_name='x', storage_account_name='x')


# %% Testing 2




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

        files_meta = get_all_csv_files_meta(folder_path=folder_path, date_start=date_start, crd_number=firm['crd_number'])

        for file_meta in files_meta:
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
                table = tableinfo_name,
                partitionBy = partitionBy,
                file_format = file_format,
            )



save_tableinfo()


# %%


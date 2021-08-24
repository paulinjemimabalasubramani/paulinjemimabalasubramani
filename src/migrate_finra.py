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

import os, sys, tempfile, shutil, json, copy
from collections import defaultdict
from datetime import datetime
from pprint import pprint


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import make_logging, catch_error
from modules.spark_functions import create_spark, read_sql, write_sql, read_csv, read_xml, add_id_key, add_md5_key, \
    IDKeyIndicator, MD5KeyIndicator, get_sql_table_names
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, get_firms_with_crd, get_azure_sp, add_table_to_tableinfo
from modules.data_functions import  remove_column_spaces, add_elt_columns, execution_date, partitionBy, strftime
from modules.build_finra_tables import base_to_schema, build_branch_table, build_individual_table, flatten_df, flatten_n_divide_df


import pyspark.sql.functions as F
from pyspark.sql.functions import col, lit, to_date, to_json, to_timestamp, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, BooleanType


# %% Logging
logger = make_logging(__name__)


# %% Parameters

save_xml_to_adls_flag = True
save_tableinfo_adls_flag = True
flatten_n_divide_flag = False # Always keep it False

if not is_pc:
    save_xml_to_adls_flag = True
    save_tableinfo_adls_flag = True
    flatten_n_divide_flag = False # Always keep it False

date_start = '1990-01-01'

domain_name = 'financial_professional'
database = 'FINRA'
tableinfo_source = database

sql_server = 'DSQLOLTP02'
sql_database = 'EDIPIngestion'
sql_schema = 'edip'
sql_ingest_table_name = f'{tableinfo_source.lower()}_ingest'
sql_key_vault_account = 'sqledipingestion'

FirmCRDNumber = 'Firm_CRD_Number'

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'
firmCRDNumber_name = '_firmCRDNumber'
DualRegistrations_name = 'DualRegistrations'

key_column_names = ['crd_number', 'table_name']
key_column_names_with_load = key_column_names + ['is_full_load']
key_column_names_with_load_n_date = key_column_names_with_load + ['file_date']

tmpdirs = []



# %% Initiate Spark
spark = create_spark()


# %% Read Key Vault Data

_, sql_id, sql_pass = get_azure_sp(sql_key_vault_account.lower())



# %% Create tableinfo

tableinfo = defaultdict(list)



# %% Get Paths

python_dirname = os.path.dirname(__file__)
print(f'Main Path: {os.path.realpath(python_dirname)}')

if is_pc:
    data_path_folder = os.path.realpath(python_dirname + f'/../../Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(python_dirname + f'/../config/finra_schema')
else:
    # /usr/local/spark/resources/fileshare/
    data_path_folder = os.path.realpath(python_dirname + f'/../resources/fileshare/Shared/{tableinfo_source}')
    schema_path_folder = os.path.realpath(python_dirname + f'/../resources/fileshare/EDIP-Code/config/finra_schema')



# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=tableinfo_source)

if is_pc: pprint(firms)



# %% Get SQL Ingest Table

table_names, sql_tables = get_sql_table_names(
    spark = spark, 
    schema = sql_schema,
    database = sql_database,
    server = sql_server,
    user = sql_id,
    password = sql_pass,
    )

sql_ingest_table_exists = sql_ingest_table_name in table_names

sql_ingest_table = None
if sql_ingest_table_exists:
    sql_ingest_table = read_sql(
        spark = spark, 
        user = sql_id, 
        password = sql_pass, 
        schema = sql_schema, 
        table_name = sql_ingest_table_name, 
        database = sql_database, 
        server = sql_server
        )





# %% Name extract

@catch_error(logger)
def extract_data_from_finra_file_path(file_path:str, crd_number:str):
    basename = os.path.basename(file_path)
    dirname = os.path.dirname(file_path)
    sp = basename.split("_")

    ans = {
        'file': basename,
        'root': dirname,
        'date_modified': datetime.fromtimestamp(os.path.getmtime(file_path)),
    }

    if basename.startswith("INDIVIDUAL_-_DUAL_REGISTRATIONS_-_FIRMS_DOWNLOAD_-_"):
        ans = {**ans,
            'crd_number': crd_number,
            'table_name': DualRegistrations_name,
            'date': datetime.strftime(datetime.strptime(execution_date, strftime), r'%Y-%m-%d'),
        }
        return ans

    try:
        ans = {**ans,
            'crd_number': sp[0],
            'table_name': sp[1],
            'date': sp[2].rsplit('.', 1)[0],
        }

        if ans['table_name'].upper() == 'IndividualInformationReportDelta'.upper():
            ans['table_name'] = 'IndividualInformationReport'

        _ = datetime.strptime(ans['date'], r'%Y-%m-%d')
        assert len(sp)==3 or (len(sp)==4 and sp[1].upper()==finra_individual_delta_name.upper())
    except:
        print(f'Cannot parse file name: {file_path}')
        return

    return ans



# %% Get Meta Data from Finra XML File

@catch_error(logger)
def get_finra_file_xml_meta(file_path:str, crd_number:str):
    global sql_ingest_table_exists, sql_ingest_table
    rowTag = '?xml'
    csv_flag = False

    file_meta = extract_data_from_finra_file_path(file_path=file_path, crd_number=crd_number)
    if not file_meta:
        return

    fcol = None
    if sql_ingest_table_exists:
        filtersql = sql_ingest_table.where(
            (col('crd_number')==lit(file_meta['crd_number'])) &
            (col('table_name')==lit(file_meta['table_name'])) &
            (col('file_date')==to_date(lit(file_meta['date']), format='yyyy-MM-dd')) &
            (col('file_name')==lit(file_meta['file'])) &
            (col('root_folder')==lit(file_meta['root']))
            )
        if filtersql.count()>0:
            fcol = filtersql.limit(1).collect()[0]
            criteria = json.loads(fcol['xml_criteria'])
            rowTags = json.loads(fcol['xml_rowtags'])

    if not fcol:
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

    file_meta['is_full_load'] = criteria.get('_IIRType') == 'FULL' or file_meta['table_name'].upper() in ['BranchInformationReport'.upper(), DualRegistrations_name.upper()]

    file_meta = {
        **file_meta,
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




# %% Write xml table list to Azure

@catch_error(logger)
def write_xml_table_list_to_azure(xml_table_list:dict, firm_name:str, storage_account_name:str):
    if not xml_table_list:
        print(f"\nNo data to write\n")
        return

    for table_name, xml_table in xml_table_list.items():
        print(f'\nWriting {table_name} to Azure...')

        add_table_to_tableinfo(
            tableinfo = tableinfo, 
            table = xml_table, 
            firm_name = firm_name, 
            table_name = table_name, 
            tableinfo_source = tableinfo_source, 
            storage_account_name = storage_account_name,
            )

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        if is_pc: # and manual_iteration:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{table_name}'
            print(fr'Save to local {local_path}')
            xml_table.coalesce(1).write.json(path = fr'{local_path}.json', mode='overwrite')

        if save_xml_to_adls_flag:
            save_adls_gen2(
                table_to_save = xml_table,
                storage_account_name = storage_account_name,
                container_name = container_name,
                container_folder = container_folder,
                table_name = table_name,
                partitionBy = partitionBy,
                file_format = file_format
            )

    print('Done writing to Azure')



# %% Read XML File

@catch_error(logger)
def read_xml_file(table_name:str, file_path:str, rowTag:str):
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
    return xml_table



# %% Get XML Table List

@catch_error(logger)
def get_xml_table_list(xml_table, table_name:str, crd_number:str):
    xml_table_list = {}

    if flatten_n_divide_flag:
        xml_table_list={**xml_table_list, **flatten_n_divide_df(xml_table=xml_table, table_name=table_name)}

    semi_flat_table = flatten_df(xml_table=xml_table)

    if table_name.upper() == 'BranchInformationReport'.upper():
        xml_table_list={**xml_table_list, table_name: build_branch_table(semi_flat_table=semi_flat_table)}
    elif table_name.upper() == 'IndividualInformationReport'.upper():
        xml_table_list = {**xml_table_list, table_name: build_individual_table(semi_flat_table=semi_flat_table, crd_number=crd_number)}
    else:
        xml_table_list = {**xml_table_list, table_name: semi_flat_table}
    
    return xml_table_list



# %% Process Columns for ELT

@catch_error(logger)
def process_columns_for_elt(table_list, file_meta):
    new_table_list = {}

    for table_name, table in table_list.items():
        table1 = table
        table1 = remove_column_spaces(table_to_remove = table1)
        table1 = table1.withColumn('FILE_DATE', lit(str(file_meta['date'])))
        table1 = table1.withColumn(FirmCRDNumber, lit(str(file_meta['crd_number'])))

        if table_name.upper() == 'IndividualInformationReport'.upper():
            table1 = add_id_key(table1, key_column_names=['FIRM_CRD_NUMBER', 'CRD_NUMBER'])
        elif table_name.upper() == 'BranchInformationReport'.upper():
            table1 = add_id_key(table1, key_column_names=['FIRM_CRD_NUMBER', 'BRANCH_CRD_NUMBER'])
        else:
            table1 = add_md5_key(table1)

        table1 = add_elt_columns(
            table_to_add = table1,
            reception_date = file_meta['date'],
            source = tableinfo_source,
            is_full_load = file_meta['is_full_load'],
            dml_type = 'I' if file_meta['is_full_load'] or file_meta['table_name'].upper() not in ['IndividualInformationReport'.upper()] else 'U',
            )

        new_table_list = {**new_table_list, table_name: table1}

    return new_table_list




# %% Main Processing of Finra file

@catch_error(logger)
def process_finra_file(file_meta):
    file_path = os.path.join(file_meta['root'], file_meta['file'])

    print('\n', file_meta['crd_number'], file_meta['table_name'], file_meta['date'])

    rowTags = file_meta['rowTags']
    print(f'\nrowTags: {rowTags}\n')

    xml_table = read_xml_file(table_name=file_meta['table_name'], file_path=file_path, rowTag=rowTags[0])
    xml_table_list = get_xml_table_list(xml_table=xml_table, table_name=file_meta['table_name'], crd_number=file_meta['crd_number'])
    xml_table_list = process_columns_for_elt(table_list=xml_table_list, file_meta=file_meta)

    return xml_table_list




# %% Process Single File

@catch_error(logger)
def process_one_file(file_meta):
    global tmpdirs
    file_path = os.path.join(file_meta['root'], file_meta['file'])
    print(f'\nProcessing {file_path}')

    if file_path.lower().endswith('.zip'):
        tmpdir = tempfile.TemporaryDirectory(dir=os.path.dirname(file_path))
        tmpdirs.append(tmpdir)
        print(f'\nExtracting {file_path} to {tmpdir.name}')
        shutil.unpack_archive(filename=file_path, extract_dir=tmpdir.name)
        for root1, dirs1, files1 in os.walk(tmpdir.name):
            for file1 in files1:
                file_meta1 = copy.deepcopy(file_meta)
                file_meta1['root'] = root1
                file_meta1['file'] = file1
                return process_finra_file(file_meta=file_meta1)
    else:
        return process_finra_file(file_meta=file_meta)



# %% Remove temporary folders

@catch_error(logger)
def cleanup_tmpdirs():
    global tmpdirs
    while len(tmpdirs)>0:
        tmpdirs.pop(0).cleanup()




# %% Create Ingest Table from files_meta

@catch_error(logger)
def ingest_table_from_files_meta(files_meta, firm_name:str, storage_account_name:str):
    global sql_ingest_table_exists, sql_ingest_table

    ingest_table = spark.createDataFrame(files_meta)
    ingest_table = ingest_table.select(
        col('crd_number').cast(StringType()),
        col('table_name').cast(StringType()),
        to_date(col('date'), format='yyyy-MM-dd').alias('file_date'),
        col('is_full_load').cast(BooleanType()),
        lit(firm_name).cast(StringType()).alias('firm_name'),
        lit(storage_account_name).cast(StringType()).alias('storage_account_name'),
        lit(None).cast(StringType()).alias('remote_source'),
        col('root').cast(StringType()).alias('root_folder'),
        col('file').cast(StringType()).alias('file_name'),
        to_json(col('criteria')).alias('xml_criteria'),
        to_json(col('rowTags')).alias('xml_rowtags'),
        to_timestamp(lit(None)).alias('ingestion_date'), # execution_date
        lit(True).cast(BooleanType()).alias('is_ingested'),
        to_timestamp(lit(None)).alias('full_load_date'),
        to_timestamp(col('date_modified')).alias('date_modified'),
    )

    ingest_table = add_md5_key(ingest_table, key_column_names=key_column_names_with_load_n_date)

    if not sql_ingest_table_exists:
        sql_ingest_table_exists = True
        sql_ingest_table = spark.createDataFrame(spark.sparkContext.emptyRDD(), ingest_table.schema)
        write_sql(
            table = sql_ingest_table,
            table_name = sql_ingest_table_name,
            schema = sql_schema,
            database = sql_database,
            server = sql_server,
            user = sql_id,
            password = sql_pass,
            mode = 'overwrite',
        )

    return ingest_table



# %% Get Seletec Files

@catch_error(logger)
def get_selected_files(ingest_table):
    # Union all files
    new_files = ingest_table.alias('t'
        ).join(sql_ingest_table, ingest_table[MD5KeyIndicator]==sql_ingest_table[MD5KeyIndicator], how='left_anti'
        ).select('t.*')

    union_columns = new_files.columns
    all_files = new_files.select(union_columns).union(sql_ingest_table.select(union_columns)).sort(key_column_names_with_load_n_date)

    # Filter out old files
    full_files = all_files.where('is_full_load').withColumn('row_number', 
            row_number().over(Window.partitionBy(key_column_names_with_load).orderBy(col('file_date').desc(), col(MD5KeyIndicator).desc()))
        ).where(col('row_number')==lit(1))

    all_files = all_files.alias('a').join(full_files.alias('f'), key_column_names, how='left').select('a.*', col('f.file_date').alias('max_date')) \
        .withColumn('full_load_date', col('max_date')).drop('max_date')

    all_files = all_files.withColumn('is_ingested', 
            when(col('ingestion_date').isNull() & col('full_load_date').isNotNull() & (col('file_date')<col('full_load_date')), lit(False)).otherwise(col('is_ingested'))
        )

    # Select and Order Files for ingestion
    new_files = all_files.where(col('ingestion_date').isNull()).withColumn('ingestion_date', to_timestamp(lit(execution_date)))
    selected_files = new_files.where('is_ingested').orderBy(col('crd_number').desc(), col('table_name').desc(), col('is_full_load').desc(), col('file_date').asc())

    return new_files, selected_files



# %% Testing

if is_pc and False:
    crd_number = '25803'
    firm = [f for f in firms if f['crd_number']==crd_number][0]




# %% Process all files

@catch_error(logger)
def process_all_files():
    all_new_files = None

    for firm in firms: # Assumes each firm has different Firm CRD Number
        folder_path = os.path.join(data_path_folder, firm['crd_number'])
        print(f"\n\nFirm: {firm['firm_name']}, Firm CRD Number: {firm['crd_number']}")

        if not os.path.isdir(folder_path):
            print(f'Path does not exist: {folder_path}   -> SKIPPING')
            continue

        files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start, crd_number=firm['crd_number'])
        if not files_meta:
            continue

        storage_account_name = to_storage_account_name(firm_name=firm['storage_account_name'])
        setup_spark_adls_gen2_connection(spark, storage_account_name)

        print('Getting New Files')
        ingest_table = ingest_table_from_files_meta(files_meta, firm_name=firm['firm_name'], storage_account_name=storage_account_name)
        new_files, selected_files = get_selected_files(ingest_table)

        print(f'Total of {new_files.count()} new file(s). {selected_files.count()} eligible for data migration.')

        if all_new_files:
            union_columns = new_files.columns
            all_new_files = all_new_files.select(union_columns).union(new_files.select(union_columns))
        else:
            all_new_files = new_files

        print("Iterating over Selected Files")
        xml_table_list_union = {}
        for ingest_row in selected_files.rdd.toLocalIterator():
            file_meta = ingest_row.asDict()
            file_meta['criteria'] = json.loads(file_meta['xml_criteria'])
            file_meta['rowTags'] = json.loads(file_meta['xml_rowtags'])
            file_meta['date'] = datetime.strftime(file_meta['file_date'], r'%Y-%m-%d')
            file_meta['root'] = file_meta['root_folder']
            file_meta['file'] = file_meta['file_name']

            if is_pc and False and 'IndividualInformationReport'.upper() in file_meta['table_name'].upper():
                print(f"\nSkipping {file_meta['table_name']} in PC\n")
                continue

            xml_table_list = process_one_file(file_meta=file_meta)

            for table_name, table in xml_table_list.items():
                if table_name in xml_table_list_union.keys():
                    table_prev = xml_table_list_union[table_name]
                    primary_key_columns = [c for c in table_prev.columns if c.upper() in [MD5KeyIndicator.upper(), IDKeyIndicator.upper()]]
                    if not primary_key_columns:
                        raise ValueError(f'No Primary Key Found for {table_name}')
                    table_prev = table_prev.alias('tp'
                        ).join(table, primary_key_columns, how='left_anti'
                        ).select('tp.*')
                    union_columns = table_prev.columns
                    table_prev = table_prev.select(union_columns).union(table.select(union_columns))
                    xml_table_list_union[table_name] = table_prev
                else:
                    xml_table_list_union[table_name] = table

        write_xml_table_list_to_azure(
            xml_table_list = xml_table_list_union,
            firm_name = firm['firm_name'],
            storage_account_name = storage_account_name,
        )
        
        cleanup_tmpdirs()

    print('\nFinished processing all Files and Firms\n')
    return all_new_files


all_new_files = process_all_files()




# %% Save Tableinfo

@catch_error(logger)
def save_tableinfo(all_new_files):
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

        if all_new_files:
            write_sql(
                table = all_new_files,
                table_name = sql_ingest_table_name,
                schema = sql_schema,
                database = sql_database,
                server = sql_server,
                user = sql_id,
                password = sql_pass,
                mode = 'append',
            )



save_tableinfo(all_new_files)


# %%


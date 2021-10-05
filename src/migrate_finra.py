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

import os, sys, json, tempfile, shutil
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'financial_professional'
sys.domain_abbr = 'FP'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import logger, catch_error, is_pc, data_settings, config_path, execution_date, strftime, mark_execution_end
from modules.spark_functions import create_spark, read_csv, read_xml, add_id_key, add_md5_key, remove_column_spaces, add_elt_columns, \
    flatten_table, flatten_n_divide_table, table_to_list_dict, base_to_schema
from modules.azure_functions import tableinfo_name, get_firms_with_crd, read_tableinfo_rows

from modules.build_finra_tables import build_branch_table, build_individual_table
from modules.snowflake_ddl import connect_to_snowflake, iterate_over_all_tables_snowflake, create_source_level_tables, snowflake_ddl_params
from modules.migrate_files import save_tableinfo_dict_and_sql_ingest_table, process_all_files_with_incrementals, FirmCRDNumber, \
    get_key_column_names


from pprint import pprint
from datetime import datetime

from pyspark.sql.functions import col, lit, to_date, to_json



# %% Parameters

save_finra_to_adls_flag = True
save_tableinfo_adls_flag = True
flatten_n_divide_flag = False # Always keep it False

tableinfo_source = 'FINRA'

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
schema_path_folder = os.path.join(config_path, 'finra_schema')

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'
firmCRDNumber_name = '_firmCRDNumber'
DualRegistrations_name = 'DualRegistrations'

date_start = '1990-01-01'

date_column_name = 'file_date'
key_column_names = get_key_column_names(date_column_name=date_column_name)

logger.info({
    'tableinfo_source': tableinfo_source,
    'data_path_folder': data_path_folder,
    'schema_path_folder': schema_path_folder,
})



# %% Create Spark Session

spark = create_spark()
snowflake_ddl_params.spark = spark


# %% Get Firms that have CRD Number

firms = get_firms_with_crd(spark=spark, tableinfo_source=tableinfo_source)

if is_pc: pprint(firms)



# %% Extract metadata from finra file name and path

@catch_error(logger)
def extract_data_from_finra_file_path(file_path:str, firm_crd_number:str):
    """"
    Extract metadata from finra file name and path
    """
    basename = os.path.basename(file_path)
    dirname = os.path.dirname(file_path)
    sp = basename.split("_")

    ans = {
        'file_name': basename,
        'file_path': file_path,
        'folder_path': dirname,
        'date_file_modified': datetime.fromtimestamp(os.path.getmtime(file_path)),
    }

    if basename.startswith("INDIVIDUAL_-_DUAL_REGISTRATIONS_-_FIRMS_DOWNLOAD_-_"):
        ans = {**ans,
            FirmCRDNumber: firm_crd_number,
            'table_name': DualRegistrations_name.lower(),
            date_column_name: datetime.strftime(datetime.strptime(execution_date, strftime), r'%Y-%m-%d'),
        }
        return ans

    try:
        ans = {**ans,
            FirmCRDNumber: sp[0],
            'table_name': sp[1],
            date_column_name: sp[2].rsplit('.', 1)[0],
        }

        if ans['table_name'].upper() == 'IndividualInformationReportDelta'.upper():
            ans['table_name'] = 'IndividualInformationReport'

        _ = datetime.strptime(ans[date_column_name], r'%Y-%m-%d')
        assert len(sp)==3 or (len(sp)==4 and sp[1].upper()==finra_individual_delta_name.upper())

        ans['table_name'] = ans['table_name'].lower()
    except:
        logger.warning(f'Cannot parse file name: {file_path}')
        return

    return ans



# %% Get Meta Data from Finra XML File

@catch_error(logger)
def get_finra_file_xml_meta(file_path:str, firm_crd_number:str, sql_ingest_table):
    """
    Get Meta Data from Finra XML File (reading metadata from inside the file and also use metadata from file name)
    """
    rowTag = '?xml'
    csv_flag = False

    file_meta = extract_data_from_finra_file_path(file_path=file_path, firm_crd_number=firm_crd_number)
    if not file_meta:
        return

    fcol = None
    if sql_ingest_table:
        filtersql = sql_ingest_table.where(
            (col(FirmCRDNumber)==lit(file_meta[FirmCRDNumber])) &
            (col('table_name')==lit(file_meta['table_name'])) &
            (col(date_column_name)==to_date(lit(file_meta[date_column_name]), format='yyyy-MM-dd')) &
            (col('file_name')==lit(file_meta['file_name'])) &
            (col('folder_path')==lit(file_meta['folder_path']))
            )
        if not filtersql.rdd.isEmpty():
            fcol = filtersql.limit(1).collect()[0]
            criteria = json.loads(fcol['xml_criteria'])
            rowTags = json.loads(fcol['xml_rowtags'])

    if not fcol:
        if file_meta['table_name'].upper() == DualRegistrations_name.upper():
            csv_flag = True
            xml_table = read_csv(spark=spark, file_path=file_path)
            criteria = {
                reportDate_name: file_meta[date_column_name],
                firmCRDNumber_name: file_meta[FirmCRDNumber],
                }
            xml_table_columns = xml_table.columns
        elif file_path.lower().endswith('.zip'):
            with tempfile.TemporaryDirectory(dir=os.path.dirname(file_path)) as tmpdir:
                shutil.unpack_archive(filename=file_path, extract_dir=tmpdir)
                k = 0
                for root1, dirs1, files1 in os.walk(tmpdir):
                    for file1 in files1:
                        file_path1 = os.path.join(root1, file1)
                        xml_table = read_xml(spark=spark, file_path=file_path1, rowTag=rowTag)
                        criteria = table_to_list_dict(xml_table.select('Criteria.*'))[0]
                        xml_table_columns = xml_table.columns
                        k += 1
                        break
                    if k>0: break
        else:
            xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag)
            criteria = table_to_list_dict(xml_table.select('Criteria.*'))[0]
            xml_table_columns = xml_table.columns

        if not criteria.get(reportDate_name):
            criteria[reportDate_name] = criteria['_postingDate']

        if criteria[firmCRDNumber_name] != file_meta[FirmCRDNumber]:
            logger.warning(f"{file_path}\nFirm CRD Number in Criteria '{criteria[firmCRDNumber_name]}' does not match to the CRD Number in the file name '{file_meta[FirmCRDNumber]}'")
            file_meta[FirmCRDNumber] = criteria[firmCRDNumber_name]

        if criteria[reportDate_name] != file_meta[date_column_name]:
            logger.warning(f"{file_path}\nReport/Posting Date in Criteria '{criteria[reportDate_name]}' does not match to the date in the file name '{file_meta[date_column_name]}'")
            file_meta[date_column_name] = criteria[reportDate_name]

        rowTags = [c for c in xml_table_columns if c.lower() not in ['criteria']]
        assert len(rowTags) == 1 or csv_flag, f"{file_path}\nXML File has rowTags {rowTags} is not valid"

    file_meta['is_full_load'] = criteria.get('_IIRType') == 'FULL' or file_meta['table_name'].upper() in ['BranchInformationReport'.upper(), DualRegistrations_name.upper()]

    file_meta = {
        **file_meta,
        'xml_criteria': criteria,
        'xml_rowtags': rowTags,
    }

    return file_meta



# %% Read XML File

@catch_error(logger)
def read_xml_file(table_name:str, file_path:str, rowTag:str):
    """
    Read Finra XML File
    """
    schema_file = table_name+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if table_name.upper() == DualRegistrations_name.upper():
        xml_table = read_csv(spark=spark, file_path=file_path)
    elif os.path.isfile(schema_path):
        logger.info(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag, schema=schema)
    else:
        logger.info(f"Looking for Schema File Location: {schema_path}")
        logger.warning(f"No manual schema defined for {table_name}. Using default schema.")
        xml_table = read_xml(spark=spark, file_path=file_path, rowTag=rowTag)

    if is_pc: xml_table.printSchema()
    return xml_table



# %% Get XML Table List

@catch_error(logger)
def get_xml_table_list(xml_table, table_name:str, crd_number:str):
    """
    Get list of sub-tables for a given nested XML Table
    """
    xml_table_list = {}

    if flatten_n_divide_flag:
        xml_table_list={**xml_table_list, **flatten_n_divide_table(table=xml_table, table_name=table_name)}
    else:
        semi_flat_table = flatten_table(table=xml_table)

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
    """
    Process Columns for ELT (add ID key, ELT columns, etc.)
    """
    new_table_list = {}

    for table_name, table in table_list.items():
        table1 = table
        table1 = remove_column_spaces(table_to_remove = table1)
        table1 = table1.withColumn(date_column_name, lit(str(file_meta[date_column_name])))
        table1 = table1.withColumn(FirmCRDNumber, lit(str(file_meta[FirmCRDNumber])))

        if table_name.upper() == 'IndividualInformationReport'.upper():
            table1 = add_id_key(table1, key_column_names=[FirmCRDNumber, 'CRD_NUMBER'])
        elif table_name.upper() == 'BranchInformationReport'.upper():
            table1 = add_id_key(table1, key_column_names=[FirmCRDNumber, 'BRANCH_CRD_NUMBER'])
        else:
            table1 = add_md5_key(table1)

        table1 = add_elt_columns(
            table = table1,
            reception_date = file_meta[date_column_name],
            source = tableinfo_source,
            is_full_load = file_meta['is_full_load'],
            dml_type = 'I' if file_meta['is_full_load'] or file_meta['table_name'].upper() not in ['IndividualInformationReport'.upper()] else 'U',
            )

        new_table_list = {**new_table_list, table_name: table1}

    return new_table_list



# %% Main Processing of Finra file

@catch_error(logger)
def process_finra_file(file_meta, sql_ingest_table):
    """
    Main Processing of single Finra file
    """
    file_path = os.path.join(file_meta['folder_path'], file_meta['file_name'])
    rowTags = file_meta['xml_rowtags']

    logger.info(file_meta)

    xml_table = read_xml_file(table_name=file_meta['table_name'], file_path=file_path, rowTag=rowTags[0])
    xml_table_list = get_xml_table_list(xml_table=xml_table, table_name=file_meta['table_name'], crd_number=file_meta[FirmCRDNumber])
    xml_table_list = process_columns_for_elt(table_list=xml_table_list, file_meta=file_meta)

    return xml_table_list



# %% Iterate over all the files in all the firms and process them.

additional_ingest_columns = [
    to_date(col(date_column_name), format='yyyy-MM-dd').alias(date_column_name),
    to_json(col('xml_criteria')).alias('xml_criteria'),
    to_json(col('xml_rowtags')).alias('xml_rowtags'),
    ]

all_new_files, PARTITION_list, tableinfo = process_all_files_with_incrementals(
    spark = spark,
    firms = firms,
    data_path_folder = data_path_folder,
    fn_extract_file_meta = get_finra_file_xml_meta,
    date_start = date_start,
    additional_ingest_columns = additional_ingest_columns,
    fn_process_file = process_finra_file,
    key_column_names = key_column_names,
    tableinfo_source = tableinfo_source,
    save_data_to_adls_flag = save_finra_to_adls_flag,
    date_column_name = date_column_name
    )



# %% Save Tableinfo metadata table into Azure and Save Ingest files metadata to SQL Server.

tableinfo = save_tableinfo_dict_and_sql_ingest_table(
    spark = spark,
    tableinfo = tableinfo,
    tableinfo_source = tableinfo_source,
    all_new_files = all_new_files,
    save_tableinfo_adls_flag = save_tableinfo_adls_flag,
    )



# %% Read metadata.TableInfo

table_rows = read_tableinfo_rows(tableinfo_name=tableinfo_name, tableinfo_source=tableinfo_source, tableinfo=tableinfo)


# %% Connect to SnowFlake

snowflake_connection = connect_to_snowflake()
snowflake_ddl_params.snowflake_connection = snowflake_connection


# %% Iterate Over Steps for all tables

ingest_data_list = iterate_over_all_tables_snowflake(tableinfo=tableinfo, table_rows=table_rows, PARTITION_list=PARTITION_list)


# %% Create Source Level Tables

create_source_level_tables(ingest_data_list=ingest_data_list)


# %% Close Showflake connection

snowflake_connection.close()


# %% Mark Execution End

mark_execution_end()


# %%


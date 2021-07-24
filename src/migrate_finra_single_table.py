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
from modules.spark_functions import create_spark, read_xml
from modules.config import is_pc
from modules.azure_functions import setup_spark_adls_gen2_connection, save_adls_gen2, tableinfo_name, file_format, container_name, \
    to_storage_account_name, select_tableinfo_columns, tableinfo_container_name, read_adls_gen2
from modules.data_functions import  to_string, remove_column_spaces, add_elt_columns, execution_date, column_regex, partitionBy, \
    metadata_FirmSourceMap, partitionBy_value


from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, lit, explode, md5, concat_ws, from_json, arrays_zip, concat, filter, monotonically_increasing_id



# %% Logging
logger = make_logging(__name__)


# %% Parameters

save_xml_to_adls_flag = True
save_tableinfo_adls_flag = True
flatten_n_divide_flag = False

if not is_pc:
    save_xml_to_adls_flag = True
    save_tableinfo_adls_flag = True
    flatten_n_divide_flag = False

date_start = '2021-01-01'

domain_name = 'financial_professional'
database = 'FINRA'
tableinfo_source = database

KeyIndicator = 'MD5_KEY'
FirmCRDNumber = 'Firm_CRD_Number'

finra_individual_delta_name = 'IndividualInformationReportDelta'
reportDate_name = '_reportDate'
firmCRDNumber_name = '_firmCRDNumber'



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



# %% Get Meta Data from Finra XML File

@catch_error(logger)
def get_finra_file_xml_meta(file_path):
    rowTag = '?xml'

    file_meta = extract_data_from_finra_file_name(file_name=file_path)
    if not file_meta:
        return (None,) * 3

    if file_path.endswith('.zip'):
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
    assert len(rowTags) == 1, f"\n{file_path}\nXML File has rowTags {rowTags} is not valid\n"

    if file_meta['table_name'].upper() == 'IndividualInformationReportDelta'.upper():
        file_meta['table_name'] = 'IndividualInformationReport'

    file_meta['is_full_load'] = criteria.get('_IIRType') == 'FULL' or file_meta['table_name'].upper() in ['BranchInformationReport'.upper()]

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
def get_all_finra_file_xml_meta(folder_path, date_start:str, inclusive:bool=True):
    print(f'\nGetting list of candidate files from {folder_path}')
    files_meta = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_meta = get_finra_file_xml_meta(file_path)

            if file_meta and (date_start<file_meta['date'] or (date_start==file_meta['date'] and inclusive)):
                files_meta.append(file_meta)

    print(f'Finished getting list of files. Total Files = {len(files_meta)}\n')
    return files_meta





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
        var_col_type = 'string' if col_type=='string' else 'variant'

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
        tableinfo['IsNullable'].append(0 if col_name in [KeyIndicator] else 1)
        tableinfo['KeyIndicator'].append(1 if col_name in [KeyIndicator] else 0)
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
    monid = 'monotonically_increasing_id'
    FILE_DATE = 'FILE_DATE'

    for df_name, xml_table in xml_table_list.items():
        print(f'\nWriting {df_name} to Azure...')

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        xml_table1 = xml_table
        xml_table1 = remove_column_spaces(table_to_remove = xml_table1)
        xml_table1 = xml_table1.withColumn(monid, monotonically_increasing_id().cast('string'))

        xml_table2 = to_string(table_to_convert_columns = xml_table1, col_types = []) # Convert all columns to string
        xml_table2 = xml_table2.withColumn(FILE_DATE, lit(str(reception_date)))
        xml_table2 = xml_table2.withColumn(FirmCRDNumber, lit(str(crd_number)))

        md5_columns = [c for c in xml_table2.columns if c not in [monid]]
        xml_table2 = xml_table2.withColumn(KeyIndicator, md5(concat_ws('_', *md5_columns))) # add HASH column for key indicator

        xml_table1 = xml_table1.alias('x1'
            ).join(xml_table2.alias('x2'), xml_table1[monid]==xml_table2[monid], how='left'
            ).select('x1.*', 'x2.'+FILE_DATE, 'x2.'+FirmCRDNumber, 'x2.'+KeyIndicator
            ).drop(monid)

        add_table_to_tableinfo(xml_table=xml_table1, firm_name=firm_name, table_name = df_name)

        xml_table1 = add_elt_columns(
            table_to_add = xml_table1,
            reception_date = reception_date,
            source = tableinfo_source,
            is_full_load = is_full_load,
            dml_type = 'I' if is_full_load or firm_name.upper() not in ['IndividualInformationReport'.upper()] else 'U',
            )

        if is_pc: xml_table1.printSchema()

        if is_pc: # and manual_iteration:
            local_path = os.path.join(data_path_folder, 'temp') + fr'\{storage_account_name}\{container_folder}\{df_name}'
            print(fr'Save to local {local_path}')
            #xml_table1.coalesce(1).write.csv( path = fr'{local_path}.csv',  mode='overwrite', header='true')
            xml_table1.coalesce(1).write.json(path = fr'{local_path}.json', mode='overwrite')

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





# %% Build Branch Table

def build_branch_table(semi_flat_table):
    # Create Schemas
    Branch_Address_Schema = StructType([
        StructField('Country', StringType(), True),
        StructField('State', StringType(), True),
        StructField('City', StringType(), True),
        StructField('Postal_Code', StringType(), True),
        StructField('Street1', StringType(), True),
        StructField('Street2', StringType(), True),
        ])


    Associated_Individuals_Schema = ArrayType(StructType([
        StructField('First_Name', StringType(), True),
        StructField('Middle_Name', StringType(), True),
        StructField('Last_Name', StringType(), True),
        StructField('Suffix_Name', StringType(), True),
        StructField('CRD_Number', StringType(), True),
        StructField('Independent_Contractor', StringType(), True),
        ]), True)


    Registrations_Schema = ArrayType(StructType([
        StructField('Regulator', StringType(), True),
        StructField('Status', StringType(), True),
        StructField('Status_Date', StringType(), True),
        ]), True)


    def filter_Registration(x):
        return x.getField('_rgltr') == lit('FINRA')


    Other_Business_Activity_Schema = ArrayType(StructType([
        StructField('Activity_Name', StringType(), True),
        StructField('Sequence_Number', StringType(), True),
        StructField('Description', StringType(), True),
        StructField('Affiliated', StringType(), True),
        ]), True)


    Other_Business_Names_Schema =  ArrayType(StructType([
        StructField('Sequence_Number', StringType(), True),
        StructField('Business_Name', StringType(), True),
        ]), True)


    Other_Websites_Schema =  ArrayType(StructType([
        StructField('Sequence_Number', StringType(), True),
        StructField('Website_Address', StringType(), True),
        ]), True)


    Arrangements_Shared_Entities_Entity_Types_Schema = ArrayType(StructType([
        StructField('Blank', StringType(), True),
        StructField('Entity_Code', StringType(), True),
        StructField('Other_Description', StringType(), True),
        ]), True)


    Arrangements_Shared_Entities_Schema = ArrayType(StructType([
        StructField('Sequence_Number', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('CRD_Number', StringType(), True),
        StructField('Affiliate', StringType(), True),
        StructField('Entity_Types', Arrangements_Shared_Entities_Entity_Types_Schema, True),
        ]), True)


    Arrangements_Contract_Entities_Schema = ArrayType(StructType([
        StructField('Sequence_Number', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('CRD_Number', StringType(), True),
        StructField('Type', StringType(), True),
        ]), True)


    Arrangements_Expense_Entities_Schema = ArrayType(StructType([
        StructField('Sequence_Number', StringType(), True),
        StructField('Name', StringType(), True),
        StructField('Type', StringType(), True),
        StructField('Registered', StringType(), True),
        StructField('CRD_Number', StringType(), True),
        StructField('EIN', StringType(), True),
        ]), True)


    Arrangements_Records_Entities_Schema = ArrayType(StructType([
        StructField('Street1', StringType(), True),
        StructField('Street2', StringType(), True),
        StructField('City', StringType(), True),
        StructField('State', StringType(), True),
        StructField('Country', StringType(), True),
        StructField('Postal_Code', StringType(), True),
        StructField('Sequence_Number', StringType(), True),
        StructField('First_Name', StringType(), True),
        StructField('Last_Name', StringType(), True),
        StructField('Email', StringType(), True),
        StructField('Phone', StringType(), True),
        ]), True)


    # Create Branch Table

    branch = semi_flat_table.select(
        #col('_orgPK').alias('Firm_CRD_Number'),
        col('BrnchOfcs_BrnchOfc_brnchPK').alias('Branch_CRD_Number'),
        col('BrnchOfcs_BrnchOfc_bllngCd').alias('Billing_Code'),
        col('BrnchOfcs_BrnchOfc_brnchPhone').alias('Branch_Phone'),
        col('BrnchOfcs_BrnchOfc_brnchFax').alias('Branch_Fax'),
        col('BrnchOfcs_BrnchOfc_prvtRsdnc').alias('Is_Private_Residence'),
        col('BrnchOfcs_BrnchOfc_dstrtPK').alias('District_Office'),
        col('BrnchOfcs_BrnchOfc_oprnlStCd').alias('Operational_Status'),
        from_json(
            col = concat(
            lit('{'),
            lit('  "Country": "'), col('BrnchOfcs_BrnchOfc_Addr_cntry'), lit('"'),
            lit(', "State": "'), col('BrnchOfcs_BrnchOfc_Addr_state'), lit('"'),
            lit(', "City": "'), col('BrnchOfcs_BrnchOfc_Addr_city'), lit('"'),
            lit(', "Postal_Code": "'), col('BrnchOfcs_BrnchOfc_Addr_postlCd'), lit('"'),
            lit(', "Street1": "'), col('BrnchOfcs_BrnchOfc_Addr_strt1'), lit('"'),
            lit(', "Street2": "'), col('BrnchOfcs_BrnchOfc_Addr_strt2'), lit('"'),
            lit('}'),
            ),
            schema = Branch_Address_Schema
            ).alias('Branch_Address'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._first').alias('First_Name'),
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._mid').alias('Middle_Name'),
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._last').alias('Last_Name'),
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._suf').alias('Suffix_Name'),
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._indvlPK').alias('CRD_Number'),
            col('BrnchOfcs_BrnchOfc_AsctdIndvls_AsctdIndvl._ndpndCntrcrFl').alias('Independent_Contractor'),
            ).cast(Associated_Individuals_Schema
            ).alias('Associated_Individuals'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn._rgltr').alias('Regulator'),
            col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn._st').alias('Status'),
            col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn._stDt').alias('Status_Date'),
            ).cast(Registrations_Schema
            ).alias('Registrations'),
        filter(
            col = col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn'), 
            f = filter_Registration
            ).getField('_st').getItem(0).alias('Registration_Status'),
        filter(
            col = col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn'), 
            f = filter_Registration
            ).getField('_stDt').getItem(0).alias('Registration_Start_Date'),
        filter(
            col = col('BrnchOfcs_BrnchOfc_Rgstns_Rgstn'),
            f = filter_Registration
            ).getField('_rgltr').getItem(0).alias('Registration_Regulator_Code'),
        col('BrnchOfcs_BrnchOfc_TypeOfc_typeOfcBDFl').alias('Is_Broker_Dealer_Office'),
        col('BrnchOfcs_BrnchOfc_TypeOfc_typeOfcIAFl').alias('Is_Investment_Advisor_Office'),
        col('BrnchOfcs_BrnchOfc_TypeOfc_OSJFl').alias('Is_OSJ'),
        concat_ws(', ', col('BrnchOfcs_BrnchOfc_FinActvys_FinActvy._actvyCd')).alias('Financial_Activity'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC._indvlPK').getItem(0).alias('PIC_CRDNumber'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC._roleCd').getItem(0).alias('PIC_Role_Code'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC._sprvPICDt').getItem(0).alias('PIC_StartDate'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC._ndpndCntrcrFl').getItem(0).alias('PIC_Is_Independent_Contractor'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC.Nm._first').getItem(0).alias('PIC_First_Name'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC.Nm._mid').getItem(0).alias('PIC_Middle_Name'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC.Nm._last').getItem(0).alias('PIC_Last_Name'),
        col('BrnchOfcs_BrnchOfc_SprvPICs_SprvPIC.Nm._suf').getItem(0).alias('PIC_Suffix_Name'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ._sprvgOSJBrnchPK').getItem(0).alias('Supervisor_Branch_CRDNumber'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ._sprvgOSJIndvlPK').getItem(0).alias('Supervisor_OSJ_CRDNumber'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ.Nm._first').getItem(0).alias('Supervisor_First_Name'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ.Nm._mid').getItem(0).alias('Supervisor_Middle_Name'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ.Nm._last').getItem(0).alias('Supervisor_Last_Name'),
        col('BrnchOfcs_BrnchOfc_SprvgOSJs_SprvgOSJ.Nm._suf').getItem(0).alias('Supervisor_Suffix_Name'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusActvys_OthrBusActvy._nm').alias('Activity_Name'),
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusActvys_OthrBusActvy._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusActvys_OthrBusActvy._desc').alias('Description'),
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusActvys_OthrBusActvy._affltdFl').alias('Affiliated'),
            ).cast(Other_Business_Activity_Schema
            ).alias('Other_Business_Activity'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusNms_OthrBusNm._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrBusNms_OthrBusNm._nm').alias('Business_Name'),
            ).cast(Other_Business_Names_Schema
            ).alias('Other_Business_Names'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrWebs_OthrWeb._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_OthrBuss_OthrWebs_OthrWeb._webAddr').alias('Website_Address'),
            ).cast(Other_Websites_Schema
            ).alias('Other_Websites'),
        col('BrnchOfcs_BrnchOfc_Rgmnts_shrOfcFl').alias('Has_Shared_Space'),
        col('BrnchOfcs_BrnchOfc_Rgmnts_undrCntrcFl').alias('Is_Under_Contract'),
        col('BrnchOfcs_BrnchOfc_Rgmnts_empDcsnFl').alias('Has_Employment_Responsibility'),
        col('BrnchOfcs_BrnchOfc_Rgmnts_xpnsRspbyEnttyFl').alias('Has_Financial_Responsibility'),
        col('BrnchOfcs_BrnchOfc_Rgmnts_xpnsEnttyDesc').alias('Has_Financial_Interest'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_Rgmnts_ShrdEnttys_ShrdEntty._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_ShrdEnttys_ShrdEntty._nm').alias('Name'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_ShrdEnttys_ShrdEntty._crdPK').alias('CRD_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_ShrdEnttys_ShrdEntty._affltdFl').alias('Affiliate'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_ShrdEnttys_ShrdEntty.EnttyTypes.EnttyType').alias('Entity_Types'),
            ).cast(Arrangements_Shared_Entities_Schema
            ).alias('Arrangements_Shared_Entities'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_Rgmnts_CntrcEnttys_CntrcEntty._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_CntrcEnttys_CntrcEntty._nm').alias('Name'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_CntrcEnttys_CntrcEntty._crdPK').alias('CRD_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_CntrcEnttys_CntrcEntty._type').alias('Type'),
            ).cast(Arrangements_Contract_Entities_Schema
            ).alias('Arrangements_Contract_Entities'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._nm').alias('Name'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._type').alias('Type'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._rgstdFl').alias('Registered'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._crdPK').alias('CRD_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_XpnsRspbyEnttys_XpnsRspbyEntty._EIN').alias('EIN'),
            ).cast(Arrangements_Expense_Entities_Schema
            ).alias('Arrangements_Expense_Entities'),
        arrays_zip(
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._strt1').alias('Street1'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._strt2').alias('Street2'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._city').alias('City'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._state').alias('State'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._cntry').alias('Country'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc.Addr._postlCd').alias('Postal_Code'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc._seqNb').alias('Sequence_Number'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc._cntctFirstNm').alias('First_Name'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc._cntctLastNm').alias('Last_Name'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc._cntctEmailAddr').alias('Email'),
            col('BrnchOfcs_BrnchOfc_Rgmnts_RcrdsLocs_RcrdsLoc._phone').alias('Phone'),
            ).cast(Arrangements_Records_Entities_Schema
            ).alias('Arrangements_Records_Entities'),
    )

    #pprint(branch.limit(10).toJSON().map(lambda j: json.loads(j)).collect())
    #pprint(branch.where('Branch_CRD_Number = 701667').toJSON().map(lambda j: json.loads(j)).collect())
    return branch




# %% Main Processing of Finra file

@catch_error(logger)
def process_finra_file(file_meta, firm_name:str, storage_account_name:str):
    file_path = os.path.join(file_meta['root'], file_meta['file'])

    criteria = file_meta['criteria']
    rowTags = file_meta['rowTags']
    table_name = file_meta['table_name']

    print('\n', file_meta['crd_number'], table_name, file_meta['date'])
    print(f'\nrowTags: {rowTags}\n')
    rowTag = rowTags[0]

    schema_file = table_name+'.json'
    schema_path = os.path.join(schema_path_folder, schema_file)

    if os.path.isfile(schema_path):
        print(f"Loading schema from file: {schema_file}")
        with open(schema_path, 'r') as f:
            base = json.load(f)
        schema = base_to_schema(base)
        xml_table = read_xml(spark, file_path, rowTag=rowTag, schema=schema)
    else:
        print(f"Looking for Schema File Location: {schema_path}")
        print(f"No manual schema defined for {table_name}. Using default schema.")
        xml_table = read_xml(spark, file_path, rowTag=rowTag)

    if is_pc: xml_table.printSchema()
    xml_table_list = {}

    if flatten_n_divide_flag:
        xml_table_list={**xml_table_list, **flatten_n_divide_df(xml_table=xml_table, table_name=table_name)}

    semi_flat_table = flatten_df(xml_table=xml_table)

    if table_name.upper() == 'BranchInformationReport'.upper():
        xml_table_list={**xml_table_list, table_name: build_branch_table(semi_flat_table=semi_flat_table)}
    else:
        xml_table_list={**xml_table_list, table_name: semi_flat_table}


    write_xml_table_list_to_azure(
        xml_table_list= xml_table_list,
        file_name = table_name,
        reception_date = file_meta['date'],
        firm_name = firm_name,
        storage_account_name = storage_account_name,
        is_full_load = file_meta['is_full_load'],
        crd_number = file_meta['crd_number'],
        )

    return xml_table




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
                    process_finra_file(file_meta=file_meta1, firm_name=firm_name, storage_account_name=storage_account_name)
                    k += 1
                    break
                if k>0: break
    else:
        process_finra_file(file_meta=file_meta, firm_name=firm_name, storage_account_name=storage_account_name)



# %% Testing

if False:
    folder_path = r'C:\Users\smammadov\packages\Shared\test'
    files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start)
    file_meta = files_meta[0]
    print(file_meta)
    xml_table = process_finra_file(file_meta, firm_name='x', storage_account_name='x')
    semi_flat_table = flatten_df(xml_table=xml_table)





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

        files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start)

        for file_meta in files_meta:
            if is_pc and 'IndividualInformationReport'.upper() in file_meta['table_name'].upper():
                continue
            process_one_file(file_meta=file_meta, firm_name=firm_name, storage_account_name=storage_account_name)



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


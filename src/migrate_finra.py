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
from pyspark.sql.functions import col, lit, explode, md5, concat_ws, from_json, arrays_zip, concat, filter, monotonically_increasing_id, \
    struct



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

    for df_name, xml_table in xml_table_list.items():
        print(f'\nWriting {df_name} to Azure...')

        data_type = 'data'
        container_folder = f'{data_type}/{domain_name}/{database}/{firm_name}'

        xml_table1 = xml_table
        xml_table1 = remove_column_spaces(table_to_remove = xml_table1)
        xml_table1 = xml_table1.withColumn('FILE_DATE', lit(str(reception_date)))
        xml_table1 = xml_table1.withColumn(FirmCRDNumber, lit(str(crd_number)))

        xml_table1 = xml_table1.withColumn(monid, monotonically_increasing_id().cast('string'))
        xml_table2 = to_string(table_to_convert_columns = xml_table1, col_types = []) # Convert all columns to string
        md5_columns = [c for c in xml_table2.columns if c not in [monid]]
        xml_table2 = xml_table2.withColumn(KeyIndicator, md5(concat_ws('_', *md5_columns))) # add HASH column for key indicator

        xml_table1 = xml_table1.alias('x1'
            ).join(xml_table2.alias('x2'), xml_table1[monid]==xml_table2[monid], how='left'
            ).select('x1.*', 'x2.'+KeyIndicator
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

@catch_error(logger)
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

    return semi_flat_table




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

if True:
    folder_path = r'C:\Users\smammadov\packages\Shared\test'
    files_meta = get_all_finra_file_xml_meta(folder_path=folder_path, date_start=date_start)
    file_meta = files_meta[0]
    print(file_meta)
    semi_flat_table = process_finra_file(file_meta, firm_name='x', storage_account_name='x')



# %% Create Individual Table

IA_Affiliations_Schema = ArrayType(StructType([
    StructField('Street1', StringType(), True),
    StructField('Street2', StringType(), True),
    StructField('City', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Postal_Code', StringType(), True),
    StructField('Affiliation_Category', StringType(), True),
    StructField('Branch_Name', StringType(), True),
    StructField('Branch_CRD_Number', StringType(), True),
    ]), True)


Other_Names_Schema = ArrayType(StructType([
    StructField('Last_Name', StringType(), True),
    StructField('First_Name', StringType(), True),
    StructField('Middle_Name', StringType(), True),
    StructField('Suffix_Name', StringType(), True),
    StructField('Sequence_Number', StringType(), True),
    ]), True)


Address_History_Schema = ArrayType(StructType([
    StructField('Street1', StringType(), True),
    StructField('Street2', StringType(), True),
    StructField('City', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Postal_Code', StringType(), True),
    StructField('From_Date', StringType(), True),
    StructField('To_Date', StringType(), True),
    StructField('Sequence_Number', StringType(), True),
    ]), True)


def filter_Current_Date(x):
    return (x.getField('DtRng').getField('_toDt').isNull()) | (x.getField('DtRng').getField('_toDt') == lit(''))


Designations_Schema = ArrayType(StructType([
    StructField('Designation_Code', StringType(), True),
    StructField('Designating_Authority', StringType(), True),
    StructField('Date_First_Filing', StringType(), True),
    ]), True)


Continuing_Education_Details_Appointments_Schema = ArrayType(StructType([
    StructField('Blank', StringType(), True), # _VALUE
    StructField('Appointment_ID', StringType(), True), # _apptID
    StructField('Current_Status', StringType(), True), # _apptSt
    StructField('Current_Status_Detail', StringType(), True), # _apptDt
    StructField('Vendor', StringType(), True), # _vndrNm
    StructField('Vendor_Number', StringType(), True), # _vndrCnfrtNb
    StructField('Vendor_Center_ID', StringType(), True), # _cntrID
    StructField('Vendor_Center_City', StringType(), True), # _cntrCity
    StructField('Vendor_Center_State', StringType(), True), # _cntrSt
    StructField('Vendor_Center_Country', StringType(), True), # _cntrCntry
    StructField('Date_Apppointment_Updated', StringType(), True), # _updtTS
    ]), True)


Continuing_Education_Details_Schema = ArrayType(StructType([
    StructField('Appointments', Continuing_Education_Details_Appointments_Schema, True),
    StructField('Start_Date', StringType(), True),
    StructField('End_Date', StringType(), True),
    StructField('Information_Changed_Date', StringType(), True),
    StructField('Foreign_Deferred', StringType(), True),
    StructField('Military_Deferred', StringType(), True),
    StructField('Enrollment_ID', StringType(), True),
    StructField('Result', StringType(), True),
    StructField('Session_Type', StringType(), True),
    StructField('Session_Date', StringType(), True),
    StructField('Session_Status_Type', StringType(), True),
    StructField('Session_Status', StringType(), True),
    ]), True)


Filing_History_Schema = ArrayType(StructType([
    StructField('Filing_Date', StringType(), True),
    StructField('Filing_Type', StringType(), True),
    StructField('Form_Type', StringType(), True),
    StructField('Filing_ID', StringType(), True),
    StructField('Submitted_By', StringType(), True),
    StructField('Event_Type', StringType(), True),
    ]), True)


Other_Businesses_Schema = ArrayType(StructType([
    StructField('Description', StringType(), True),
    ]), True)


Finger_Print_Info_Schema = ArrayType(StructType([
    StructField('Card_Bar_Code', StringType(), True),
    StructField('Firm_Submitted', StringType(), True),
    StructField('Position', StringType(), True),
    StructField('Card_Status', StringType(), True),
    StructField('Card_Status_Date', StringType(), True),
    StructField('Card_Received_Date', StringType(), True),
    ]), True)


Exams_Appointments_Schema = ArrayType(StructType([
    StructField('Blank', StringType(), True), # _VALUE
    StructField('Appointment_ID', StringType(), True), # _apptID
    StructField('Current_Status', StringType(), True), # _apptSt
    StructField('Current_Status_Detail', StringType(), True), # _apptDt
    StructField('Vendor', StringType(), True), # _vndrNm
    StructField('Vendor_Number', StringType(), True), # _vndrCnfrtNb
    StructField('Vendor_Center_ID', StringType(), True), # _cntrID
    StructField('Vendor_Center_City', StringType(), True), # _cntrCity
    StructField('Vendor_Center_State', StringType(), True), # _cntrSt
    StructField('Vendor_Center_Country', StringType(), True), # _cntrCntry
    StructField('Date_Apppointment_Updated', StringType(), True), # _updtTS
    ]), True)


Exams_Schema = ArrayType(StructType([
    StructField('Appointments', Exams_Appointments_Schema, True),
    StructField('Exam_Code', StringType(), True),
    StructField('Exam_Date', StringType(), True),
    StructField('Currently_Valid', StringType(), True),
    StructField('Valid_Until', StringType(), True),
    StructField('Grade', StringType(), True),
    StructField('Score', StringType(), True),
    StructField('Enrollment_ID', StringType(), True),
    StructField('Exam_Status', StringType(), True),
    StructField('Exam_Status_Date', StringType(), True),
    StructField('Date_Updated', StringType(), True),
    StructField('Window_Begin_Date', StringType(), True),
    StructField('Window_End_Date', StringType(), True),
    ]), True)


Exam_Waivers_Schema = ArrayType(StructType([
    StructField('Enrollment_ID', StringType(), True),
    StructField('Exam_Code', StringType(), True),
    StructField('Regulator', StringType(), True),
    StructField('Waiver_Reason', StringType(), True),
    StructField('Currently_Valid', StringType(), True),
    StructField('Valid_Until', StringType(), True),
    StructField('Waived_Date', StringType(), True),
    ]), True)


Employment_History_Schema = ArrayType(StructType([
    StructField('From_Date', StringType(), True),
    StructField('To_Date', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Investment_Related', StringType(), True),
    StructField('Organization_Name', StringType(), True),
    StructField('Postal_Code', StringType(), True),
    StructField('Position', StringType(), True),
    StructField('Sequence_Number', StringType(), True),
    StructField('State', StringType(), True),
    ]), True)


Address_Schema = StructType([
    StructField('Blank', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Country2', StringType(), True),
    StructField('Postal_Code', StringType(), True),
    StructField('State', StringType(), True),
    StructField('Street1', StringType(), True),
    StructField('Street2', StringType(), True),
    ])


Employment_Locations_Schema = ArrayType(StructType([
    StructField('Address', Address_Schema, True), # Addr
    StructField('Billing_Code', StringType(), True), # _bllngCd
    StructField('Branch_CRD_Number', StringType(), True), # _brnchPK
    StructField('From_Date', StringType(), True), # _fromDt
    StructField('Located_At', StringType(), True), # _lctdFl
    StructField('Main_Office', StringType(), True), # _mainOfcBDFl
    StructField('IA_Main_Office', StringType(), True), # _mainOfcIAFl
    StructField('Old_Sequence_Number', StringType(), True), # _oldSeqNb
    StructField('Private_Residence', StringType(), True), # _prvtRsdnc
    StructField('Registered_Location', StringType(), True), # _regdLocFl
    StructField('Sequence_Number', StringType(), True), # _seqNb
    StructField('Supervised', StringType(), True), # _sprvdFl
    StructField('To_Date', StringType(), True), # _toDt
    ]), True)


Affilated_Firms_Schema = ArrayType(StructType([
    StructField('Employment_Locations', Employment_Locations_Schema, True),
    StructField('Firm_Name', StringType(), True),
    StructField('Firm_CRD_Number', StringType(), True),
    StructField('Date_Hired', StringType(), True),
    StructField('Independent_Contractor', StringType(), True),
    ]), True)


Office_Employment_History_Schema = ArrayType(StructType([
    StructField('From_Date', StringType(), True),
    StructField('To_Date', StringType(), True),
    StructField('Employment_Locations', Employment_Locations_Schema, True),
    StructField('Employment_Type', StringType(), True),
    StructField('Firm_Association', StringType(), True),
    StructField('Independent_Contractor', StringType(), True),
    StructField('Firm_Name', StringType(), True),
    StructField('Firm_CRD_Number', StringType(), True),
    StructField('Termination_Date', StringType(), True),
    StructField('Termination_Explanation', StringType(), True),
    StructField('Termination_Reason', StringType(), True),
    StructField('Termination_Amendment_Explanation', StringType(), True),
    StructField('Termination_Date_Amendment_Explanation', StringType(), True),
    ]), True)


def filter_Current_Date_and_Firm(crd_number:str):
    def inner_filter_func(x):
        return (((x.getField('DtRng').getField('_toDt').isNull()) | (x.getField('DtRng').getField('_toDt') == lit(''))) 
            & (x.getField('_orgPK') == lit(crd_number)))
    return inner_filter_func


individual = semi_flat_table.select(
    col('Comp_indvlSSN').alias('SSN'),
    col('Comp_indvlPK').alias('CRD_Number'),
    col('Comp_rptblDisc').alias('Has_Reportable_Disclosure'),
    col('Comp_statDisq').alias('Has_Statutory_Disqualifications'),
    col('Comp_rgstdMult').alias('Has_Multiple_Firm_Registrations'),
    col('Comp_matDiff').alias('Has_Material_Difference_In_Disc'),
    col('Comp_bllngCd').alias('Billing_Code'),
    col('Comp_actvMltry').alias('Is_Active_Military_Duty'),
    col('Comp_Nm_first').alias('First_Name'),
    col('Comp_Nm_mid').alias('Middle_Name'),
    col('Comp_Nm_last').alias('Last_Name'),
    col('Comp_Nm_suf').alias('Suffix_Name'),
    arrays_zip(
        col('IAAffltns_IAAffltn.Addr._strt1').alias('Street1'),
        col('IAAffltns_IAAffltn.Addr._strt2').alias('Street2'),
        col('IAAffltns_IAAffltn.Addr._city').alias('City'),
        col('IAAffltns_IAAffltn.Addr._state').alias('State'),
        col('IAAffltns_IAAffltn.Addr._cntryCd').alias('Country'),
        col('IAAffltns_IAAffltn.Addr._postlCd').alias('Postal_Code'),
        col('IAAffltns_IAAffltn._iaAffltnCtgry').alias('Affiliation_Category'),
        col('IAAffltns_IAAffltn._orgNm').alias('Firm_Name'),
        col('IAAffltns_IAAffltn._orgPk').alias('Firm_CRD_Number'),
        ).cast(IA_Affiliations_Schema
        ).alias('IA_Affiliations'),
    arrays_zip(
        col('OthrNms_OthrNm.Nm._last').alias('Last_Name'),
        col('OthrNms_OthrNm.Nm._first').alias('First_Name'),
        col('OthrNms_OthrNm.Nm._mid').alias('Middle_Name'),
        col('OthrNms_OthrNm.Nm._suf').alias('Suffix_Name'),
        col('OthrNms_OthrNm._seqNb').alias('Sequence_Number'),
        ).cast(Other_Names_Schema
        ).alias('Other_Names'),
    arrays_zip(
        col('ResHists_ResHist.Addr._strt1').alias('Street1'),
        col('ResHists_ResHist.Addr._strt2').alias('Street2'),
        col('ResHists_ResHist.Addr._city').alias('City'),
        col('ResHists_ResHist.Addr._state').alias('State'),
        col('ResHists_ResHist.Addr._cntryCd').alias('Country'),
        col('ResHists_ResHist.Addr._postlCd').alias('Postal_Code'),
        col('ResHists_ResHist.DtRng._fromDt').alias('From_Date'),
        col('ResHists_ResHist.DtRng._toDt').alias('To_Date'),
        col('ResHists_ResHist._seqNb').alias('Sequence_Number'),
        ).cast(Address_History_Schema
        ).alias('Address_History'),
    struct(
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_strt1').getItem(0).alias('Street1'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_strt2').getItem(0).alias('Street2'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_city').getItem(0).alias('City'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_state').getItem(0).alias('State'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_cntryCd').getItem(0).alias('Country'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('Addr').getField('_postlCd').getItem(0).alias('Postal_Code'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_fromDt').getItem(0).alias('From_Date'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_toDt').getItem(0).alias('To_Date'),
        filter(
            col = col('ResHists_ResHist'), 
            f = filter_Current_Date
            ).getField('_seqNb').getItem(0).alias('Sequence_Number'),
        ).alias('Current_Address'),
    arrays_zip(
        col('Dsgntns_Dsgntn._cd').alias('Designation_Code'),
        col('Dsgntns_Dsgntn._auth').alias('Designating_Authority'),
        col('Dsgntns_Dsgntn._flngDt').alias('Date_First_Filing'),
        ).cast(Designations_Schema
        ).alias('Designations'),
    col('ContEds_baseDt').alias('Continuing_Education_Date'),
    col('ContEds_st').alias('Continuing_Education_Status'),
    arrays_zip(
        col('ContEds_ContEd.Appts.Appt').alias('Appointments'),
        col('ContEds_ContEd._begDt').alias('Start_Date'),
        col('ContEds_ContEd._endDt').alias('End_Date'),
        col('ContEds_ContEd._eventDt').alias('Information_Changed_Date'),
        col('ContEds_ContEd._frgnDfrdFl').alias('Foreign_Deferred'),
        col('ContEds_ContEd._mltryDfrdFl').alias('Military_Deferred'),
        col('ContEds_ContEd._nrlmtID').alias('Enrollment_ID'),
        col('ContEds_ContEd._rslt').alias('Result'),
        col('ContEds_ContEd._sssn').alias('Session_Type'),
        col('ContEds_ContEd._sssnDt').alias('Session_Date'),
        col('ContEds_ContEd._sssnReqSt').alias('Session_Status_Type'),
        col('ContEds_ContEd._sssnSt').alias('Session_Status'),
        ).cast(Continuing_Education_Details_Schema
        ).alias('Continuing_Education_Details'),
    # arrays_zip(
    #     col('EventFlngHists_EventFlngHist._dt').alias('Filing_Date'),
    #     col('EventFlngHists_EventFlngHist._flngType').alias('Filing_Type'),
    #     col('EventFlngHists_EventFlngHist._frmType').alias('Form_Type'),
    #     col('EventFlngHists_EventFlngHist._id').alias('Filing_ID'),
    #     col('EventFlngHists_EventFlngHist._src').alias('Submitted_By'),
    #     col('EventFlngHists_EventFlngHist._type').alias('Event_Type'),
    #     ).cast(Filing_History_Schema
    #     ).alias('Filing_History'),
    arrays_zip(
        col('OthrBuss_OthrBus._desc').alias('Description'),
        ).cast(Other_Businesses_Schema
        ).alias('Other_Businesses'),
    col('IdentInfo_dob').alias('Identification_Birth_Date'),
    col('IdentInfo_birthStCd').alias('Identification_Birth_State'),
    col('IdentInfo_birthPrvnc').alias('Identification_Birth_Province'),
    col('IdentInfo_birthCntryCd').alias('Identification_Birth_Country'),
    col('IdentInfo_gender').alias('Identification_Gender'),
    struct(
        col('IdentInfo_Ht_ft').alias('Height_ft'),
        col('IdentInfo_Ht_in').alias('Height_in'),
        col('IdentInfo_eye').alias('Eye_Color'),
        col('IdentInfo_hair').alias('Hair_Color'),
        col('IdentInfo_wt').alias('Weight'),
        ).alias('Identification_Personal'),
    arrays_zip(
        col('FngprInfos_FngprInfo._barCd').alias('Card_Bar_Code'),
        col('FngprInfos_FngprInfo._orgNm').alias('Firm_Submitted'),
        col('FngprInfos_FngprInfo._pstnInFirm').alias('Position'),
        col('FngprInfos_FngprInfo._st').alias('Card_Status'),
        col('FngprInfos_FngprInfo._stDt').alias('Card_Status_Date'),
        col('FngprInfos_FngprInfo._recdDt').alias('Card_Received_Date'),
        ).cast(Finger_Print_Info_Schema
        ).alias('Finger_Print_Info'),
    arrays_zip(
        col('Exms_Exm.Appts.Appt').alias('Appointments'),
        col('Exms_Exm._exmCd').alias('Exam_Code'),
        col('Exms_Exm._exmDt').alias('Exam_Date'),
        col('Exms_Exm._exmValidFl').alias('Currently_Valid'),
        col('Exms_Exm._exmValidDt').alias('Valid_Until'),
        col('Exms_Exm._grd').alias('Grade'),
        col('Exms_Exm._scr').alias('Score'),
        col('Exms_Exm._nrlmtID').alias('Enrollment_ID'),
        col('Exms_Exm._st').alias('Exam_Status'),
        col('Exms_Exm._stDt').alias('Exam_Status_Date'),
        col('Exms_Exm._updateTS').alias('Date_Updated'),
        col('Exms_Exm._wndwBeginDt').alias('Window_Begin_Date'),
        col('Exms_Exm._wndwEndDt').alias('Window_End_Date'),
        ).cast(Exams_Schema
        ).alias('Exams'),
    arrays_zip(
        col('ExmWvrs_ExmWvr._nrlmtID').alias('Enrollment_ID'),
        col('ExmWvrs_ExmWvr._exmCd').alias('Exam_Code'),
        col('ExmWvrs_ExmWvr._regAuth').alias('Regulator'),
        col('ExmWvrs_ExmWvr._rsnCd').alias('Waiver_Reason'),
        col('ExmWvrs_ExmWvr._exmValidFl').alias('Currently_Valid'),
        col('ExmWvrs_ExmWvr._exmValidDt').alias('Valid_Until'),
        col('ExmWvrs_ExmWvr._efctvDt').alias('Waived_Date'),
        ).cast(Exam_Waivers_Schema
        ).alias('Exam_Waivers'),
    arrays_zip(
        col('EmpHists_EmpHist.DtRng._fromDt').alias('From_Date'),
        col('EmpHists_EmpHist.DtRng._toDt').alias('To_Date'),
        col('EmpHists_EmpHist._city').alias('City'),
        col('EmpHists_EmpHist._cntryCd').alias('Country'),
        col('EmpHists_EmpHist._invRel').alias('Investment_Related'),
        col('EmpHists_EmpHist._orgNm').alias('Organization_Name'),
        col('EmpHists_EmpHist._postlCd').alias('Postal_Code'),
        col('EmpHists_EmpHist._pstnHeld').alias('Position'),
        col('EmpHists_EmpHist._seqNb').alias('Sequence_Number'),
        col('EmpHists_EmpHist._state').alias('State'),
        ).cast(Employment_History_Schema
        ).alias('Employment_History'),
    arrays_zip(
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_fromDt').alias('From_Date'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_toDt').alias('To_Date'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_city').alias('City'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_cntryCd').alias('Country'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_invRel').alias('Investment_Related'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_orgNm').alias('Organization_Name'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_postlCd').alias('Postal_Code'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_pstnHeld').alias('Position'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_seqNb').alias('Sequence_Number'),
        filter(
            col = col('EmpHists_EmpHist'), 
            f = filter_Current_Date
            ).getField('_state').alias('State'),
        ).cast(Employment_History_Schema
        ).alias('Current_Employment'),
    arrays_zip(
        col('AffltdFirms_AffltdFirm.EmpLocs.EmpLoc').alias('Employment_Locations'),
        col('AffltdFirms_AffltdFirm._orgNm').alias('Firm_Name'),
        col('AffltdFirms_AffltdFirm._orgPK').alias('Firm_CRD_Number'),
        col('AffltdFirms_AffltdFirm._empStDt').alias('Date_Hired'),
        col('AffltdFirms_AffltdFirm._ndpndCntrcrFl').alias('Independent_Contractor'),
        ).cast(Affilated_Firms_Schema
        ).alias('Affilated_Firms'),
    arrays_zip(
        col('OffHists_OffHist.DtRng._fromDt').alias('From_Date'),
        col('OffHists_OffHist.DtRng._toDt').alias('To_Date'),
        col('OffHists_OffHist.EmpLocs.EmpLoc').alias('Employment_Locations'),
        col('OffHists_OffHist._empCntxt').alias('Employment_Type'),
        col('OffHists_OffHist._firmAsctnSt').alias('Firm_Association'),
        col('OffHists_OffHist._ndpndCntrcrFl').alias('Independent_Contractor'),
        col('OffHists_OffHist._orgNm').alias('Firm_Name'),
        col('OffHists_OffHist._orgPK').alias('Firm_CRD_Number'),
        col('OffHists_OffHist._termDt').alias('Termination_Date'),
        col('OffHists_OffHist._termExpln').alias('Termination_Explanation'),
        col('OffHists_OffHist._termRsn').alias('Termination_Reason'),
        col('OffHists_OffHist._termRsnAmndtExpln').alias('Termination_Amendment_Explanation'),
        col('OffHists_OffHist._termDtAmndtExpln').alias('Termination_Date_Amendment_Explanation'),
        ).cast(Office_Employment_History_Schema
        ).alias('Office_Employment_History'),
    arrays_zip(
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_fromDt').alias('From_Date'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('DtRng').getField('_toDt').alias('To_Date'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('EmpLocs').getField('EmpLoc').alias('Employment_Locations'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_empCntxt').alias('Employment_Type'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_firmAsctnSt').alias('Firm_Association'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_ndpndCntrcrFl').alias('Independent_Contractor'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_orgNm').alias('Firm_Name'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_orgPK').alias('Firm_CRD_Number'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_termDt').alias('Termination_Date'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_termExpln').alias('Termination_Explanation'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_termRsn').alias('Termination_Reason'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_termRsnAmndtExpln').alias('Termination_Amendment_Explanation'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date
            ).getField('_termDtAmndtExpln').alias('Termination_Date_Amendment_Explanation'),
        ).cast(Office_Employment_History_Schema
        ).alias('Current_Office'),
    filter(
        col = col('OffHists_OffHist'), 
        f = filter_Current_Date_and_Firm(crd_number=file_meta['crd_number'])
        ).getField('_orgNm').getItem(0).alias('Current_Office_Firm_Name'),
    filter(
        col = col('OffHists_OffHist'), 
        f = filter_Current_Date_and_Firm(crd_number=file_meta['crd_number'])
        ).getField('_empCntxt').getItem(0).alias('Current_Office_Employee_Type'),
    filter(
        col = col('OffHists_OffHist'), 
        f = filter_Current_Date_and_Firm(crd_number=file_meta['crd_number'])
        ).getField('_ndpndCntrcrFl').getItem(0).alias('Current_Office_Independent_Contractor'),
    filter(
        col = col('OffHists_OffHist'), 
        f = filter_Current_Date_and_Firm(crd_number=file_meta['crd_number'])
        ).getField('_firmAsctnSt').getItem(0).alias('Current_Office_Firm_Association'),
    filter(
        col = col('OffHists_OffHist'), 
        f = filter_Current_Date_and_Firm(crd_number=file_meta['crd_number'])
        ).getField('DtRng').getField('_fromDt').getItem(0).alias('Current_Office_From_Date'),



)

individual = individual.persist()


semi_flat_table.printSchema()
print('\n')
individual.printSchema()

pprint(individual.limit(10).toJSON().map(lambda j: json.loads(j)).collect())


pprint(individual.where('CRD_Number = 716965').toJSON().map(lambda j: json.loads(j)).collect())




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


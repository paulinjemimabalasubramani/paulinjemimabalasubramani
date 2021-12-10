"""
Library for Building Custom FINRA Tables

"""


# %% Import Libraries

from .common_functions import logger, catch_error

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, lit, concat_ws, arrays_zip, filter, struct




# %% Build Branch Table

@catch_error(logger)
def build_branch_table(semi_flat_table):
    """
    Build Custom Finra Branch Table from given semi-flattened raw branch table.
    """

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


    branch = semi_flat_table.select(
        #col('_orgPK').alias('Firm_CRD_Number'),
        col('BrnchOfcs_BrnchOfc_brnchPK').alias('Branch_CRD_Number'),
        col('BrnchOfcs_BrnchOfc_bllngCd').alias('Billing_Code'),
        col('BrnchOfcs_BrnchOfc_brnchPhone').alias('Branch_Phone'),
        col('BrnchOfcs_BrnchOfc_brnchFax').alias('Branch_Fax'),
        col('BrnchOfcs_BrnchOfc_prvtRsdnc').alias('Is_Private_Residence'),
        col('BrnchOfcs_BrnchOfc_dstrtPK').alias('District_Office'),
        col('BrnchOfcs_BrnchOfc_oprnlStCd').alias('Operational_Status'),
        struct(
            col('BrnchOfcs_BrnchOfc_Addr_cntry').alias('Country'),
            col('BrnchOfcs_BrnchOfc_Addr_state').alias('State'),
            col('BrnchOfcs_BrnchOfc_Addr_city').alias('City'),
            col('BrnchOfcs_BrnchOfc_Addr_postlCd').alias('Postal_Code'),
            col('BrnchOfcs_BrnchOfc_Addr_strt1').alias('Street1'),
            col('BrnchOfcs_BrnchOfc_Addr_strt2').alias('Street2'),
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

    return branch





# %% Build Individual Table

@catch_error(logger)
def build_individual_table(semi_flat_table, crd_number:str):
    """
    Build Custom Finra Individual Table from given semi-flattened raw Individual table.
    """

    def filter_Current_Date(x):
        return (x.getField('DtRng').getField('_toDt').isNull()) | (x.getField('DtRng').getField('_toDt') == lit(''))


    def filter_Current_Date_zipped(x):
        return (x.getField('To_Date').isNull()) | (x.getField('To_Date') == lit(''))


    def filter_Current_Date_and_Firm(crd_number:str):
        def inner_filter_func(x):
            return (((x.getField('DtRng').getField('_toDt').isNull()) | (x.getField('DtRng').getField('_toDt') == lit(''))) 
                & (x.getField('_orgPK') == lit(crd_number)))
        return inner_filter_func


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


    Affiliated_Firms_Schema = ArrayType(StructType([
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


    State_Registrations_Deficiencies_Schema = ArrayType(StructType([
        StructField('Blank', StringType(), True),
        StructField('Current_Registration_Deficiency', StringType(), True),
        StructField('Exam_Code', StringType(), True),
        StructField('Date_Deficiency_Created', StringType(), True),
        ]), True)


    State_Registrations_Schema = ArrayType(StructType([
        StructField('Deficiencies', State_Registrations_Deficiencies_Schema, True),
        StructField('Active_Registration', StringType(), True),
        StructField('Approval_Date', StringType(), True),
        StructField('Date_Registration_Created', StringType(), True),
        StructField('Employment_Start_Date', StringType(), True),
        StructField('Regulatory_Authority', StringType(), True),
        StructField('Registration_Category', StringType(), True),
        StructField('Registration_Status', StringType(), True),
        StructField('Date_Status_Change', StringType(), True),
        StructField('Date_Registration_Terminated', StringType(), True),
        StructField('Date_Updated', StringType(), True),
        ]), True)


    DRP_Schema = ArrayType(StructType([
        StructField('Occurrence_Number', StringType(), True),
        StructField('Disclosure_Type', StringType(), True),
        StructField('Public_Disclosable', StringType(), True),
        StructField('Reportable', StringType(), True),
        StructField('Form_Type', StringType(), True),
        StructField('Received_Date', StringType(), True),
        StructField('Source', StringType(), True),
        StructField('Form_Version', StringType(), True),
        StructField('Disclosure_Questions', StringType(), True),
        ]), True)


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
        arrays_zip(
            col('EventFlngHists_EventFlngHist._dt').alias('Filing_Date'),
            col('EventFlngHists_EventFlngHist._flngType').alias('Filing_Type'),
            col('EventFlngHists_EventFlngHist._frmType').alias('Form_Type'),
            col('EventFlngHists_EventFlngHist._id').alias('Filing_ID'),
            col('EventFlngHists_EventFlngHist._src').alias('Submitted_By'),
            col('EventFlngHists_EventFlngHist._type').alias('Event_Type'),
            ).cast(Filing_History_Schema
            ).alias('Filing_History'),
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
        filter(
            col = arrays_zip(
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
                ),
            f = filter_Current_Date_zipped
                ).alias('Current_Employment'),
        arrays_zip(
            col('AffltdFirms_AffltdFirm.EmpLocs.EmpLoc').alias('Employment_Locations'),
            col('AffltdFirms_AffltdFirm._orgNm').alias('Firm_Name'),
            col('AffltdFirms_AffltdFirm._orgPK').alias('Firm_CRD_Number'),
            col('AffltdFirms_AffltdFirm._empStDt').alias('Date_Hired'),
            col('AffltdFirms_AffltdFirm._ndpndCntrcrFl').alias('Independent_Contractor'),
            ).cast(Affiliated_Firms_Schema
            ).alias('Affiliated_Firms'),
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
        filter(
            col = arrays_zip(
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
                ),
            f = filter_Current_Date_zipped
            ).alias('Current_Office'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date_and_Firm(crd_number=crd_number)
            ).getField('_orgNm').getItem(0).alias('Current_Office_Firm_Name'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date_and_Firm(crd_number=crd_number)
            ).getField('_empCntxt').getItem(0).alias('Current_Office_Employee_Type'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date_and_Firm(crd_number=crd_number)
            ).getField('_ndpndCntrcrFl').getItem(0).alias('Current_Office_Independent_Contractor'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date_and_Firm(crd_number=crd_number)
            ).getField('_firmAsctnSt').getItem(0).alias('Current_Office_Firm_Association'),
        filter(
            col = col('OffHists_OffHist'), 
            f = filter_Current_Date_and_Firm(crd_number=crd_number)
            ).getField('DtRng').getField('_fromDt').getItem(0).alias('Current_Office_From_Date'),
        arrays_zip(
            col('CrntRgstns_CrntRgstn.CrntDfcnys.CrntDfcny').alias('Deficiencies'),
            col('CrntRgstns_CrntRgstn._actvReg').alias('Active_Registration'),
            col('CrntRgstns_CrntRgstn._aprvlDt').alias('Approval_Date'),
            col('CrntRgstns_CrntRgstn._crtnDt').alias('Date_Registration_Created'),
            col('CrntRgstns_CrntRgstn._empStDt').alias('Employment_Start_Date'),
            col('CrntRgstns_CrntRgstn._regAuth').alias('Regulatory_Authority'),
            col('CrntRgstns_CrntRgstn._regCat').alias('Registration_Category'),
            col('CrntRgstns_CrntRgstn._st').alias('Registration_Status'),
            col('CrntRgstns_CrntRgstn._stDt').alias('Date_Status_Change'),
            col('CrntRgstns_CrntRgstn._trmnnDt').alias('Date_Registration_Terminated'),
            col('CrntRgstns_CrntRgstn._updateTS').alias('Date_Updated'),
            ).cast(State_Registrations_Schema
            ).alias('State_Registrations'),
        arrays_zip(
            col('DRPs_OcrnInfo._ocrn').alias('Occurrence_Number'),
            col('DRPs_OcrnInfo._dsclrType').alias('Disclosure_Type'),
            col('DRPs_OcrnInfo._pblcDscl').alias('Public_Disclosable'),
            col('DRPs_OcrnInfo._rptbl').alias('Reportable'),
            col('DRPs_OcrnInfo._frm').alias('Form_Type'),
            col('DRPs_OcrnInfo._rcvdDt').alias('Received_Date'),
            col('DRPs_OcrnInfo._src').alias('Source'),
            col('DRPs_OcrnInfo._frmVer').alias('Form_Version'),
            col('DRPs_OcrnInfo._qstns').alias('Disclosure_Questions'),
            ).cast(DRP_Schema
            ).alias('DRP'),
    )

    return individual




# %%




import os
import sys
import logging
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'modules')))
from common_sql_functions import set_pipeline_params, extract_data

pipeline_name = 'extract_lr'

log_file_name = f"{os.getenv('app_folder')}\\logs\\{pipeline_name}_log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S.log')}"
logging.basicConfig(level=logging.INFO, filename=log_file_name, format='%(asctime)s - %(levelname)s - %(message)s')

database_name='LR'

db_server = os.getenv('LR_DB_SERVER')

connection_string = f'DRIVER={{SQL Server}};SERVER={db_server};DATABASE={database_name};Trusted_Connection=yes;'

tables = [
    'history.registration',
    'Affirm.JointRep',
    'Affirm.Lookup',
    'Affirm.Ownershiptype',
    'Affirm.RepHierarchy',
    'Affirm.UserRelation',
    'BD.JointRep',
    'BD.NASDLicense',
    'BD.Office',
    'BD.Rep',
    'BD.RepHierarchy',
    'BD.StateLicense',
    'BDCon.BranchXref',
    'BDCon.IndividualBranchXref',
    'BDCon.RepCodeXref',
    'BRACS.Lookup',
    'dbo.DataDump',
    'dbo.Region',
    'dbo.TermedRepcodeListForReclaim',
    'dbo.USState',
    'OLTP.AlternateBranchNameIndicators',
    'OLTP.Appointment',
    'OLTP.AppointmentLOA',
    'OLTP.AvailableBranchCodes',
    'OLTP.AvailableCustodians',
    'OLTP.AvailableMBOCode',
    'OLTP.AvailableRepCode',
    'OLTP.AvailableRIACodes',
    'OLTP.Branch',
    'OLTP.BranchAddress',
    'OLTP.BranchContract',
    'OLTP.BranchExtract',
    'OLTP.BranchFinActvy',
    'OLTP.BranchInsuranceLicense',
    'OLTP.BranchInsuranceLicenseLOA',
    'OLTP.BranchOfficeHierarchy',
    'OLTP.BranchOtherBusActvy',
    'OLTP.BranchOtherName',
    'OLTP.BranchOtherWebSite',
    'OLTP.BranchRegistration',
    'OLTP.BranchShare',
    'OLTP.Comment',
    'OLTP.ContinuingEd',
    'OLTP.CorpInsuranceEntity',
    'OLTP.CorpInsuranceLicenseLOA',
    'OLTP.CorpInsuranceLicenses',
    'OLTP.Designation',
    'OLTP.EmploymentHistory',
    'OLTP.EmploymentLocation',
    'OLTP.EntityClearingTypeRefPoolIds',
    'OLTP.Exam',
    'OLTP.FilingHistory',
    'OLTP.FinancialInstitution',
    'OLTP.FinancialInstitutionAffiliatedRep',
	'OLTP.Firm',
	'OLTP.FirmAffiliation',
	'OLTP.HouseAccount',
	'OLTP.Individual',
	'OLTP.IndividualAddress',
	'OLTP.IndividualContact',
	'OLTP.IndividualOtherName',
	'OLTP.InsuranceCarrier',
	'OLTP.JointRep',
	'OLTP.JointRep_Info',
	'OLTP.JointRepMember',
	'OLTP.Lookup',
	'OLTP.Organization',
	'OLTP.OrgMember',
	'OLTP.Registration',
	'OLTP.RegistrationRenewal',
	'OLTP.RejectedAvailableRepCodes',
	'OLTP.RepCode',
	'OLTP.RepInsuranceLicenseLOA',
	'OLTP.RepInsuranceLicenses',
	'OLTP.RepODSQuery',
	'OLTP.RepTypeReference',
	'OLTP.RepTypeReferenceBD',
	'OLTP.RIA',
	'OLTP.RIARegistration',
	'OLTP.RVP',
	'OLTP.RVPMap',
	'OLTP.StateLicenseFees',
    'OltpBD.JointRep',
    'OltpBD.NASDLicense',
    'OltpBD.Office',
    'OltpBD.Rep',
    'OltpBD.RepHierarchy',
    'OltpBD.StateLicense',
    'Rep.BranchMaster',
    'Rep.RepCentral',
    'Rep.RepContact',
    'Rep.RepMaster', 
]


params_config = set_pipeline_params(pipeline_name, database_name, connection_string)
extract_data(params_config, tables)

# %%

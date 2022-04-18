"""
Common Library for extracting data from FINRA files header.

"""

# %% Import Libraries

import os
from datetime import datetime

from .common_functions import catch_error, data_settings, logger, execution_date_start
from .migrate_files import add_firm_to_table_name



# %% Parameters

finra_individual_delta_name = 'IndividualInformationReportDelta'
finra_individual_name = 'IndividualInformationReport'
finra_branch_name = 'BranchInformationReport'
finra_dualregistrations_file_name = 'INDIVIDUAL_-_DUAL_REGISTRATIONS_-_FIRMS_DOWNLOAD_-_'
finra_dualregistrations_name = 'DualRegistrations'

file_date_column_format = r'%Y-%m-%d'



# %% Extract metadata from finra file path

@catch_error(logger)
def extract_data_from_finra_file_path(file_path:str, get_firm_crd:bool=True):
    """"
    Extract metadata from finra file path
    """
    file_name = os.path.basename(file_path)
    file_name_noext, file_ext = os.path.splitext(file_name)

    file_meta = {
        'file_name': file_name,
        'file_path': file_path,
        'folder_path': os.path.dirname(file_path),
    }

    if get_firm_crd: file_meta['firm_crd_number'] = data_settings.firm_crd_number

    if file_name_noext.upper().startswith(finra_dualregistrations_file_name.upper()):
        table_name = finra_dualregistrations_name.lower()
        file_date = execution_date_start
    else:
        sp = file_name_noext.split("_")

        if get_firm_crd and sp[0].strip() != data_settings.firm_crd_number:
            logger.warning(f'Wrong Firm CRD Number in file name. Expected {data_settings.firm_crd_number}. File: {file_path} -> SKIPPING')
            return

        try:
            table_name = sp[1].strip().lower()
            if table_name == finra_individual_delta_name.lower():
                table_name = finra_individual_name.lower()

            file_date = datetime.strptime(sp[2].rsplit('.', 1)[0].strip(), file_date_column_format)

            assert len(sp)==3 or (len(sp)==4 and sp[1].strip().lower()==finra_individual_delta_name.lower())

        except Exception as e:
            logger.warning(f'Cannot parse file name: {file_path}. {str(e)}')
            return

    table_name_no_firm = table_name
    table_name = add_firm_to_table_name(table_name=table_name)

    file_meta = {**file_meta,
        'table_name_no_firm': table_name_no_firm.lower(),
        'table_name': table_name.lower(),
        'file_date': file_date,
    }

    return file_meta



# %%



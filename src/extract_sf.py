"""
Extract data from Salesforce - Triad Data

Spark Web UI:
http://10.128.25.82:8181/

Airflow:
http://10.128.25.82:8282/


"""

# %% Import Libraries

import os, sys, unicodecsv, csv
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'financial_professional'
sys.domain_abbr = 'FP'


# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))


from modules.common_functions import data_settings, get_secrets, logger, mark_execution_end, catch_error, \
    config_path


from salesforce_bulk import SalesforceBulk # https://github.com/heroku/salesforce-bulk
from time import sleep



# %% Parameters

tableinfo_source = 'SF'

salesforce_api_version = data_settings.salesforce_api_version
triad_salesforce_host = data_settings.triad_salesforce_host
is_triad_salesforce_sandbox = data_settings.is_triad_salesforce_sandbox
triad_salesforce_key_vault_account = data_settings.triad_salesforce_key_vault_account

data_path_folder = data_settings.get_value(attr_name=f'data_path_{tableinfo_source}', default_value=os.path.join(data_settings.data_path, tableinfo_source))
salesforce_download_path_folder = os.path.join(config_path, 'salesforce_download')


csv_encoding = 'utf-8'
csv_delimiter = ','
csv_quotechar = '"'

logger.info({
    'data_path_folder': data_path_folder,
    })



# %% Clean dictionary values from enter and tab characters

@catch_error(logger)
def clean_dictionary(src_dict:dict):
    """
    Clean dictionary values from enter and tab characters
    """
    for key in src_dict:
        if src_dict[key] is not None:
            src_dict[key] = src_dict[key].replace('\n',"").replace('\t',"").replace('\r',"")
    return src_dict



# %% Download data from Salesforce and save as CSV file

@catch_error(logger)
def salesforce_download_csv(bulk, query:str, salesforce_object:str, csv_columns:list, output_file:str):
    """
    Download data from Salesforce and save as CSV file
    """
    job = bulk.create_query_job(salesforce_object, contentType='CSV')
    batch = bulk.query(job, query)
    bulk.close_job(job)
    while not bulk.is_batch_done(batch): sleep(2)

    with open(output_file, mode='w', newline='', encoding=csv_encoding) as out_file:
        out_writer = csv.DictWriter(out_file, delimiter=csv_delimiter, quotechar=csv_quotechar, quoting=csv.QUOTE_ALL, skipinitialspace=True, fieldnames=csv_columns)
        out_writer.writeheader()
        for result in bulk.get_all_results_for_query_batch(batch):
            reader = unicodecsv.DictReader(result, encoding=csv_encoding)
            for row in reader:
                clean_dict = clean_dictionary(row)
                out_writer.writerow(clean_dict)



# %% Query data from Salesforce and save as CSV file

@catch_error(logger)
def query_download_salesforce_csv(bulk, filter_query:str, salesforce_object:str, csv_columns:str, data_path_folder:str, output_table_name:str):
    """
    Query and Download data from Salesforce and save as CSV file
    """
    query = 'SELECT ' + csv_columns + ' FROM ' + salesforce_object
    if filter_query and filter_query.strip(): query += ' WHERE ' + filter_query

    output_file = os.path.join(data_path_folder, output_table_name+'.csv')

    logger.info({
        'query': query,
        'output_file': output_file
    })

    salesforce_download_csv(bulk=bulk, query=query, salesforce_object=salesforce_object, csv_columns=csv_columns.split(','), output_file=output_file)



# %% Download all Salesforce Objects for given source/firm file

@catch_error(logger)
def download_all_salesforce_objects(download_file:str, salesforce_key_vault_account:str, salesforce_host:str, is_sandbox:bool):
    """
    Download all Salesforce Objects for given source/firm file
    """
    download_file_full_path = os.path.join(salesforce_download_path_folder, download_file)

    _, salesforce_id, salesforce_pass, salesforce_token = get_secrets(salesforce_key_vault_account.lower(), logger=logger, additional_secrets=['token'])

    bulk = SalesforceBulk(
        username = salesforce_id,
        password = salesforce_pass,
        security_token = salesforce_token,
        API_version = salesforce_api_version,
        host = salesforce_host,
        sandbox = is_sandbox,
        )

    with open(download_file_full_path, newline='', mode='r', encoding='utf-8-sig') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            query_download_salesforce_csv(
                bulk = bulk,
                filter_query = row['filter_query'],
                salesforce_object = row['object_name'],
                csv_columns = row['csv_columns'],
                data_path_folder = data_path_folder,
                output_table_name = row['table_name'],
                )



download_all_salesforce_objects(
    download_file = 'triad.csv',
    salesforce_key_vault_account = triad_salesforce_key_vault_account,
    salesforce_host = triad_salesforce_host,
    is_sandbox = is_triad_salesforce_sandbox,
    )



# %% Mark Execution End

mark_execution_end()


# %%




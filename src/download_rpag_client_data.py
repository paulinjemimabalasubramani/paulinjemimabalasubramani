if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='CA_MIGRATE_RPAG')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'CA_MIGRATE_RPAG',
        'source_path': r'C:\myworkdir\data\MIGRATE_RPAG',
        }


import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, normalize_name, get_csv_rows, is_pc, get_secrets
import requests,json,csv
import time

azure_kv_rpag_oas_account_name = 'rpag-oas'
azure_kv_rpag_owi_account_name = 'rpag-owi'
azure_kv_rpag_oii_account_name = 'rpag-oii'

@catch_error(logger)
def call_api(http_method,url,headers=None,data1=None,params1=None,max_retries=3, backoff_factor=1):
    for attempt in range(max_retries):
        try:
            response = requests.request(
                    method = http_method,
                    url = url,
                    headers = headers,
                    data = data1,
                    params = params1,
                    timeout = 180,
                    )
            response.raise_for_status()
            return response.json()  # Return the JSON response if successful
        except requests.exceptions.RequestException as e:
            logger.warning(f'Attempt {attempt + 1} failed: {e}')
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2 ** attempt)  # Exponential backoff
                logger.warning(f'Retrying in {sleep_time} seconds...')
                time.sleep(sleep_time)
            else:
                logger.error(f'Max retries reached for {url}. Exiting.')
                raise e

@catch_error(logger)
def get_oauth_rpag_token(broker_delear_key:str):
    
    payload = {
        'clientID': '',    
        'ClientSecret': '',
        'userName': '',
        'password':''
    }

    response = call_api("POST",data_settings.rpag_token_url,headers={ 'Content-Type': 'application/json'},data1=json.dumps(payload))
    logger.info(f'Retrieved token for broker dealer : {broker_delear_key}')
    return response['AccessToken']


@catch_error(logger)
def download_rpag_client_data():
    
    output_file_name=f'rpag_client_plan_crm.csv'
    os.makedirs(data_settings.source_path, exist_ok=True)
        
    output_file = open(os.path.join(data_settings.source_path, output_file_name), mode='wt', newline='', encoding='utf-8')
        
    fieldnames=['client_id','firm_name','client_details']
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(fieldnames)

    broker_delears = ['owi']
    
    for broker_dealer in broker_delears:
    
        headers = {
            "Token": get_oauth_rpag_token(broker_dealer), 
            "Content-Type": "application/json",
            "User-Agent" : ""
        }
        
        results:list =  call_api("GET", data_settings.rpag_client_data_url, headers=headers)
        logger.info(f'Retrieved client data for broker dealer : {broker_dealer}')
        
        for item in results:
            csv_writer.writerow([item['clientId'],broker_dealer.upper(),json.dumps(item)])

if __name__ == '__main__':
    download_rpag_client_data()
    mark_execution_end()


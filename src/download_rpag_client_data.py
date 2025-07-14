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

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, normalize_name, get_csv_rows, is_pc,get_azure_key_vault, get_secrets
from modules3.snowflake_ddl import connect_to_snowflake
from datetime import datetime
import requests,json,csv
import time
        
def is_valid_day_to_run_job(day_indicator: str, day_in_number: int) -> bool:
    """
    Checks if the current date is a valid day to run the job based on day_indicator and day_in_number.
    """
    is_valid = False
    current_date = datetime.now().strftime('%Y-%m-%d') # Format for comparison with CALENDARDATE
    current_year = datetime.now().year
    current_month = datetime.now().month

    try:
        
        sf_conn = connect_to_snowflake()

        cursor = sf_conn.cursor()

        query = f"""
        SELECT CALENDARDATE, IsWEEKDAY, IsHoliday
        FROM DATAHUB.PIPELINE_METADATA.DIM_DATE_REF
        WHERE YearNumber = '{current_year}' AND MonthNumber = '{current_month}';
        """

        cursor.execute(query)
        dim_date_ref_data = cursor.fetchall()
        cursor.close()
        sf_conn.close()
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake or fetch data: {e}")
        return False # If we can't get date info, we can't validate

    count = 0
    for row in dim_date_ref_data:
        calendar_date = row[0].strftime('%Y-%m-%d') # Assuming CALENDARDATE is a datetime object
        is_weekday = row[1]
        is_holiday = row[2]

        if ((day_indicator == 'BD' and is_weekday == 'Y' and is_holiday == 'N') or
            (day_indicator == 'CD')):
            count += 1
            if count == day_in_number and calendar_date == current_date:
                is_valid = True
                break # Found the day, no need to continue looping
            elif calendar_date == current_date and count != day_in_number:
                # If we are on the current date but the count doesn't match, it's not valid for this config
                break
    return is_valid


def get_valid_day_to_run_job(job_run_config_string: str) -> bool:
    """
    Parses the job run configuration string and checks if the job should proceed today.
    Example: "BD=[5,6],CD=[7]"
    """
    is_valid = False
    if job_run_config_string is None:
        return is_valid

    # Parse the configuration string: "BD=[5,6],CD=[7]" -> {'BD': ['5', '6'], 'CD': ['7']}
    parsed_config = {}
    parts = job_run_config_string.split(',')
    for part in parts:
        key_value = part.split('=')
        if len(key_value) == 2:
            key = key_value[0].strip()
            values_str = key_value[1].strip('[]')
            values = [int(v.strip()) for v in values_str.split(',') if v.strip()]
            parsed_config[key] = values

    # Loop through the parsed configurations and check for today
    for day_indicator, day_numbers in parsed_config.items():
        for day_in_number in day_numbers:
            if is_valid_day_to_run_job(day_indicator, day_in_number):
                is_valid = True
                break # Found a valid configuration for today, no need to check further
        if is_valid:
            break
    return is_valid


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
                    timeout = None,
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

    azure_rpag_kv = {'OII': 'rpag-oii','OWI': 'rpag-owi',}
    azure_kv_rpag_account_name = azure_rpag_kv[broker_delear_key]

    payload = {
        'clientID': azure_client.get_secret(f'{env_name}-{azure_kv_rpag_account_name}-client-id').value,    
        'ClientSecret': azure_client.get_secret(f'{env_name}-{azure_kv_rpag_account_name}-client-secret').value,
        'userName': azure_client.get_secret(f'{env_name}-{azure_kv_rpag_account_name}-username').value,
        'password': azure_client.get_secret(f'{env_name}-{azure_kv_rpag_account_name}-password').value
    }

    response = call_api("POST",data_settings.rpag_token_url,headers={ 'Content-Type': 'application/json'},data1=json.dumps(payload))
    logger.info(f'Retrieved token for broker dealer : {broker_delear_key}')
    return response['AccessToken']


@catch_error(logger)
def download_rpag_client_data():
    
    output_file_name=f'crm_client_plan.csv'
    os.makedirs(data_settings.source_path, exist_ok=True)
        
    output_file = open(os.path.join(data_settings.source_path, output_file_name), mode='wt', newline='', encoding='utf-8')
        
    fieldnames=['client_id','firm_name','client_plan']
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(fieldnames)

    run_date = datetime.now().strftime('%Y%m%d')

    broker_delears = data_settings.broker_dealers.split(',')
    
    for broker_dealer in broker_delears:
    
        headers = {
            "Token": get_oauth_rpag_token(broker_dealer), 
            "Content-Type": "application/json",
            "User-Agent" : ""
        }
        
        results:list =  call_api("GET", data_settings.rpag_client_data_url, headers=headers)
        logger.info(f'Retrieved client data for broker dealer : {broker_dealer}')

        for item in results:
            csv_writer.writerow([item['clientId'],broker_dealer.upper(),run_date,json.dumps(item)])

if __name__ == '__main__':  
    logger.info(f"Checking if today is a valid day to run the job with config: {data_settings.job_run_bd}")
    if get_valid_day_to_run_job(data_settings.job_run_bd):
        logger.info("Today is a valid day to run the job. Proceeding with RPAG extraction.")
        download_rpag_client_data()
        mark_execution_end()
    else:
        logger.info("Today is NOT a valid day to run the job based on configuration. Exiting.")
        sys.exit(0) # Exit gracefully if not a valid day  

azure_tenant_id, azure_client = get_azure_key_vault()
env_name = sys.app.environment.lower()

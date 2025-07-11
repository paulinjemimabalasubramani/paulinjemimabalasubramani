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
from datetime import datetime
import requests,json,csv
import time
from modules3.snowflake_ddl import connect_to_snowflake


@catch_error(logger)
def is_fifth_business_day(conn):
    """
    Checks if today is the 5th business day of the month by querying Snowflake.
    """
    try:
        # Connect using snowflake-connector-python
        cursor = conn.cursor()

        current_year = datetime.now().year
        current_month = datetime.now().month

        query = f"""
            SELECT CALENDARDATE
            FROM DATAHUB.PIPELINE_METADATA.DIM_DATE_REF
            WHERE YearNumber = '{current_year}'
              AND MonthNumber = '{current_month}'
              AND IsWEEKDAY = 'Y'
              AND IsHoliday = 'N'
            ORDER BY CALENDARDATE ASC
            LIMIT 5;
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        if len(rows) >= 5:
            fifth_business_day = rows[4][0].date()
            today = datetime.now().date()

            if today == fifth_business_day:
                logger.info(f"Today ({today}) is the 5th business day ({fifth_business_day}).")
                return True
            else:
                logger.info(f"Today ({today}) is NOT the 5th business day ({fifth_business_day}).")
                return False
        else:
            logger.warning(f"Could not find 5 business days for {current_month}/{current_year} in DIM_DATE_REF.")
            return False

    except Exception as e:
        logger.error(f"Error checking 5th business day from Snowflake: {e}")
        return False # Fail gracefully or re-raise if this is a hard requirement
    finally:
        cursor.close()
        conn.close()


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
    
    snowflake_conn = connect_to_snowflake()

    if not is_fifth_business_day(snowflake_conn):
        logger.info("Not the 5th business day. Skipping data download.")
        sys.exit(0)

    logger.info("It is the 5th business day. Proceeding with data download.")

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
    download_rpag_client_data()
    mark_execution_end()

azure_tenant_id, azure_client = get_azure_key_vault()
env_name = sys.app.environment.lower()

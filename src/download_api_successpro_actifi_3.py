description = """
Download data from Successpro Actifi REST API
https://documenter.getpostman.com/view/2614838/S11GQegf?version=latest

"""

# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'REPLICA_MIGRATE_SUCCESSPRO_ACTIFI',
        'source_path': r'C:\myworkdir\data\SUCCESSPRO_ACTIFI',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, normalize_name, get_csv_rows, is_pc, get_secrets

from typing import Dict
import requests, json, csv



# %%

domain = 'advisorgroup'

#_, client_id, client_secret = get_secrets(account_name='actifi')
client_id = 'apiuser@advisorgroup.actifi'
client_secret = os.environ.get(key='SUCCESSPRO_ACTIFI_SECRET', default='')

csv_encoding = 'utf-8'
csv_delimiter = ','
csv_quotechar = '"'

path_names = {
    'roadmaps': 'roadmaps',
    'users': 'users',
    'userAssessments': 'user_assessments',
    'userAssessmentAnswers': 'user_assessment_answers',
    'userAssessmentResults': 'user_assessment_results',
    'userMeetings': 'user_meetings',
}



# %%

class SuccessproActifi:
    """
    Class to inteact with Successpro Actifi REST API
    https://documenter.getpostman.com/view/2614838/S11GQegf?version=latest
    """
    @catch_error(logger)
    def __init__(self, domain:str) -> None:
        """
        Initialize the class
        """
        self.max_request_attempts = 3
        self.domain = domain
        self.domain_url = f'https://{self.domain}.actifi.com'
        self.api_url = f'{self.domain_url}/n/api/v3/rest/'


    @catch_error(logger)
    def request(self, method:str='GET', path_name:str='', data:dict={}, headers:dict={}, params:dict={}, table_name:str=None) -> Dict:
        """
        Make an API Request
        """
        if not headers:
            headers = self.headers

        results:list = []
        len_results:int = 0
        first_time:bool = True

        while True:
            if first_time:
                url = f'{self.api_url}{path_name}'
                data1 = json.dumps(data) if data else None
                params1 = params
            else:
                url = f'{self.domain_url}/n' + response_data['nextPageUrl']
                data1 = None
                params1 = None # captured in url

            #logger.info(f'url = {url}')

            attempts = 0
            while True:
                response = requests.request(
                    method = method,
                    url = url,
                    headers = headers,
                    data = data1,
                    params = params1,
                    )

                if response.status_code == 200:
                    response_data = response.json()
                    results:list =  response_data['result']
                    if type(results)==dict:
                        results = [results]
                    len_results += len(results)
                    #logger.info(f'Total number of results = {len_results}')
                    break

                attempts += 1
                if attempts >= self.max_request_attempts:
                    raise Exception(f'API request failed with status code: {response.status_code}')

            if table_name and results:
                if first_time:
                    os.makedirs(data_settings.source_path, exist_ok=True)
                    table_name1 = normalize_name(table_name)
                    output_file_path = os.path.join(data_settings.source_path, table_name1 + '.csv')
                    logger.info(f'Saving to file {output_file_path}')
                    output_file = open(output_file_path, mode='wt', newline='', encoding=csv_encoding)
                    csv_writer = csv.DictWriter(output_file, delimiter=csv_delimiter, quotechar=csv_quotechar, quoting=csv.QUOTE_ALL, skipinitialspace=True, fieldnames=results[0].keys())
                    csv_writer.writeheader()
                csv_writer.writerows(results)

            if not ('nextPageUrl' in response_data.keys() and response_data['nextPageUrl'].strip() and response_data['result']):
                break

            first_time = False

        if table_name:
            try:
                output_file.close()
            except:
                pass

        return results # only last results are returned. All other results including last results are saved to file if enabled.


    @catch_error(logger)
    def authenticate(self, client_id:str, client_secret:str) -> None:
        """
        Authenticate to actifi REST API and get Access Token
        """
        path_name = 'auth/token'
        headers = {'Content-Type': 'application/json'}
        data = {
            'client_id': client_id,
            'client_secret': client_secret
            }

        response = self.request(method='POST', path_name=path_name, data=data, headers=headers)

        self.access_token = response[0]['accessToken']
        self.access_token_expires = response[0]['expiresOn']
        self.headers = {'Authorization': f'Bearer {self.access_token}'}



# %%

@catch_error(logger)
def fetch_user_profiles(sa:SuccessproActifi):
    """
    Fetch user_profiles data from API. Run this only after getting user.csv file.
    """
    table_name = 'user_profiles'
    user_file_path = os.path.join(data_settings.source_path, 'users.csv')
    output_file_path = os.path.join(data_settings.source_path, table_name + '.csv')
    logger.info(f'Saving to file {output_file_path}')

    first_time:bool = True
    with open(output_file_path, mode='wt', newline='', encoding=csv_encoding) as output_file:
        for i, row in enumerate(get_csv_rows(csv_file_path=user_file_path, csv_encoding=csv_encoding)):
            userid = row['userid']
            if is_pc:
                print(f'{i} userid = {userid}')
            results = sa.request(path_name=f'users/{userid}/profile')

            if first_time:
                csv_writer = csv.DictWriter(output_file, delimiter=csv_delimiter, quotechar=csv_quotechar, quoting=csv.QUOTE_ALL, skipinitialspace=True, fieldnames=results[0].keys())
                csv_writer.writeheader()
            csv_writer.writerows(results)

            first_time = False



# %%

@catch_error(logger)
def main():
    """
    Main Function to run
    """
    sa = SuccessproActifi(domain=domain)
    sa.authenticate(client_id=client_id, client_secret=client_secret)

    for path_name, table_name in path_names.items():
        sa.request(path_name=path_name, table_name=table_name)

    #fetch_user_profiles(sa=sa)

    mark_execution_end()



# %%

if __name__ == '__main__':
    main()



# %%



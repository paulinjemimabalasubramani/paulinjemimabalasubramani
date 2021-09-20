"""
Library for common generic functions

"""

# %% Import Libraries
import os, sys, logging, platform, psutil, yaml, json, requests, hashlib, hmac, base64

from pprint import pprint
from datetime import datetime
from functools import wraps

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None, raise_error:bool=True):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            response = None
            try:
                response = fn(*args, **kwargs)
            except Exception as e:
                exception_message  = f"Exception occurred inside '{fn.__name__}'"
                exception_message += f"\nException Message: {e}"
                pprint(exception_message)

                if logger: logger.error(exception_message)
                if raise_error: raise e
            return response
        return inner
    return outer



# %% Get Environment Variable

@catch_error()
def get_env(variable_name:str, logger=None):
    try:
        value = os.environ.get(variable_name)
        if not value:
            raise ValueError(f'Environment variable does not exist: {variable_name}')

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return value



# %% Config Class to load data from Config Files
class Config:
    """
    Class for retrieving and storing configuration data
    """
    @catch_error()
    def __init__(self, file_path:str, defaults:dict={}, logger=None):
        try:
            for name, value in defaults.items():
                setattr(self, name, value) # Write defaults

            try:
                with open(file_path, 'r') as f:
                    contents = yaml.load(f, Loader=yaml.FullLoader)
            except Exception as e:
                except_str = f'Error File was not read: {file_path}'
                pprint(except_str)
                return

            for name, value in contents.items():
                setattr(self, name, value) # Overwrite defaults from file

        except Exception as e:
            if logger:
                logger.error(str(e))
            else:
                pprint(e)
            raise e

    @catch_error()
    def get_value(self, attr_name:str, default_value, check_is_pc:bool=True):
        if not hasattr(self, attr_name) or (check_is_pc and is_pc):
            setattr(self, attr_name, default_value) 
        return getattr(self, attr_name)




# %% Parameters

execution_date_start = datetime.now()
strftime = r"%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = execution_date_start.strftime(strftime)

is_pc = platform.system().lower() == 'windows'

log_analytics_workspace_id = '' # TODO

join_drivers_by = ':' # for extraClassPath

fileshare = '/usr/local/spark/resources/fileshare'
drivers_path = fileshare + '/EDIP-Code/drivers'
config_path = fileshare + '/EDIP-Code/config'
data_settings_file_name = 'data_settings.yaml'


if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.1-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_241'
    #os.environ["PYSPARK_PYTHON"] = r'C:\Users\smammadov\AppData\Local\Programs\Python\Python38\python.exe' # add this line as necessary

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    python_dirname = os.path.dirname(__file__)
    drivers_path = os.path.realpath(python_dirname + '/../../drivers')
    config_path = os.path.realpath(python_dirname + '/../../config')

    join_drivers_by = ';' # for extraClassPath


defaults = {
    'default_data_path': fileshare + '/Shared' + ('/'+sys.domain_name if hasattr(sys, 'domain_name') else ''),
    'environment': sys.environment if hasattr(sys, 'environment') else 'QA',
}

data_settings = Config(file_path=os.path.join(config_path, data_settings_file_name), defaults=defaults)
data_settings.data_path = data_settings.default_data_path

if is_pc:
    data_settings.data_path = os.path.realpath(python_dirname + '/../../../Shared'+ ('/'+sys.domain_name if hasattr(sys, 'domain_name') else ''))

data_paths_per_source = data_settings.get_value(attr_name='data_paths_per_source', default_value=dict())
for source, source_path in data_paths_per_source.items():
    _ = data_settings.get_value(attr_name=f'data_path_{source}', default_value=source_path)

    if is_pc:
        setattr(data_settings, f'data_path_{source}', os.path.join(data_settings.data_path, source))



# %% Get Azure Key Vault

@catch_error()
def get_azure_key_vault(logger=None):
    try:
        azure_tenant_id = get_env('AZURE_TENANT_ID', logger=logger)
        azure_client_id = get_env('AZURE_KV_ID', logger=logger)
        azure_client_secret = get_env('AZURE_KV_SECRET', logger=logger)
        vault_endpoint = 'https://ag-kv-west2-secondary.vault.azure.net/'

        credential = ClientSecretCredential(azure_tenant_id, azure_client_id, azure_client_secret)
        client = SecretClient(vault_endpoint, credential, logging_enable=True)

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return azure_tenant_id, client



# %% Get Secrets

@catch_error()
def get_secrets(account_name:str, logger=None):
    try:
        environment = data_settings.environment.lower()
        account_name = account_name.lower()
        azure_tenant_id, client = get_azure_key_vault()

        sp_id = client.get_secret(f'{environment}-{account_name}-id').value
        sp_pass = client.get_secret(f'{environment}-{account_name}-pass').value

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return azure_tenant_id, sp_id, sp_pass



azure_tenant_id, log_customer_id, log_shared_key = get_secrets('loganalytics')



# %% Build the API signature

@catch_error()
def build_log_signature(customer_id:str, shared_key:str, rfc1123date:str, content_length:int, method:str, content_type:str, resource:str, logger=None):
    try:
        x_headers = 'x-ms-date:' + rfc1123date
        string_to_hash = '\n'.join([method, str(content_length), content_type, x_headers, resource])
        bytes_to_hash = bytes(string_to_hash, encoding='utf-8')
        decoded_key = base64.b64decode(shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
        authorization = f'SharedKey {customer_id}:{encoded_hash}'

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return authorization



# %% Build and send a request to the POST API
# https://docs.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api

@catch_error()
def post_log_data(log_data:dict, log_type:str, logger=None, backup_logger_func=None):
    try:
        log_data = {
            'TimeGenerated': datetime.now(),
            'MainScript': sys.parent_name if hasattr(sys, 'parent_name') else '',
            **log_data}
        body = json.dumps(log_data, sort_keys=True, default=str)

        method = 'POST'
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        uri = 'https://' + log_customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        signature = build_log_signature(
            customer_id = log_customer_id,
            shared_key = log_shared_key,
            rfc1123date = rfc1123date,
            content_length = len(body),
            method = method,
            content_type = content_type,
            resource = resource,
            logger = logger,
            )

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': log_type,
            'x-ms-date': rfc1123date,
        }

        response = requests.post(uri, data=body, headers=headers)
        if (response.status_code >= 200 and response.status_code <= 299):
            #pprint('Log Accepted')
            pass
        else:
            pprint(f'Log Response code: {response.status_code}')
            if backup_logger_func:
                backup_logger_func(body, exc_info=False)

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)



# %% Create file with associated directory tree

@catch_error()
def write_file(file_path:str, contents, mode = 'w', logger=None):
    """
    Create file with associated directory tree
    if directory does not exist, then create the directory as well.
    """
    try:
        dirname = os.path.dirname(file_path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        with open(file_path, mode) as f:
            f.write(contents)

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e



# %% Get System Info in String

@catch_error()
def system_info(logger=None):
    try:
        uname = platform.uname()

        sysinfo = {
            'Python_Version': platform.python_version(),
            'Operating_System': uname.system,
            'Network_Name': uname.node,
            'OS_Release': uname.release,
            'OS_Version': uname.version,
            'Machine_Type': uname.machine,
            'Processor': uname.processor,
            'RAM': str(round(psutil.virtual_memory().total / (1024.0 **3))) + ' GB',
            'Drivers_Path': drivers_path,
            'Config_Path': config_path,
            'Data_Path': data_settings.data_path,
        }

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        #raise e

    return sysinfo



# %% Create Logger with custom configuration

class CreateLogger:

    @catch_error()
    def __init__(self):
        self.log_type = 'AirflowPrintedLogs'
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        log_path = data_settings.get_value(attr_name='output_log_path', default_value=os.path.join(data_settings.data_path, 'logs'))
        log_file_name_no_ext = sys.parent_name.split('.')[0] if hasattr(sys, 'parent_name') else 'logs'
        self.log_file = os.path.join(log_path, f'{log_file_name_no_ext}.log')

        write_file(file_path=self.log_file, contents='', mode='a')

        logging.basicConfig(
            filename = self.log_file, 
            filemode = 'a',
            format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
            datefmt = '%d-%b-%y %H:%M:%S',
            level = logging.INFO,
            )

        other_loggers = [
            'azure.core.pipeline.policies.http_logging_policy',
            'azure.identity._internal.get_token_mixin',
            ]

        for other_logger in other_loggers:
            logging.getLogger(other_logger).setLevel(logging.WARNING)


    @catch_error()
    def log(self, msg, msg_type, extra_log:dict={}, backup_logger_func=None):
        log_data = {
            'msg': msg,
            'msg_type': msg_type,
            **extra_log,
        }
        pprint(log_data)
        post_log_data(log_data=log_data, log_type=self.log_type, logger=self.logger, backup_logger_func=backup_logger_func)


    @catch_error()
    def info(self, msg):
        self.log(msg=msg, msg_type='INFO', backup_logger_func=self.logger.info)


    @catch_error()
    def warning(self, msg):
        self.log(msg=msg, msg_type='WARNING', extra_log=system_info(logger=self.logger), backup_logger_func=self.logger.warning)


    @catch_error()
    def error(self, msg):
        self.log(msg=msg, msg_type='ERROR', extra_log=system_info(logger=self.logger), backup_logger_func=self.logger.error)




logger = CreateLogger()


logger.info({'execution_date': execution_date})
logger.info(system_info(logger=logger))




# %% Obtain authentication token using a Service Principal

@catch_error(logger, raise_error=False)
def get_log_token():
    login_url = 'https://login.microsoftonline.com/' + azure_tenant_id + '/oauth2/token'
    resource = 'https://api.loganalytics.io'

    payload = {
        'grant_type': 'client_credentials',
        'client_id': log_customer_id,
        'client_secret': log_shared_key,
        'Content-Type': 'x-www-form-urlencoded',
        'resource': resource
    }

    try:
        response = requests.post(login_url, data=payload, verify=True)
    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)

    if (response.status_code >= 200 and response.status_code <= 299):
        if logger: logger.info('Log Token obtained')
        token = json.loads(response.content)['access_token']
        return {'Authorization': str('Bearer '+ token), 'Content-Type': 'application/json'}
    else:
        error_log = 'Unable to get log token: ' + format(response.status_code)
        if logger:
            logger.error(error_log)
        else:
            pprint(error_log)




# %% Execute Kusto Query on an Azure Log Analytics Workspace
# https://docs.microsoft.com/en-us/azure/data-explorer/kusto/query/
# https://dev.loganalytics.io/

@catch_error(logger, raise_error=False)
def get_log_data(kusto_query):
    log_token = get_log_token()
    if not log_token:
        return

    az_url = 'https://api.loganalytics.io/v1/workspaces/'+ log_analytics_workspace_id + '/query'
    query = {'query': kusto_query}

    try:
        response = requests.get(az_url, params=query, headers=log_token)
    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)

    if (response.status_code >= 200 and response.status_code <= 299):
        if logger: logger.info('Kusto Query ran successfully')
        return json.loads(response.content)
    else:
        error_log = 'Unable to run Kusto Query: ' + format(response.status_code)
        if logger:
            logger.error(error_log)
        else:
            pprint(error_log)



# %% get extraClassPath:

@catch_error(logger)
def get_extraClassPath(drivers_path:str, join_drivers_by:str):
    drivers = []

    for file in os.listdir(drivers_path):
        if file.endswith('.jar'):
            drivers.append(os.path.join(drivers_path, file))

    extraClassPath = join_drivers_by.join(drivers)
    return extraClassPath



extraClassPath = get_extraClassPath(drivers_path=drivers_path, join_drivers_by=join_drivers_by)



# %% Mark Execution End

@catch_error(logger)
def mark_execution_end():
    execution_date_end = datetime.now()
    timedelta1 = execution_date_end - execution_date_start

    h = timedelta1.seconds // 3600
    m = (timedelta1.seconds - h * 3600) // 60
    s = timedelta1.seconds - h * 3600 - m * 60
    total_time = f'{timedelta1.days} day(s), {h} hour(s), {m} minute(s), {s} second(s)'

    logger.info({
        'execution_date_start': execution_date,
        'execution_date_end': execution_date_end.strftime(strftime),
        'total_seconds': timedelta1.seconds,
        'total_time': total_time,
    })



# %%



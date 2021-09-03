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



# %% Parameters

execution_date_start = datetime.now()
strftime = r"%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = execution_date_start.strftime(strftime)

is_pc = platform.system().lower() == 'windows'

fileshare = '/usr/local/spark/resources/fileshare'
drivers_path = fileshare + '/EDIP-Code/drivers'
config_path = fileshare + '/EDIP-Code/config'
data_path = fileshare + '/Shared'

join_drivers_by = ':' # for extraClassPath

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
    data_path = os.path.realpath(python_dirname + '/../../../Shared')

    join_drivers_by = ';' # for extraClassPath



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None):
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            try:
                response = fn(*args, **kwargs)
            except Exception as e:
                exception_message  = f"Exception occurred inside '{fn.__name__}'"
                exception_message += f"\nException Message: {e}"
                pprint(exception_message)

                if logger:
                    logger.error(exception_message)
                raise e
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
        account_name = account_name.lower()
        azure_tenant_id, client = get_azure_key_vault()

        sp_id = client.get_secret(f"qa-{account_name}-id").value
        sp_pass = client.get_secret(f"qa-{account_name}-pass").value

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return azure_tenant_id, sp_id, sp_pass



_, log_customer_id, log_shared_key = get_secrets("loganalytics")



# %% Build the API signature

@catch_error()
def build_log_signature(customer_id, shared_key, date, content_length, method, content_type, resource, logger=None):
    try:
        x_headers = 'x-ms-date:' + date
        string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
        bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
        decoded_key = base64.b64decode(shared_key)
        encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
        authorization = "SharedKey {}:{}".format(customer_id, encoded_hash)

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    return authorization



# %% Build and send a request to the POST API

@catch_error()
def post_log_data(log_data:dict, log_type:str, logger=None):
    try:
        log_data = {
            'TimeGenerated': datetime.now(),
            **log_data}

        method = 'POST'
        body = json.dumps(log_data, sort_keys=True, default=str)
        content_type = 'application/json'
        resource = '/api/logs'
        rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        content_length = len(body)
        signature = build_log_signature(customer_id=log_customer_id, shared_key=log_shared_key, date=rfc1123date, content_length=content_length, method=method, content_type=content_type, resource=resource)
        uri = 'https://' + log_customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

        headers = {
            'content-type': content_type,
            'Authorization': signature,
            'Log-Type': log_type,
            'x-ms-date': rfc1123date,
        }

        response = requests.post(uri, data=body, headers=headers)
        if (response.status_code >= 200 and response.status_code <= 299):
            #pprint('Log Accepted')
            return True
        else:
            pprint(f"Log Response code: {response.status_code}")
            return False

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e




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



# %% Create Logger with custom configuration

class CreateLogger:

    @catch_error()
    def __init__(self):
        self.log_type = 'AirflowPrintedLogs'
        self.logger = logging.getLogger(__name__)

        self.log_file = f'./logs/data_eng.log'
        write_file(file_path=self.log_file, contents='', mode='a')

        logging.basicConfig(
            filename = self.log_file, 
            filemode = 'a',
            format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
            datefmt = '%d-%b-%y %H:%M:%S',
            level = logging.INFO,
            )


    @catch_error()
    def log(self, msg, msg_type):
        log_data = {
            'msg': msg,
            'msg_type': msg_type,
        }
        pprint(log_data)
        return post_log_data(log_data=log_data, log_type=self.log_type)


    @catch_error()
    def info(self, msg):
        if not self.log(msg=msg, msg_type='INFO'):
            self.logger.info(msg)


    @catch_error()
    def warning(self, msg):
        if not self.log(msg=msg, msg_type='WARNING'):
            self.logger.warning(msg)


    @catch_error()
    def error(self, msg):
        if not self.log(msg=msg, msg_type='ERROR'):
            self.logger.error(msg, exc_info=True)



logger = CreateLogger()


logger.info({'execution_date': execution_date})




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



# %% Get System Info in String

@catch_error(logger)
def system_info():
    uname = platform.uname()

    sysinfo = {
        'Python_Version': platform.python_version(),
        'Operating_System': uname.system,
        'Network_Name': uname.node,
        'OS_Release': uname.release,
        'OS_Version': uname.version,
        'Machine_Type': uname.machine,
        'Processor': uname.processor,
        'RAM': str(round(psutil.virtual_memory().total / (1024.0 **3))) + " GB",
        'Drivers_Path': drivers_path,
        'Config_Path': config_path,
        'Data_Path': data_path,
    }

    return sysinfo



logger.info(system_info())



# %%



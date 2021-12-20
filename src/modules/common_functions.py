"""
Library for common generic functions

"""

# %% Import Libraries
import os, sys, logging, platform, psutil, yaml, json, requests, hashlib, hmac, base64, collections, pymssql

from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from pprint import pprint
from datetime import datetime
from functools import wraps
from collections import OrderedDict

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient



# %% Parameters

execution_date_start = datetime.now()
strftime = r"%Y-%m-%d %H:%M:%S"  # http://strftime.org/
execution_date = execution_date_start.strftime(strftime)
EXECUTION_DATE_str = 'EXECUTION_DATE'

is_pc = platform.system().lower() == 'windows'

fileshare = '/usr/local/spark/resources/fileshare'
drivers_path = fileshare + '/EDIP-Code/drivers'
config_path = fileshare + '/EDIP-Code/config'

data_settings_file_name = 'data_settings.yaml'


if is_pc:
    os.environ["SPARK_HOME"]  = r'C:\Spark\spark-3.1.2-bin-hadoop3.2'
    os.environ["HADOOP_HOME"] = r'C:\Spark\Hadoop'
    os.environ["JAVA_HOME"]   = r'C:\Program Files\Java\jre1.8.0_311'
    #os.environ["PYSPARK_PYTHON"] = r'C:\Users\smammadov\AppData\Local\Programs\Python\Python38\python.exe' # add this line as necessary

    sys.path.insert(0, '%SPARK_HOME%\bin')
    sys.path.insert(0, '%HADOOP_HOME%\bin')
    sys.path.insert(0, '%JAVA_HOME%\bin')

    python_dirname = os.path.dirname(__file__)
    drivers_path = os.path.realpath(python_dirname + '/../../drivers')
    config_path = os.path.realpath(python_dirname + '/../../config')



# %% Wrapper/Decorator function for catching errors

def catch_error(logger=None, raise_error:bool=True):
    """
    Wrapper/Decorator function for catching errors
    The function catches errors, and then writes to logger
    """
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            response = None
            try:
                response = fn(*args, **kwargs)
            except (BaseException, AssertionError) as e:
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
def get_env(variable_name:str, default:str=None, logger=None):
    """
    Get Environment Variable
    """
    try:
        value = os.environ.get(key=variable_name, default=default)
        if not value:
            raise ValueError(f'Environment variable does not exist: {variable_name}')
        elif value.isnumeric() and variable_name not in ['salesforce_api_version']:
            value = float(value) if '.' in value else int(value)
        elif value.lower() in ['true', 'false']:
            value = value.lower()=='true'

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
        """
        Initiate the class.
        Read the configuration YAML file.
        Assign defaults if any config data doesn't exist.
        """
        try:
            for name, value in defaults.items():
                setattr(self, name, value) # Write defaults

            if file_path:
                try:
                    with open(file_path, 'r') as f:
                        contents = yaml.load(f, Loader=yaml.SafeLoader)
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
        """
        Get Config value. If value doesn't exist then save default_value and retrieve it.
        """
        if not hasattr(self, attr_name) or (check_is_pc and is_pc):
            setattr(self, attr_name, default_value) 
        return getattr(self, attr_name)



# %% Get Data Settings

@catch_error()
def get_data_settings():
    data_settings = Config(file_path=os.path.join(config_path, data_settings_file_name), defaults={})

    if is_pc: # Read Data Settings from file
        data_settings.data_path = os.path.realpath(python_dirname + '/../../../Shared')
        data_settings.temporary_file_path = os.path.join(data_settings.data_path, 'TEMP')

        for source, source_path in data_settings.data_paths_per_source.items():
            setattr(data_settings, f'data_path_{source}', os.path.join(data_settings.data_path, source))

        data_settings.copy_history_log_databases = [f'{data_settings.environment}_{domain}' for domain in data_settings.copy_history_log_databases]

        data_settings.reverse_etl_map = {
                f'{data_settings.environment}_{domain}': domain_val
            for domain, domain_val in data_settings.reverse_etl_map.items()
            }

    else: # Read Data Settings from Environment if not is_pc
        env_data_settings_names = [k for k, v in data_settings.__dict__.items() if not isinstance(v, (list, tuple, collections.Mapping))]
        data_settings.data_path = fileshare + '/Shared'

        for domain in data_settings.copy_history_log_databases:
            env_data_settings_names.append(f'copy_history_log_databases_{domain}')

        for domain in data_settings.reverse_etl_map.keys():
            env_data_settings_names.append(f'reverse_etl_map_{domain}_snowflake_schema')
            env_data_settings_names.append(f'reverse_etl_map_{domain}_sql_database')
            env_data_settings_names.append(f'reverse_etl_map_{domain}_sql_schema')

        for source, source_path in data_settings.data_paths_per_source.items():
            _ = data_settings.get_value(attr_name=f'data_path_{source}', default_value=source_path)
            env_data_settings_names.append(f'data_path_{source}')

        file_history_sources = list(data_settings.file_history_start_date.keys())
        for source in file_history_sources:
            env_data_settings_names.append(f'file_history_start_date_{source}')

        for envv in env_data_settings_names: # Read all the environmental variables
            setattr(data_settings, envv, get_env(variable_name=envv.upper()))

        data_settings.copy_history_log_databases = [f'{data_settings.environment}_{domain}' for domain in data_settings.copy_history_log_databases if getattr(data_settings, f'copy_history_log_databases_{domain}')]

        data_settings.reverse_etl_map = {
            f'{data_settings.environment}_{domain}': {
                'snowflake_schema': getattr(data_settings, f'reverse_etl_map_{domain}_snowflake_schema'),
                'sql_database': getattr(data_settings, f'reverse_etl_map_{domain}_sql_database'),
                'sql_schema': getattr(data_settings, f'reverse_etl_map_{domain}_sql_schema'),
                }
            for domain in data_settings.reverse_etl_map.keys()
            }

        data_settings.file_history_start_date = {source: getattr(data_settings, f'file_history_start_date_{source}') for source in file_history_sources}

    os.makedirs(data_settings.temporary_file_path, exist_ok=True)
    _ = data_settings.get_value(attr_name='output_cicd_path', default_value=os.path.join(data_settings.data_path, 'CICD'))
    return data_settings



data_settings = get_data_settings()



# %% Get Azure Key Vault Handler

@catch_error()
def get_azure_key_vault(logger=None):
    """
    Get Azure Key Vault Handler
    """
    try:
        azure_tenant_id = get_env('AZURE_TENANT_ID', logger=logger)
        azure_client_id = get_env('AZURE_KV_ID', logger=logger)
        azure_client_secret = get_env('AZURE_KV_SECRET', logger=logger)
        vault_endpoint = get_env('KEYVAULTURL', logger=logger)

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
def get_secrets(account_name:str, logger=None, additional_secrets:list=[]):
    """
    Read Secrets (-id and -pass) from Azure Key Vault
    """
    sp_additional_secrets = []

    try:
        environment = data_settings.environment.lower()
        account_name = account_name.lower()
        azure_tenant_id, client = get_azure_key_vault()

        sp_id = client.get_secret(f'{environment}-{account_name}-id').value
        sp_pass = client.get_secret(f'{environment}-{account_name}-pass').value

        for additional_secret in additional_secrets:
            sp_additional_secrets.append(client.get_secret(f'{environment}-{account_name}-{additional_secret}').value)

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)
        raise e

    if sp_additional_secrets:
        return azure_tenant_id, sp_id, sp_pass, sp_additional_secrets[0]
    else:
        return azure_tenant_id, sp_id, sp_pass



azure_tenant_id, log_customer_id, log_shared_key = get_secrets('loganalytics')



# %% Build the API signature

@catch_error()
def build_log_signature(customer_id:str, shared_key:str, rfc1123date:str, content_length:int, method:str, content_type:str, resource:str, logger=None):
    """
    Build Azure Log API signature
    """
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



# %% Build and send log data to Azure Monitor

@catch_error()
def post_log_data(log_data:dict, log_type:str, logger=None, backup_logger_func=None):
    """
    Build and send log data to Azure Monitor
    https://docs.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api
    """
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

        if backup_logger_func:
            backup_logger_func(body, exc_info=False)
        else:
            pprint(body)

        if not logger or log_type!=logger.print_log_type: # Temporarily stop sending print logs to Azure
            response = requests.post(uri, data=body, headers=headers)
            if response.status_code >= 200 and response.status_code <= 299 and not is_pc:
                #pprint('Log Accepted')
                pass
            else:
                pprint(f'Log Response code: {response.status_code}')

    except Exception as e:
        if logger:
            logger.error(str(e))
        else:
            pprint(e)



# %% Get System Info as JSON

@catch_error()
def system_info(logger=None):
    """
    Get System Info as JSON
    """
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
    """
    Class for Logging Print Statements
    """

    @catch_error()
    def __init__(self, logging_level=logging.INFO):
        """
        Initiate the class. Set Logging Policy.
        """
        self.print_log_type = 'AirflowPrintedLogs'
        self.log_name = sys.parent_name.split('.')[0] if hasattr(sys, 'parent_name') else 'logs'
        self.logger = logging.getLogger(self.log_name)
        self.logger.setLevel(logging_level)

        log_format = logging.Formatter(fmt=r'%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s', datefmt=r'%Y-%m-%d %H:%M:%S')

        log_path = data_settings.get_value(attr_name='output_log_path', default_value=os.path.join(data_settings.data_path, 'logs'))
        os.makedirs(log_path, exist_ok=True)
        self.log_file = os.path.join(log_path, f'{self.log_name}.log')

        stdout_handler = StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.addFilter(lambda record: record.levelno <= logging.WARNING)
        stdout_handler.setFormatter(log_format)
        self.logger.addHandler(stdout_handler)

        stderr_handler = StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(log_format)
        self.logger.addHandler(stderr_handler)

        file_handler = RotatingFileHandler(
            filename = self.log_file,
            mode = 'a',
            encoding = 'utf-8',
            maxBytes = 2 * (1024**2), # up to 2 Mb file size
            backupCount = 3, # up to 3 file backups
            )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(log_format)
        self.logger.addHandler(file_handler)

        warning_loggers = [ # Elevate the following loggers to WARNING level
            'azure.core.pipeline.policies.http_logging_policy',
            'azure.identity._internal.get_token_mixin',
            ]

        for warning_logger in warning_loggers:
            logging.getLogger(warning_logger).setLevel(logging.WARNING)


    @catch_error()
    def log(self, msg, msg_type, extra_log:dict={}, backup_logger_func=None):
        """
        Log Print Message
        """
        log_data = {
            'msg': msg,
            'msg_type': msg_type,
            **extra_log,
        }
        post_log_data(log_data=log_data, log_type=self.print_log_type, logger=self, backup_logger_func=backup_logger_func)


    @catch_error()
    def info(self, msg):
        """
        Log info message
        """
        self.log(msg=msg, msg_type='INFO', backup_logger_func=self.logger.info)


    @catch_error()
    def warning(self, msg):
        """
        Log warning message
        """
        #self.log(msg=msg, msg_type='WARNING', extra_log=system_info(logger=self.logger), backup_logger_func=self.logger.warning)
        self.log(msg=msg, msg_type='WARNING', backup_logger_func=self.logger.warning)


    @catch_error()
    def error(self, msg):
        """
        Log error message
        """
        #self.log(msg=msg, msg_type='ERROR', extra_log=system_info(logger=self.logger), backup_logger_func=self.logger.error)
        self.log(msg=msg, msg_type='ERROR', backup_logger_func=self.logger.error)




logger = CreateLogger()


logger.info({EXECUTION_DATE_str: execution_date})
logger.info(system_info(logger=logger))



# %% get extraClassPath:

@catch_error(logger)
def get_extraClassPath(drivers_path:str):
    """
    Get list of all the JAR files for PySpark
    """
    drivers = []
    join_drivers_by = ':' if not is_pc else ';'

    for file in os.listdir(drivers_path):
        if file.endswith('.jar'):
            drivers.append(os.path.join(drivers_path, file))

    extraClassPath = join_drivers_by.join(drivers)
    return extraClassPath



extraClassPath = get_extraClassPath(drivers_path=drivers_path)



# %% Mark Execution End

@catch_error(logger)
def mark_execution_end():
    """
    Log Execution End date and Execution duration of the entire code
    """
    execution_date_end = datetime.now()
    timedelta1 = execution_date_end - execution_date_start

    h = timedelta1.seconds // 3600
    m = (timedelta1.seconds - h * 3600) // 60
    s = timedelta1.seconds - h * 3600 - m * 60
    total_time = f'{timedelta1.days} day(s), {h} hour(s), {m} minute(s), {s} second(s)'

    logger.info({
        f'{EXECUTION_DATE_str}_start': execution_date,
        f'{EXECUTION_DATE_str}_end': execution_date_end.strftime(strftime),
        'total_seconds': timedelta1.seconds,
        'total_time': total_time,
    })



# %% Utility function to convert dict to OrderedDict

@catch_error(logger)
def to_OrderedDict(dict_:dict, reverse:bool=False):
    """
    Utility function to convert dict to OrderedDict
    """
    return OrderedDict(sorted(dict_.items(), key=lambda x:x[0], reverse=reverse))



# %% pymssql message handler - information sent by the server

@catch_error(logger)
def pymssql_msg_handler(msgstate, severity, srvname, procname, line, msgtext):
    """
    pymssql message handler - information sent by the server
    http://www.pymssql.org/en/stable/_mssql_examples.html#custom-message-handlers
    """
    logger.info({
        'function': 'pymssql_msg_handler',
        'msgstate': msgstate,
        'severity': severity,
        'srvname': srvname,
        'procname': procname,
        'line': line,
        'msgtext': msgtext,
    })



# %% pymssql execute non query statements

@catch_error(logger)
def pymssql_execute_non_query(sqlstr_list:list, sql_server:str, sql_id:str, sql_pass:str, sql_database:str):
    """
    pymssql execute non query statements
    """
    conn = pymssql.connect(
        server = sql_server,
        user = sql_id,
        password = sql_pass,
        database = sql_database,
        appname = __name__,
        autocommit = True,
        )
    conn._conn.set_msghandler(pymssql_msg_handler)
    for sqlstr in sqlstr_list:
        logger.info({'execute': sqlstr})
        conn._conn.execute_non_query(sqlstr)
    conn.close()



# %%



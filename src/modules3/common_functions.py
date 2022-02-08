"""
Library for common generic functions

"""

# %% Import Libraries
import os, sys, logging, platform, psutil, yaml, json, requests, hashlib, hmac, base64, pymssql, shutil

from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from datetime import datetime
from functools import wraps
from collections import OrderedDict

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient



# %% Parameters

execution_date_start = datetime.now()
strftime = r'%Y-%m-%d %H:%M:%S'  # http://strftime.org/
execution_date = execution_date_start.strftime(strftime)
execution_date_start = datetime.strptime(execution_date, strftime) # to ensure identity with the string form of execution date
EXECUTION_DATE_str = 'elt_execution_date'
ELT_PROCESS_ID_str = 'elt_process_id'

is_pc = platform.system().lower() == 'windows'

code_repo_path = '/usr/local/spark/resources/fileshare/EDIP-Code'
drivers_path = os.path.join(code_repo_path, 'drivers')
config_path = os.path.join(code_repo_path, 'config')

if is_pc:
    os.environ['SPARK_HOME']  = r'C:\Spark\spark-3.1.2-bin-hadoop3.2'
    os.environ['HADOOP_HOME'] = r'C:\Spark\Hadoop'
    os.environ['JAVA_HOME']   = r'C:\Program Files\Java\jre1.8.0_311'
    #os.environ['PYSPARK_PYTHON'] = r'~\.venv\Scripts\python.exe' # add this line as necessary

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
                if not hasattr(sys.app, 'is_error'):
                    sys.app.is_error = True
                    exception_message  = f"Exception occurred inside '{fn.__name__}'"
                    exception_message += f"\nException Message: {e}"
                    print(exception_message)

                    if hasattr(sys.app, 'finalize_new_pipeline_instance'):
                        try:
                            sys.app.finalize_new_pipeline_instance(error_message=str(e))
                        except:
                            pass

                    if logger: logger.error(exception_message)
                if raise_error: raise e
            return response
        return inner
    return outer



# %% Get Environment Variable

@catch_error()
def get_env(variable_name:str, default:str=None, logger=None, raise_error_if_no_value:bool=True):
    """
    Get Environment Variable
    """
    try:
        value = os.environ.get(key=variable_name, default=default)
        if not value:
            if raise_error_if_no_value:
                raise ValueError(f'Environment variable does not exist: {variable_name}')
        elif value.isnumeric() and variable_name not in ['salesforce_api_version']:
            value = float(value) if '.' in value else int(value)
        elif value.lower() in ['true', 'false']:
            value = value.lower()=='true'

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)
        raise e

    return value



sys.app.environment = get_env(variable_name='ENVIRONMENT').upper()



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

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)
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
        environment = sys.app.environment.lower()
        account_name = account_name.lower()
        azure_tenant_id, client = get_azure_key_vault()

        sp_id = client.get_secret(f'{environment}-{account_name}-id').value
        sp_pass = client.get_secret(f'{environment}-{account_name}-pass').value

        for additional_secret in additional_secrets:
            sp_additional_secrets.append(client.get_secret(f'{environment}-{account_name}-{additional_secret}').value)

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)
        raise e

    if sp_additional_secrets:
        return azure_tenant_id, sp_id, sp_pass, sp_additional_secrets[0]
    else:
        return azure_tenant_id, sp_id, sp_pass



azure_tenant_id, log_customer_id, log_shared_key = get_secrets(account_name='loganalytics')



# %% Config Class to load data from Config Files
class Config:
    """
    Class for retrieving and storing configuration data
    """

    @catch_error()
    def __init__(self, file_path:str=None, defaults:dict={}, logger=None):
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
                except (BaseException, AssertionError) as e:
                    except_str = f'Error File was not read: {file_path}'
                    if logger:
                        logger.error(except_str)
                    else:
                        print(except_str)
                    return

                for name, value in contents.items():
                    setattr(self, name, value) # Overwrite defaults from file

        except (BaseException, AssertionError) as e:
            if logger:
                logger.error(str(e))
            else:
                print(e)
            raise e


    @catch_error()
    def get_value(self, attr_name:str, default_value, check_is_pc:bool=True):
        """
        Get Config value. If value doesn't exist then save default_value and retrieve it.
        """
        if not hasattr(self, attr_name) or (check_is_pc and is_pc):
            setattr(self, attr_name, default_value) 
        return getattr(self, attr_name)



# %% Add PipelineConfiguration to Data Settings

@catch_error()
def add_PipelineConfiguration_to_data_settings(data_settings, pipeline_metadata_conf:dict, PipelineKey:str):
    """
    Add PipelineConfiguration to Data Settings
    """
    sqlstr = f"""SELECT *
    FROM {pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_pipe_config']}
    WHERE UPPER(PipelineKey) = '{PipelineKey.upper()}'
    ;"""

    with pymssql.connect(
        server = pipeline_metadata_conf['sql_server'],
        user = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
        database = pipeline_metadata_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr)
            for row in cursor:
                key, value = row['ConfigKey'].strip().lower(), row['ConfigValue'].strip()
                if not hasattr(data_settings, key): # previous settings (CLI args and Environment Variables) takes precendene over SQL server config
                    setattr(data_settings, key, value)



# %% Get Data Settings

@catch_error()
def get_data_settings(logger=None):
    """
    Apply Data Settings from various environments
    Order of Command: CLI args > Environmental Variables > SQL Server Settings (Pipeline Specific > Generic)
    """
    generic_pipelinekey = 'GENERIC'
    defaults = {
        'pipelinekey': generic_pipelinekey,
        'environment': sys.app.environment,
        'parent_program': sys.app.parent_name,
    }

    data_settings = Config(defaults=defaults)

    env_data_settings_names = [
        'metadata_sql_key_vault_account',
        'metadata_sql_server',
        'metadata_sql_database',
        ]

    for envv in env_data_settings_names: # Read all the environmental variables
        setattr(data_settings, envv, get_env(variable_name=envv.upper()))

    if hasattr(sys.app, 'args'): # CLI Arguments takes precedence over environment variables
        for arg_key, arg_val in sys.app.args.items():
            setattr(data_settings, arg_key, arg_val)

    cloud_file_hist_conf = {
        'sql_key_vault_account': data_settings.metadata_sql_key_vault_account,
        'sql_server': data_settings.metadata_sql_server,
        'sql_database': data_settings.metadata_sql_database,
        'sql_schema': 'edip',
    }

    _, cloud_file_hist_conf['sql_id'], cloud_file_hist_conf['sql_pass'] = get_secrets(cloud_file_hist_conf['sql_key_vault_account'].lower(), logger=logger)

    pipeline_metadata_conf = cloud_file_hist_conf.copy()
    pipeline_metadata_conf['sql_schema'] = 'metadata'
    pipeline_metadata_conf['sql_table_name_primary_key'] = 'PrimaryKey'
    pipeline_metadata_conf['sql_table_name_pipe_config'] = 'PipelineConfiguration'
    pipeline_metadata_conf['sql_table_name_pipe_instance'] = 'PipelineInstance'
    pipeline_metadata_conf['sql_table_name_pipeline'] = 'Pipeline'
    pipeline_metadata_conf['sql_table_name_datasource'] = 'DataSource'

    add_PipelineConfiguration_to_data_settings(data_settings=data_settings, pipeline_metadata_conf=pipeline_metadata_conf, PipelineKey=data_settings.pipelinekey)
    add_PipelineConfiguration_to_data_settings(data_settings=data_settings, pipeline_metadata_conf=pipeline_metadata_conf, PipelineKey=generic_pipelinekey) # generic pipelinekeys have the lowest power.

    if hasattr(data_settings, 'db_name'):  data_settings.domain_name = data_settings.db_name.lower()
    if hasattr(data_settings, 'schema_name'): data_settings.schema_name = data_settings.schema_name.upper()
    data_settings.elt_process_id = '_'.join([data_settings.pipelinekey, execution_date_start.strftime(r'%Y%m%d%H%M%S')])

    to_storage_account = lambda storage_account_mid:  f"{data_settings.azure_storage_accounts_prefix}{storage_account_mid}{data_settings.azure_storage_accounts_suffix}".lower()
    data_settings.default_storage_account_name = to_storage_account(storage_account_mid=data_settings.azure_storage_accounts_default_mid)
    if hasattr(data_settings, 'azure_storage_account_mid'): data_settings.storage_account_name = to_storage_account(storage_account_mid=data_settings.azure_storage_account_mid)

    key_datetime = data_settings.file_history_start_date if hasattr(data_settings, 'file_history_start_date') else '2000-01-15'
    data_settings.key_datetime = datetime.strptime(key_datetime, r'%Y-%m-%d')

    if hasattr(data_settings, 'domain_name') and hasattr(data_settings, 'schema_name'):
        sql_file_history_table = ('_'.join([data_settings.domain_name, data_settings.schema_name, 'file_history3'])).lower()
        sql_file_history_table = data_settings.sql_file_history_table.lower() if hasattr(data_settings, 'sql_file_history_table') else sql_file_history_table
        cloud_file_hist_conf['sql_file_history_table'] = sql_file_history_table

    if is_pc: # Read Data Settings from file
        data_path = os.path.realpath(python_dirname + '/../../../Shared')
        data_settings.temporary_file_path = os.path.join(data_path, 'TEMP')
        data_settings.output_ddl_path = os.path.join(data_path, 'DDL')
        data_settings.output_log_path = os.path.join(data_path, 'logs')
        data_settings.app_data_path = os.path.join(data_path, 'APPDATA')

    data_settings.app_data_path = os.path.join(data_settings.app_data_path, data_settings.pipelinekey)
    os.makedirs(data_settings.temporary_file_path, exist_ok=True)
    return data_settings, cloud_file_hist_conf, pipeline_metadata_conf



data_settings, cloud_file_hist_conf, pipeline_metadata_conf = get_data_settings()



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

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)
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
            'MainScript': sys.app.parent_name if hasattr(sys.app, 'parent_name') else '',
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
            print(body)

        if not logger or log_type!=logger.print_log_type: # Temporarily stop sending print logs to Azure
            response = requests.post(uri, data=body, headers=headers)
            if response.status_code >= 200 and response.status_code <= 299 and not is_pc:
                #print('Log Accepted')
                pass
            else:
                print(f'Log Response code: {response.status_code}')

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)



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
            **data_settings.__dict__,
        }

    except (BaseException, AssertionError) as e:
        if logger:
            logger.error(str(e))
        else:
            print(e)
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
        self.log_name = sys.app.parent_name.split('.')[0] if hasattr(sys.app, 'parent_name') else 'logs'
        self.logger = logging.getLogger(self.log_name)
        self.logger.setLevel(logging_level)

        log_format = logging.Formatter(fmt=r'%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s', datefmt=r'%Y-%m-%d %H:%M:%S')

        log_path = data_settings.output_log_path
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



# %% Get Pipeline Info

@catch_error(logger)
def get_pipeline_info():
    """
    Get Generic Information about the Pipeline
    """
    if not hasattr(data_settings, 'pipelinekey'):
        return

    pipelinekey = data_settings.pipelinekey
    sql_pipeline_table = f"{pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_pipeline']}"
    sql_datasource_table = f"{pipeline_metadata_conf['sql_schema']}.{pipeline_metadata_conf['sql_table_name_datasource']}"

    sqlstr = f"""SELECT TOP 1 p.*, ds.Category, ds.SubCategory, ds.Firm, ds.DataSourceType, ds.DataProvider
FROM {sql_pipeline_table} p
    LEFT JOIN {sql_datasource_table} ds ON p.DataSourceKey = ds.DataSourceKey
WHERE p.IsActive = 1
    and p.PipelineKey = '{pipelinekey}'
ORDER BY p.UpdateTs DESC, p.PipelineId DESC
;"""

    with pymssql.connect(
        server = pipeline_metadata_conf['sql_server'],
        user = pipeline_metadata_conf['sql_id'],
        password = pipeline_metadata_conf['sql_pass'],
        database = pipeline_metadata_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr)
            row = cursor.fetchone()

    if not row: raise ValueError(f'Pipeline "{pipelinekey}" is not defined in metadata.Pipeline')

    for key, value in row.items():
        setattr(data_settings, 'pipeline_' + key.strip().lower(), value.strip() if isinstance(value, str) else value)



get_pipeline_info()



# %% get extraClassPath:

@catch_error(logger)
def get_extraClassPath(drivers_path:str):
    """
    Get list of all the JAR files for PySpark
    """
    drivers = []
    is_windows = platform.system().lower() == 'windows'
    join_drivers_by = ';' if is_windows else ':'

    for root, dirs, files in os.walk(drivers_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() in ['.jar']:
                drivers.append(file_path)

    extraClassPath = join_drivers_by.join(drivers)
    return extraClassPath



extraClassPath = get_extraClassPath(drivers_path=drivers_path)



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
        appname = sys.app.parent_name,
        autocommit = True,
        )
    conn._conn.set_msghandler(pymssql_msg_handler)
    for sqlstr in sqlstr_list:
        logger.info({'execute': sqlstr})
        conn._conn.execute_non_query(sqlstr)
    conn.close()



# %% Utility function to convert Python values to SQL equivalent values

@catch_error(logger)
def to_sql_value(cval):
    """
    Utility function to convert Python values to SQL equivalent values
    """
    strftime = r'%Y-%m-%d %H:%M:%S'  # http://strftime.org/

    if isinstance(cval, datetime):
        cval = cval.strftime(strftime)
    else:
        cval = str(cval)
        cval = cval.replace("'", "''")

    return cval



# %% Add new pipeline instance. Create Pipeline Instance table if not exists

@catch_error(logger)
def add_new_pipeline_instance():
    """
    Add new pipeline instance. Create Pipeline Instance table if not exists
    """
    logger.info(f'Creating new Pipeline Instance: {data_settings.elt_process_id}')
    sys.app.table_count = 0
    sys.app.aggregate_file_size_kb = 0.0
    sys.app.error_file = None

    schema_name = pipeline_metadata_conf['sql_schema']
    table_name = pipeline_metadata_conf['sql_table_name_pipe_instance']
    full_table_name = f"{schema_name}.{table_name}"

    sqlstr_table_exists = f"""SELECT COUNT(*) AS CNT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE UPPER(TABLE_SCHEMA) = '{schema_name.upper()}'
        AND UPPER(TABLE_TYPE) = 'BASE TABLE'
        AND UPPER(TABLE_NAME) = '{table_name.upper()}'
    ;"""

    sqlstr_table_create = f"""CREATE TABLE {full_table_name} (
        PipelineInstanceId int Identity,
        PipelineKey varchar(500) NOT NULL,
        {ELT_PROCESS_ID_str.lower()} varchar(500) NOT NULL,
        {EXECUTION_DATE_str.lower()} datetime NULL,
        total_minutes numeric(38, 3) NULL,
        table_count int NULL,
        aggregate_file_size_kb numeric(38, 3) NULL,
        run_status varchar(100) NULL,
        error_message varchar(1500) NULL,
        error_file varchar(1000)
        );"""

    sqlstr_new_instance = f"""INSERT INTO {full_table_name} (
        PipelineKey,
        {ELT_PROCESS_ID_str.lower()},
        {EXECUTION_DATE_str.lower()},
        run_status
    ) VALUES (
        '{data_settings.pipelinekey}',
        '{data_settings.elt_process_id}',
        '{to_sql_value(execution_date_start)}',
        'Running'
    );"""

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute(sqlstr_table_exists)
            row = cursor.fetchone()
            if int(row['CNT']) == 0:
                logger.info(f'{full_table_name} table does not exist in SQL server. Creating new table.')
                conn._conn.execute_non_query(sqlstr_table_create)

            conn._conn.execute_non_query(sqlstr_new_instance)



add_new_pipeline_instance()



# %% Update the Pipeline Instance with the final data

@catch_error(logger)
def finalize_new_pipeline_instance(error_message:str=None):
    """
    Update the Pipeline Instance with the final data
    """
    schema_name = pipeline_metadata_conf['sql_schema']
    table_name = pipeline_metadata_conf['sql_table_name_pipe_instance']
    full_table_name = f"{schema_name}.{table_name}"
    total_minutes = (datetime.now() - execution_date_start).seconds / 60
    run_status = 'Failed' if error_message else 'Succeeded'

    sqlstr_error = f"""
        error_message = '{to_sql_value(error_message)}',
        error_file = '{to_sql_value(sys.app.error_file)}',
        """ if error_message else ''

    sqlstr_update = f"""UPDATE {full_table_name} SET
        total_minutes = {total_minutes},
        table_count = {sys.app.table_count},
        aggregate_file_size_kb = {sys.app.aggregate_file_size_kb},
        {sqlstr_error}
        run_status = '{run_status}'
    WHERE
        {ELT_PROCESS_ID_str.lower()} = '{data_settings.elt_process_id}'
    """

    with pymssql.connect(
        server = cloud_file_hist_conf['sql_server'],
        user = cloud_file_hist_conf['sql_id'],
        password = cloud_file_hist_conf['sql_pass'],
        database = cloud_file_hist_conf['sql_database'],
        appname = sys.app.parent_name,
        autocommit = True,
        ) as conn:
        with conn.cursor(as_dict=True) as cursor:
            conn._conn.execute_non_query(sqlstr_update)



sys.app.finalize_new_pipeline_instance = finalize_new_pipeline_instance



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

    finalize_new_pipeline_instance()



# %% Utility function to clear folder contents - USE with Caution!

@catch_error(logger)
def clear_folder_contents(folder_path:str):
    """
    Utility function to clear folder contents - USE with Caution!
    """
    for root, dirs, files in os.walk(folder_path):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))



# %% Utility function to convert JSON data to Spark RDD

@catch_error(logger)
def json_to_spark(spark, json_data):
    """
    Utility function to convert JSON data to Spark RDD
    """
    json_string = json.dumps(json_data)
    return spark.read.json(spark.sparkContext.parallelize([json_string]))



# %%



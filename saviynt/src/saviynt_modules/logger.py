"""
Module for logging, handling errors and sending alerts

"""

# %% Import Libraries

import os, sys, logging, json, pymsteams
from functools import wraps
from logging.handlers import RotatingFileHandler
from datetime import datetime



# %% Parameters



# %% Wrapper/Decorator function for catching errors

class CatchError:
    """
    Class for cathing and re-directing errors
    """
    def __init__(self):
        """
        Initiate the class
        """
        self.is_error = False # By default, there are no errors


    def catch(self, raise_error:bool=True):
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
                    if not self.is_error:
                        self.is_error = True
                        exception_message  = f"Exception occurred inside '{fn.__name__}'"
                        exception_message += f"\nException Message: {e}"
                        try:
                            logger.error(exception_message)
                        except:
                            print(exception_message)
                            print('ERROR: Logger is not defined, or error in logging')
                    if raise_error: raise e
                return response
            return inner
        return outer


catch_error_instance = CatchError()
catch_error = catch_error_instance.catch # simplify naming



# %% Get Environment Variable

@catch_error()
def get_env(variable_name:str, default:str=None, raise_error_if_no_value:bool=True):
    """
    Get Environment Variable
    """
    variable_name_clean = variable_name.upper().strip()
    value = os.environ.get(key=variable_name_clean, default=default)

    if not value:
        if raise_error_if_no_value:
            raise ValueError(f'Environment variable does not exist: {variable_name_clean}')
    elif value.isnumeric():
        value = float(value) if '.' in value else int(value)
    elif value.lower() in ['true', 'false']:
        value = value.lower()=='true'

    return value



# %%

class Environment:
    """
    Class to check wheter it is PROD or DEV environment
    """
    ENVIRONMENT_OPTIONS = {
        'DEBUG': 1,
        'DEV': 2,
        'QA': 3,
        'PROD': 4,
        }

    @catch_error()
    def __init__(self):
        """
        Initialize the class. Read environment
        """
        self.ENVIRONMENT = get_env(variable_name='ENVIRONMENT').upper().strip() # string form
        self.environment = self.ENVIRONMENT_OPTIONS[self.ENVIRONMENT] # number form

        for e in self.ENVIRONMENT_OPTIONS:
            setattr(self, f'is_{e.lower()}', self.ENVIRONMENT==e) # e.g: if environment.is_prod
            setattr(self, e.lower(), self.ENVIRONMENT_OPTIONS[e]) # e.g: if environment.environment < environment.prod
            setattr(self, e, e)


environment = Environment()



# %%

class RunDate:
    """
    Mark Start and End of Run
    """
    @catch_error()
    def __init__(self):
        """
        Initialize the Class and Mark Start Datetime
        """
        self.start = datetime.now()
        self.strftime = r'%Y-%m-%d %H:%M:%S'  # http://strftime.org/
        self.start_str =self. start.strftime(self.strftime) # in string form
        self.start = datetime.strptime(self.start_str, self.strftime) # to ensure identity with the string form of start date


    @catch_error()
    def end_run(self):
        """
        Mark End of Run
        """
        self.end = datetime.now()
        self.timedelta = self.end - self.start

        h = self.timedelta.seconds // 3600
        m = (self.timedelta.seconds - h * 3600) // 60
        s = self.timedelta.seconds - h * 3600 - m * 60
        total_time = f'{self.timedelta.days} day(s), {h} hour(s), {m} minute(s), {s} second(s)'

        run_info = {
            'run_start': self.start_str,
            'run_end': self.end.strftime(self.strftime),
            'total_seconds': self.timedelta.seconds,
            'total_time': total_time,
            }

        return run_info



# %% Create Logger with custom configuration

class CreateLogger:
    """
    Class for Logging Print Statements
    """
    log_format = logging.Formatter(fmt=r'%(asctime)s :: %(name)s :: %(levelname)-8s :: %(message)s', datefmt=r'%Y-%m-%d %H:%M:%S')

    log_folder_path = '../logs'

    class msg_type:
        DEBUG = 'DEBUG'
        INFO = 'INFO'
        WARNING = 'WARNING'
        ERROR = 'ERROR'


    def __init__(self):
        """
        Empty initialization, so that oncoming app can set the parent_name and other important info
        """
        self.run_date = RunDate()


    def set_logger(self, logging_level:int=logging.INFO, app_name:str=None, log_folder_path:str=None):
        """
        Initiate the class. Set Logging Policy.
        """
        self.environment = environment
        self.logging_level = logging_level

        self.app_name = app_name if app_name else 'logs'
        self.logger = logging.getLogger(self.app_name)
        self.logger.setLevel(level=self.logging_level)

        self.logger_func_map = {
            self.msg_type.DEBUG: self.logger.debug,
            self.msg_type.INFO: self.logger.info,
            self.msg_type.WARNING: self.logger.warning,
            self.msg_type.ERROR: self.logger.error,
            }

        if log_folder_path:
            self.log_folder_path = log_folder_path

        self.filter_out_unwanted_info_logs()
        self.add_stream_handlers()
        self.add_file_handler()

        self.info(f'Environment: {self.environment.ENVIRONMENT}, Run Date: {self.run_date.start_str}, PWD: {os.path.realpath(".")}')
        self.info(f'Logging Path: {os.path.realpath(self.log_folder_path)}')


    def filter_out_unwanted_info_logs(self, filter_log_level:int=logging.WARNING):
        """
        Filter out unwanted IMFO logs from other events. Keep Warning logs only.
        """
        warning_loggers = [ # Elevate the following loggers to WARNING level
            #'azure.core.pipeline.policies.http_logging_policy',
            #'azure.identity._internal.get_token_mixin',
            ]

        for warning_logger in warning_loggers:
            logging.getLogger(warning_logger).setLevel(filter_log_level)


    def add_stream_handlers(self):
        """
        Add handlers for I/O stream.
        """
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(self.logging_level)
        stdout_handler.addFilter(lambda record: record.levelno <= logging.WARNING)
        stdout_handler.setFormatter(self.log_format)
        self.logger.addHandler(stdout_handler)

        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(self.log_format)
        self.logger.addHandler(stderr_handler)


    def add_file_handler(self, log_folder_path:str=log_folder_path, file_size_mb:float=2.0, backupCount:int=3):
        """
        Add logging to file capabilities
        """
        self.log_folder_path = log_folder_path
        os.makedirs(self.log_folder_path, exist_ok=True)
        self.log_file = os.path.join(self.log_folder_path, f'{self.app_name}.log')

        file_handler = RotatingFileHandler(
            filename = self.log_file,
            mode = 'a',
            encoding = 'UTF-8',
            maxBytes = int(file_size_mb * (1024**2)), # max file size
            backupCount = backupCount, # max file backups
            )

        file_handler.setLevel(self.logging_level)
        file_handler.setFormatter(self.log_format)
        self.logger.addHandler(file_handler)


    def send_failure_notification(self, message:str):
        """
        Send failure notifications to MS Teams
        """
        if hasattr(self, 'msteams_webhook_url'):
            mtext = f'app = {self.app_name}; message = {message}'
            if hasattr(self, 'pipeline_key'):
                mtext = f'pipeline_key = {self.pipeline_key}; {mtext}'

            msteams_webhook = pymsteams.connectorcard(hookurl=self.msteams_webhook_url)
            msteams_webhook.text(mtext=mtext)
            msteams_webhook.send()


    def log(self, msg, msg_type, extra_log:dict={}):
        """
        Build and send log data
        """
        try:
            log_data = {
                'TimeGenerated': datetime.now(),
                'MainScript': self.app_name,
                'msg_type': msg_type,
                'msg': msg,
                **extra_log,
            }
            body = json.dumps(log_data, sort_keys=True, default=str, indent=4)

            logger_func = self.logger_func_map[msg_type]
            logger_func(msg, exc_info=False)

            if msg_type==self.msg_type.ERROR:
                self.send_failure_notification(message=body)

        except (BaseException, AssertionError) as e:
            print(e)


    def debug(self, msg): self.log(msg=msg, msg_type=self.msg_type.DEBUG)
    def info(self, msg): self.log(msg=msg, msg_type=self.msg_type.INFO)
    def warning(self, msg): self.log(msg=msg, msg_type=self.msg_type.WARNING)
    def error(self, msg): self.log(msg=msg, msg_type=self.msg_type.ERROR)


    def mark_ending(self):
        """
        Log Run End date and Run duration of the entire code
        """
        self.info(self.run_date.end_run())



logger = CreateLogger()



# %%



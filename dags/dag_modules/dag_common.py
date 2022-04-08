"""
Library for common DAG settings and functions

"""


# %% Import Libraries
import os

from urllib.parse import quote

from .teams_webhook import MSTeamsWebhookHook



# %% Spark Run Settings

spark_conn_id = 'spark_default'
num_executors = 3
executor_cores = 4
executor_memory = '16G'
spark_conf = {}

host_ip = os.environ.get(key='HOST_IP', default='')
port = '8282'
host_connection_type = 'http'



# %% Generic Parameters

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}

ingestion_path = '/opt/EDIP/ingestion'
src_path = f'{ingestion_path}/src'
jars_path = f'{ingestion_path}/drivers'



# %% Get Jars

def get_jars(jars_path:str):
    """
    Get comma separated paths for all the jars for a given path
    """
    jars = []

    if os.path.isdir(jars_path):
        for jar_file in os.listdir(jars_path):
            full_jar_path = os.path.join(jars_path, jar_file)
            if os.path.isfile(full_jar_path):
                file_ext = os.path.splitext(jar_file)[1]
                if file_ext.lower() in ['.jar']:
                    jars.append(full_jar_path)

    return ','.join(jars)



jars = get_jars(jars_path=jars_path)



# %% Construct Airflow log url

def get_log_url(context):
    """
    Construct Airflow log url
    """
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = quote(context['ts'], safe='')

    log_url = f"{host_connection_type}://{host_ip}:{port}/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"

    return log_url



# %% Function to run on task failure

def on_failure(context):
    """
    Function to run on task failure
    """
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id

    logs_url = get_log_url(context=context)

    teams_notification_hook = MSTeamsWebhookHook(
        http_conn_id='msteams_webhook',
        message=f"Failed Pipeline: {dag_id} Task: {task_id}",
        subtitle="Pipeline Failure",
        button_text="Logs",
        button_url=logs_url,
        theme_color="FF0000"
    )
    teams_notification_hook.execute()

    title = f"Title {dag_id} - {task_id}"
    body = title



default_args = {**default_args,
    'on_failure_callback': on_failure,
    }



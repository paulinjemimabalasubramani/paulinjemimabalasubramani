"""
Library for common DAG settings and functions

"""

# %% Import Libraries
import os

from .msteams_webhook import on_failure



# %% Host Address / Airflow Webserver

host_connection_type = 'http'
host_ip = os.environ.get(key='HOST_IP', default='')

airflow_webserver_port = '8282'
airflow_webserver_link = f'{host_connection_type}://{host_ip}:{airflow_webserver_port}'



# %% Default Settings

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
    'on_failure_callback': on_failure(airflow_webserver_link=airflow_webserver_link),
}

ingestion_path = '/opt/EDIP/ingestion'
src_path = f'{ingestion_path}/src'



# %% Spark Run Settings

spark_conn_id = 'spark_default'
num_executors = 3
executor_cores = 4
executor_memory = '16G'
spark_conf = {}
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



# %%



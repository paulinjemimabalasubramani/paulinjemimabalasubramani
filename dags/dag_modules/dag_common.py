"""
Library for common DAG settings and functions

"""


# %% Import Libraries
import os



# %% Spark Run Settings

spark_conn_id = 'spark_default'
num_executors = 3
executor_cores = 4
executor_memory = '16G'
spark_conf = {}



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



# %%



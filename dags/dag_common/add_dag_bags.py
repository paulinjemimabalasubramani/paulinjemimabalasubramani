"""
Add Dag Bags to Airflow from Central Location

"""


# %% Import Libraries

import os
from airflow.models import DagBag



# %% Parameters

dag_bag_root = '/opt/EDIP/ingestion/dags/dag_bags'

environments = {
    'PROD': {
        'exclude_folders': ['test_dags', 'metrics_dags'],
        },
    'QA': {
        'exclude_folders': ['ca_dags', 'assets_dags'],
        },
}



# %% Get Environment Variables

environment = os.environ.get(key='ENVIRONMENT', default='')
host_ip = os.environ.get(key='HOST_IP', default='')



# %% Get Dag Directories

def get_dag_dirs(dag_bag_root:str):
    """
    Get Dag Directories
    """
    dags_dirs = []
    if not os.path.isdir(dag_bag_root):
        return dags_dirs

    for dir_name in os.listdir(dag_bag_root):
        dir = os.path.join(dag_bag_root, dir_name)
        if os.path.isdir(dir):
            if dir_name in environments[environment.upper()]['exclude_folders']: continue
            dags_dirs.append(dir)

    return dags_dirs



dags_dirs = get_dag_dirs(dag_bag_root=dag_bag_root)



# %% Add Dag directories to Airflow

def add_dag_dirs(dags_dirs:list):
    """
    Add Dag directories to Airflow
    """
    for dir in dags_dirs:
        if not os.path.isdir(dir):
            print(f'Dag Bag directory is not found: {dir}')
            continue

        dag_bag = DagBag(os.path.realpath(dir))

        if not dag_bag or not dag_bag.dags:
            print(f'No Dag Bags found in directory {dir}')
            continue

        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag



add_dag_dirs(dags_dirs=dags_dirs)



# %%



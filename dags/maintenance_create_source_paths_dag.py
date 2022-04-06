# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



# %% Parameters

pipelinekey = 'MAINTENANCE_CREATE_SOURCE_PATHS'

airflow_app_name = pipelinekey.lower()
description_DAG = 'Create Paths for data source at the local server'

tags = ['Maintenance']

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}



# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = None, # https://crontab.guru/
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    copy_files = BashOperator(
        task_id = pipelinekey,
        bash_command = f'python /usr/local/spark/app/create_source_paths_3.py',
        dag = dag
    )


    startpipe >> copy_files



# %%


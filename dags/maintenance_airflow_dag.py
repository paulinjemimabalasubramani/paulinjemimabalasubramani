# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters

pipelinekey = 'MAINTENANCE_AIRFLOW_LOG_CLEANUP'

airflow_app_name = pipelinekey.lower()
description_DAG = 'Airflow Maintenance Log Cleanup'

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
    schedule_interval = '0 17 * * *', # https://crontab.guru/
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    log_cleanup = BashOperator(
        task_id = pipelinekey,
        bash_command = f'python /usr/local/spark/app/log_cleanup_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )


    startpipe >> log_cleanup



# %%


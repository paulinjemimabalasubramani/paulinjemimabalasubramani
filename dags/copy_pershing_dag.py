# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters

pipelinekey = 'COPY_PERSHING'

airflow_app_name = pipelinekey.lower()
description_DAG = 'Copy Pershing Tables from Remote'

tags = ['SC:Pershing']

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}



# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '12 */1 * * *', # https://crontab.guru/
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
        bash_command = f'python /usr/local/spark/app/copy_pershing_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )


    startpipe >> copy_files



# %%


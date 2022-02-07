# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



# %% Parameters
spark_master = "spark://spark:7077"
spark_executor_instances = 3
spark_master_ip = '10.128.25.82'


spark_app_name = "test_copy_files_assets_migrate_albridge_wfs"
airflow_app_name = spark_app_name
description_DAG = 'Test Copy Assets-Albridge files'

tags = ['DB:Assets', 'SC:Albridge']

default_args = {
    'owner': 'Seymur',
    'depends_on_past': False,
}


# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 13 * * *', # https://crontab.guru/#0_13_*_*_*
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    copy_files = BashOperator(
        task_id = 'COPY_FILES_ASSETS_MIGRATE_ALBRIDGE_WFS',
        bash_command = 'python /usr/local/spark/app/copy_files_3.py --pipelinekey ASSETS_MIGRATE_ALBRIDGE_WFS',
        dag = dag
    )

    startpipe >> copy_files



# %%


# %% Import Libraries

import os

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters

pipelinekey = 'METRICS_MIGRATE_ASSETS_RAA'
python_spark_code = 'migrate_csv_with_date_3'

airflow_app_name = pipelinekey.lower()
spark_app_name = airflow_app_name
description_DAG = 'Migrate Metrics-Assets Tables'

tags = ['DB:Metrics', 'SC:Assets']

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}

src_path = '/opt/EDIP/ingestion/src'



# %% Get Jars

jars_path = '/opt/EDIP/ingestion/drivers'

jars = []

if os.path.isdir(jars_path):
    for jar_file in os.listdir(jars_path):
        full_jar_path = os.path.join(jars_path, jar_file)
        if os.path.isfile(full_jar_path):
            jars.append(full_jar_path)

jars = ','.join(jars)



# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 13 * * *', # https://crontab.guru/
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    copy_files = BashOperator(
        task_id = f'COPY_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/copy_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    migrate_data = SparkSubmitOperator(
         task_id = pipelinekey,
         application = f"{src_path}/{python_spark_code}.py",
         name = spark_app_name,
         jars = jars,
         conn_id = "spark_default",
         conf = {},
         application_args = [
             '--pipelinekey', pipelinekey,
         ],
         dag = dag
         )

    delete_files = BashOperator(
        task_id = f'DELETE_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/delete_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    startpipe >> copy_files >> migrate_data >> delete_files




# %%



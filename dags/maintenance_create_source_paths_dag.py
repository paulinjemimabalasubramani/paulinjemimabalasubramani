# %% Import Libraries

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, jars, executor_cores, executor_memory, num_executors, src_path, spark_conn_id, spark_conf



# %% Pipeline Parameters

pipelinekey = 'MAINTENANCE_CREATE_SOURCE_PATHS'

tags = ['Maintenance']

schedule_interval = None # https://crontab.guru/



# %% Create DAG

with DAG(
    dag_id = pipelinekey.lower(),
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
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
        bash_command = f'python {src_path}/create_source_paths_3.py',
        dag = dag
    )

    startpipe >> copy_files




# %%



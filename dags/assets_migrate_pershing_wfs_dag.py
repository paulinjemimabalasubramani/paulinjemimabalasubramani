# %% Import Libraries

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, jars, executor_cores, executor_memory, num_executors, src_path, spark_conn_id, spark_conf



# %% Pipeline Parameters

pipelinekey = 'ASSETS_MIGRATE_PERSHING_WFS'
python_spark_code = 'migrate_pershing_3'

tags = ['DB:Assets', 'SC:Pershing']

schedule_interval = '50 */2 * * *' # https://crontab.guru/



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
        task_id = f'COPY_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/copy_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    add_bulk_id = BashOperator(
        task_id = f'ADD_BULK_ID_{pipelinekey}',
        bash_command = f'python {src_path}/pershing_bulk_id_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    migrate_data = SparkSubmitOperator(
        task_id = pipelinekey,
        application = f'{src_path}/{python_spark_code}.py',
        name = pipelinekey.lower(),
        jars = jars,
        conn_id = spark_conn_id,
        num_executors = num_executors,
        executor_cores = executor_cores,
        executor_memory = executor_memory,
        conf = spark_conf,
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

    startpipe >> copy_files >> add_bulk_id >> migrate_data >> delete_files




# %%



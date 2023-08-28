# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from datetime import timedelta

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path



# %% Pipeline Parameters

pipelinekey = 'REPLICA_MIGRATE_SUCCESSPRO_ACTIFI'
python_spark_code = 'migrate_csv_3'

tags = ['DB:REPLICA', 'SC:SUCCESSPRO_ACTIFI']

schedule_interval = '20 20 * * *' # https://crontab.guru/



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

    extractdata = BashOperator(
        task_id = f'API_{pipelinekey}',
        bash_command = f'python {src_path}/download_api_successpro_actifi_3.py --pipelinekey {pipelinekey}',
        dag = dag,
        execution_timeout = timedelta(seconds=7200), # set execution timeout for this API
    )

    start_pipe(dag) >> extractdata >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %%



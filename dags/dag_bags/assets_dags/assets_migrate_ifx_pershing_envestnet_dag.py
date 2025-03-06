# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path



# %% Pipeline Parameters

pipelinekey = 'ASSETS_MIGRATE_IFX_PERSHING_ENVESTNET'
python_spark_code = 'migrate_csv_with_date_3'

tags = ['DB:Assets', 'SC:ENVESTNET']

schedule_interval = '30 9,13,16 * * *' # https://crontab.guru/



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

    copy_files = BashOperator(
        task_id = f'COPY_ENVESTNET',
        bash_command = f'python {src_path}/copy_envestnet_3.py --pipelinekey {pipelinekey}',
        dag = dag,
        )

    start_pipe(dag) >> copy_files >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %%



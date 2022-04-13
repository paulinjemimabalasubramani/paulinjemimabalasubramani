# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, delete_files, src_path



# %% Pipeline Parameters

pipelinekey = 'FP_MIGRATE_SF'
python_spark_code = 'migrate_csv_3'

tags = ['DB:FP', 'SC:MIPS']

schedule_interval = '30 12 * * *' # https://crontab.guru/



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
        task_id = f'EXTRACT_SALESFORCE_{pipelinekey}',
        bash_command = f'python {src_path}/extract_sf.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    start_pipe(dag) >> extractdata >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %%



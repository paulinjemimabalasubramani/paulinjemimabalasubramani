# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path

from datetime import timedelta



# %% Pipeline Parameters

pipelinekey = 'REPLICA_MIGRATE_PW1SQLREPT01_SUPERVISIONCONTROLS'
python_spark_code = 'migrate_csv_3'

tags = ['DB:REPLICA', 'SC:PW1SQLREPT01_SUPERVISIONCONTROLS']

schedule_interval = '16 35 * * *' # https://crontab.guru/



# %% REPLICA_MIGRATE_PW1SQLREPT01_SUPERVISIONCONTROLS

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
        task_id = f'DOWNLOAD_{pipelinekey}',
        bash_command = f'python {src_path}/sql_server_download.py --pipelinekey {pipelinekey}',
        dag = dag,
        #execution_timeout = timedelta(seconds=7200), # set execution timeout for this API
    )


    start_pipe(dag) >> extractdata >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %% REPLICA_MIGRATE_PW1SQLREPT01_SUPERVISIONCONTROLS_MANUAL

with DAG(
    dag_id = (pipelinekey+'_MANUAL').lower(),
    default_args = default_args,
    description = (pipelinekey+'_MANUAL'),
    schedule_interval = None, # Manual Trigger
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    run_sql = BashOperator(
        task_id = f'API_{pipelinekey}',
        bash_command = f'python {src_path}/one_time_sql_script_run.py --pipelinekey {pipelinekey}',
        dag = dag,
        #execution_timeout = timedelta(seconds=7200), # set execution timeout for this API
    )

    start_pipe(dag) >> run_sql >> end_pipe(dag)



# %%



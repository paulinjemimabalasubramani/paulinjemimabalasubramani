# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, src_path



# %% Pipeline Parameters

tags = ['Maintenance']



# %% Create DAG

pipelinekey = 'MAINTENANCE_AIRFLOW_LOG_CLEANUP'

schedule_interval = '0 17 * * *' # https://crontab.guru/

with DAG(
    dag_id = pipelinekey.lower(),
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    log_cleanup = BashOperator(
        task_id = pipelinekey,
        bash_command = f'python {src_path}/log_cleanup_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    start_pipe(dag) >> log_cleanup >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %% Create DAG

pipelinekey = 'MAINTENANCE_CREATE_SOURCE_PATHS'

schedule_interval = None # https://crontab.guru/

with DAG(
    dag_id = pipelinekey.lower(),
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    create_source_paths = BashOperator(
        task_id = pipelinekey,
        bash_command = f'python {src_path}/create_source_paths_3.py',
        dag = dag
    )

    start_pipe(dag) >> create_source_paths >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %%



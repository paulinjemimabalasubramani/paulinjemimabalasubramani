# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from datetime import timedelta

from dag_modules.dag_common import default_args, start_pipe, end_pipe, saviynt_src_path



# %% Pipeline Parameters

pipelinekey_prefix = 'saviynt_'

source_map = {
    'mips': '30 */1 * * *',
    'sabos': '33 */1 * * *',
    'investalink': '36 */1 * * *',
    'one_time_load': None,
    }



# %% Create DAGs

def create_dag(source, schedule_interval):
    """
    Create DAG
    """
    pipelinekey = pipelinekey_prefix + source
    tags = ['db:saviynt', f'sc:{source}']

    with DAG(
        dag_id = pipelinekey.lower(),
        default_args = default_args,
        description = pipelinekey,
        schedule_interval = schedule_interval,
        start_date = days_ago(1),
        tags = tags,
        catchup = False,
        ) as dag:

        main_pipeline = BashOperator(
            task_id = f'{pipelinekey}_task',
            bash_command = f'python {saviynt_src_path}/csv_load.py --pipeline_key {pipelinekey}',
            dag = dag,
            execution_timeout = timedelta(seconds=7200),
            )

        start_pipe(dag) >> main_pipeline >> end_pipe(dag)


for source, schedule_interval in source_map.items():
    create_dag(source=source, schedule_interval=schedule_interval)



# %%



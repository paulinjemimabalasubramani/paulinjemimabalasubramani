# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files



# %% Pipeline Parameters

pipelinekey_prefix = 'ASSETS_MIGRATE_ALBRIDGE_MULTIRUN_'
python_spark_code = 'assets_migrate_albridge_multirun_3'

tags = ['DB:Assets', 'SC:Albridge_MULTIRUN']

firm_map = {
   
    'RAA': '*/30 * * * *'
    
    }



# %% Create DAGs per firm

def create_dag(firm_name, schedule_interval):
    """
    Create DAG
    """
    pipelinekey = pipelinekey_prefix + firm_name

    dag = DAG(
        dag_id = pipelinekey.lower(),
        default_args = default_args,
        description = pipelinekey,
        schedule_interval = schedule_interval,
        start_date = days_ago(1),
        tags = tags,
        catchup = False,
        )

    with dag:
        start_pipe(dag) >> copy_files(dag, pipelinekey) >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



for firm_name, schedule_interval in firm_map.items():
    create_dag(firm_name=firm_name, schedule_interval=schedule_interval)



# %%



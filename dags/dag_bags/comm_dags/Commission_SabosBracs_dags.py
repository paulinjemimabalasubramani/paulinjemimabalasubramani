# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files



# %% Pipeline Parameters

pipelinekey_prefix = 'COMMISSION_'
python_spark_code = 'Commission_Sabos_Bracs_load'

tags = ['DB:Comm', 'SC:Client_Revenue']

source_map = {
    'SABOS': '0 19 * * 4',
    'BRACS': '0 23 * * 4',
    }



# %% Create DAGs per Source

def create_dag(source_name, schedule_interval):
    """
    Create DAG
    """
    pipelinekey = pipelinekey_prefix + source_name

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



for source_name, schedule_interval in source_map.items():
    create_dag(source_name=source_name, schedule_interval=schedule_interval)



# %%



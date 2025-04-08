# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path



# %% Pipeline Parameters

pipelinekey_prefix = 'ASSETS_MIGRATE_PERSHING_'
python_spark_code = 'migrate_pershing_3'

tags = ['DB:Assets', 'SC:Pershing']

firm_map = {
    #'FSC': '55 */2 * * *',
    'RAA': '50 */2 * * *',
    'SAI': '45 */2 * * *',
    #'SPF': '40 */2 * * *',
    'TRI': '35 */2 * * *',
    #'WFS': '30 */2 * * *',
    'IFX': '25 */2 * * *',
    'APH': '20 */2 * * *',
    'RAA_EXPANDED': '50 */2 * * *',
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

    add_bulk_id = BashOperator(
        task_id = f'ADD_BULK_ID_{pipelinekey}',
        bash_command = f'python {src_path}/pershing_bulk_id_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    process_multiline_files = BashOperator(
        task_id=f'PROCESS_MULTILINE_FILES_{pipelinekey}',
        bash_command=f'python {src_path}/pershing_process_multiline_files.py --pipelinekey {pipelinekey}',
        dag=dag
    )

    with dag:
        start_pipe(dag) >> copy_files(dag, pipelinekey) >> process_multiline_files >> add_bulk_id >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



for firm_name, schedule_interval in firm_map.items():
    create_dag(firm_name=firm_name, schedule_interval=schedule_interval)



# %%



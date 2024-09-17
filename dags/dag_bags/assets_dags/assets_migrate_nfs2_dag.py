# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path



# %% Pipeline Parameters

pipelinekey_prefix = 'ASSETS_MIGRATE_NFS2_'
python_spark_code = 'migrate_nfs_3'

tags = ['DB:Assets', 'SC:NFS2']

firm_map = {
    'FSC': '36 */2 * * *',
    'RAA': '39 */2 * * *',
    'SAI': '42 */2 * * *',
    'SPF': '45 */2 * * *',
    'TRI': '49 */2 * * *',
    'WFS': '53 */2 * * *',
    'OFS': '57 */2 * * *',
    'LFA_DAILY': '57 */2 * * *',
    'LFA_WEEKLY': '57 */2 * * *',
    'LFS_DAILY': '57 */2 * * *',
    'LFS_WEEKLY': '57 */2 * * *',
    'HISTORY': None,
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

    convert_to_json = BashOperator(
        task_id = f'CONVERT_TO_JSON_{pipelinekey}',
        bash_command = f'python {src_path}/nfs2_to_json_3.py --pipelinekey {pipelinekey}',
        dag = dag,
        )

    with dag:
        start_pipe(dag) >> copy_files(dag, pipelinekey) >> convert_to_json >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



for firm_name, schedule_interval in firm_map.items():
    create_dag(firm_name=firm_name, schedule_interval=schedule_interval)



# %%



# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, src_path, start_pipe, end_pipe



# %% Pipeline Parameters

pipelinekey = 'COPY_NFS_TRI'

tags = ['SC:NFS']

schedule_interval = '10 */1 * * *' # https://crontab.guru/



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
        task_id = pipelinekey,
        bash_command = f'python {src_path}/copy_nfs_tri_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    start_pipe(dag) >> copy_files >> end_pipe(dag)



# %%



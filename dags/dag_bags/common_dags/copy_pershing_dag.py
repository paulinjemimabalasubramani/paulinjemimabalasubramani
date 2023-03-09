# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, src_path, start_pipe, end_pipe



# %% Pipeline Parameters

tags = ['SC:Pershing']



# %% Create DAG

pipelinekey = 'COPY_PERSHING'

schedule_interval = '12 */1 * * *' # https://crontab.guru/

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
        bash_command = f'python {src_path}/copy_pershing_3.py --pipelinekey {pipelinekey} --is_zip Y',
        dag = dag
    )

    start_pipe(dag) >> copy_files >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %%

pipelinekey = 'COPY_PERSHING_SAI'

schedule_interval = '13 */1 * * *' # https://crontab.guru/


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
        bash_command = f'python {src_path}/copy_pershing_3.py --pipelinekey {pipelinekey} --is_zip N',
        dag = dag
    )

    start_pipe(dag) >> copy_files >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %%

pipelinekey = 'COPY_PERSHING_IFX'

schedule_interval = '14 */1 * * *' # https://crontab.guru/


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
        bash_command = f'python {src_path}/copy_pershing_ifx_3.py --pipelinekey {pipelinekey} --is_zip Y',
        dag = dag
    )

    start_pipe(dag) >> copy_files >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %%

pipelinekey = 'COPY_PERSHING_APH'

schedule_interval = '15 */1 * * *' # https://crontab.guru/


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
        bash_command = f'python {src_path}/copy_pershing_aph_3.py --pipelinekey {pipelinekey} --is_zip Y',
        dag = dag
    )

    start_pipe(dag) >> copy_files >> end_pipe(dag)

    globals()[dag.safe_dag_id] = dag



# %%



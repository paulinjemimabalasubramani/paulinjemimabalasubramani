# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data



# %% Pipeline Parameters

pipelinekey = 'FP_MIGRATE_LR'
python_spark_code = 'migrate_sql_3'

tags = ['DB:FP', 'SC:LR']

schedule_interval = '0 12 * * *' # https://crontab.guru/



# %% Create DAG

with DAG(
    dag_id = pipelinekey.lower() + '_sql',
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    start_pipe(dag) >> migrate_data(dag, pipelinekey, python_spark_code) >> end_pipe(dag)



# %%



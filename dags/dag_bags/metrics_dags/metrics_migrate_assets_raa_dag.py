# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files



# %% Pipeline Parameters

pipelinekey = 'METRICS_MIGRATE_ASSETS_RAA'
python_spark_code = 'migrate_csv_with_date_3'

tags = ['DB:Metrics', 'SC:Assets']

schedule_interval = '0 14 * * *' # https://crontab.guru/



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

    start_pipe(dag) >> copy_files(dag, pipelinekey) >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %%



# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, migrate_data, start_pipe, end_pipe



# %% Pipeline Parameters

pipelinekey = 'REVERSE_ETL_FP_EDIP'
python_spark_code = 'reverse_etl_3'

tags = ['DB:SNOWFLAKE']

schedule_interval = '0 2 * * *' # https://crontab.guru/



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

    start_pipe(dag) >> migrate_data(dag, pipelinekey, python_spark_code) >> end_pipe(dag)



# %%



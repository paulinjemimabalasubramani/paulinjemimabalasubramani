# %% Import Libraries

from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

from dag_modules.dag_common import default_args, jars, executor_cores, executor_memory, num_executors, src_path, spark_conn_id, spark_conf, start_pipe, end_pipe



# %% Pipeline Parameters

pipelinekey = 'REVERSE_ETL'
python_spark_code = 'reverse_etl'

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

    migrate_data = SparkSubmitOperator(
        task_id = pipelinekey,
        application = f'{src_path}/{python_spark_code}.py',
        name = pipelinekey.lower(),
        jars = jars,
        conn_id = spark_conn_id,
        num_executors = num_executors,
        executor_cores = executor_cores,
        executor_memory = executor_memory,
        conf = spark_conf,
        application_args = [
            '--pipelinekey', pipelinekey,
            ],
        dag = dag
        )

    start_pipe(dag) >> migrate_data >> end_pipe(dag)



# %%



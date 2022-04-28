"""
Library for common DAG settings and functions

"""

# %% Import Libraries

import os
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_modules.msteams_webhook import on_failure, on_success, sla_miss



# %% Ingestion Path

ingestion_path = '/opt/EDIP/ingestion'
src_path = f'{ingestion_path}/src'



# %% Host Address / Airflow Webserver

host_connection_type = 'http'
host_ip = os.environ.get(key='HOST_IP', default='')

airflow_webserver_port = '8282'
airflow_webserver_link = f'{host_connection_type}://{host_ip}:{airflow_webserver_port}'



# %% Default Settings

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
    'on_failure_callback': on_failure(airflow_webserver_link=airflow_webserver_link),
    'on_success_callback': on_success(airflow_webserver_link=airflow_webserver_link),
    'sla_miss_callback': sla_miss(airflow_webserver_link=airflow_webserver_link),
    'sla': timedelta(hours=2),
}



# %% Spark Run Settings

spark_conn_id = 'spark_default'
num_executors = 3
executor_cores = 4
executor_memory = '16G'
jars_path = f'{ingestion_path}/drivers'
spark_conf = {}



# %% Get Jars

def get_jars(jars_path:str):
    """
    Get comma separated paths for all the jars for a given path
    """
    jars = []

    if os.path.isdir(jars_path):
        for jar_file in os.listdir(jars_path):
            full_jar_path = os.path.join(jars_path, jar_file)
            if os.path.isfile(full_jar_path):
                file_ext = os.path.splitext(jar_file)[1]
                if file_ext.lower() in ['.jar']:
                    jars.append(full_jar_path)

    return ','.join(jars)



jars = get_jars(jars_path=jars_path)



# %% Common start pipe task

def start_pipe(dag):
    """
    Common start pipe task
    """
    def fail_if_dag_running(dag_id):
        from airflow.models.dagrun import DagRun
        from airflow.utils.state import State

        dags_running = DagRun.find(dag_id=dag_id, state=State.RUNNING)
        dags_queued = DagRun.find(dag_id=dag_id, state=State.QUEUED)
        if len(dags_running)>1 or len(dags_queued)>0:
            raise RuntimeError('There is more than 1 instance of the same Dag running')

        return 'Start Pipeline'

    start_pipe_op = PythonOperator(
        task_id = 'START_PIPE',
        python_callable = fail_if_dag_running,
        op_kwargs = {'dag_id' : dag.safe_dag_id},
        dag = dag,
    )

    return start_pipe_op



# %% Common end pipe task

def end_pipe(dag):
    """
    Common end pipe task
    """
    end_pipe_op = PythonOperator(
        task_id = 'END_PIPE',
        python_callable = lambda: 'End Pipeline',
        dag = dag,
    )

    return end_pipe_op



# %% Common SparkSubmitOperator

def migrate_data(dag, pipelinekey:str, python_spark_code:str):
    """
    Common SparkSubmitOperator
    """
    migrate_data_op = SparkSubmitOperator(
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

    return migrate_data_op



# %% Common copy_files

def copy_files(dag, pipelinekey:str):
    """
    Common copy_files
    """
    copy_files_op = BashOperator(
        task_id = f'COPY_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/copy_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    return copy_files_op



# %% Common delete_files

def delete_files(dag, pipelinekey:str):
    """
    Common delete_files
    """
    delete_files_op = BashOperator(
        task_id = f'DELETE_FILES_{pipelinekey}',
        bash_command = f'python {src_path}/delete_files_3.py --pipelinekey {pipelinekey}',
        dag = dag
    )

    return delete_files_op



# %%



# %% Import Libraries

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
import glob
import os
import time



# %% Parameters
spark_master = "spark://spark:7077"
spark_app_name = "Send logs to azure"
airflow_app_name = "Logging_task"
description_DAG='Reverse ETL from Snowflake to SQL Server'
file_path = "/usr/local/spark/resources/data/airflow.cfg"

default_args = {
    'owner': 'Jared',
    'depends_on_past': False,
}


# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 13 * * *',
    start_date = days_ago(1),
) as dag:
    current_datetime = datetime.now()
    info = "CAT"

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )
    LogMove = BashOperator(
    task_id='Send_a_log',
    bash_command="python /usr/local/spark/app/test_log.py",
    )
    



    startpipe >> LogMove


# %%


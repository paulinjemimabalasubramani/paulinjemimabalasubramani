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
# Update the customer ID to your Log Analytics workspace ID
customer_id = '4600d8ac-b3ed-400e-88fc-f87d24f3470c'
# Where the logs are
log_dir='/usr/local/airflow/logs'
# For the shared key, use either the primary or the secondary Connected Sources client authentication key   
shared_key = "AgrNWMU+6Mcyv7d6nSkMEkZ8tmZtwZWlN0wlN3w/NGoyHLQ9MHHtF5To1TBVFwnWymt6V8f1F9gzAI87Chvieg=="

# The log type is the name of the event that is being submitted
log_type = 'AirflowPipelineSchedule'

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
    bash_command=post_data(customer_id, shared_key, body, log_type),
    )
    



    startpipe 


# %%


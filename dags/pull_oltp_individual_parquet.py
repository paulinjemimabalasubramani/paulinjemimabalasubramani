from datetime import timedelta, datetime

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

import os

#from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__



###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Pull Data from OLTP.Individual"
airflow_app_name = "pull-data-from-oltp-individual"
description_DAG='Get data from OLTP Individual and save as Parquet'
file_path = "/usr/local/spark/resources/data/airflow.cfg"





default_args = {
    'owner': 'S',
    'depends_on_past': False,
}


with DAG(
    airflow_app_name,
    default_args=default_args,
    description=description_DAG,
    schedule_interval=timedelta(hours=24),
    start_date=days_ago(2),
) as dag:
    current_datetime = datetime.now()
    info = "CAT"

    startpipe = BashOperator(
        task_id='Start_Pipe',
        bash_command='echo "Start Pipeline"'
    )

    sparkjob = SparkSubmitOperator(
         task_id="spark_job_1",
         application="/usr/local/spark/app/spark_sql_connection_sm.py", # mapped to M:\EDIP-Code\src
         name=spark_app_name,
         jars="/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/mssql-jdbc_auth-9.2.1.x64.dll",
         conn_id="spark_default",
         num_executors=2,
         executor_cores=3,
         executor_memory="3G",
         verbose=1,
         conf={"spark.master":spark_master},
         application_args=[file_path],
         dag=dag)



    startpipe >> [sparkjob]

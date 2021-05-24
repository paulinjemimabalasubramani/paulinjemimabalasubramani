from datetime import timedelta, datetime
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__



###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "Spark Send Metadata"
file_path = "/usr/local/spark/resources/data/airflow.cfg"
firmlist = ["raa","faa","sao"]

default_args = {
    'owner': 'JaredF',
    'depends_on_past': False,
}

with DAG(
    'Spark-Send',
    default_args=default_args,
    description='A simple Cat DAG',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(2),
) as dag:
    current_datetime = datetime.now()
    firmnum = len(firmlist)
    info = "CAT"
    startpipe = BashOperator(
        task_id='Start_Pipe',
        bash_command='echo "Start Pipeline for " + firmlist'
    )
    for firm in firmlist:
        spark_job = SparkSubmitOperator(
            task_id="spark_job" + str(firm),
                application="/usr/local/spark/app/get_tables.py",
                name=spark_app_name,
                packages="org.apache.spark:spark-sql_2.12:3.0.1",
                jars="/usr/local/spark/resources/jars/spark-mssql-connector_2.11-1.0.0.jar",
                conn_id="spark_default",
                verbose=1,
                conf={"spark.master":spark_master},
                application_args=[file_path],
                dag=dag)



        startpipe >> [spark_job]

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
spark_app_name = "Get table List from OLTP"
file_path = "/usr/local/spark/resources/data/airflow.cfg"









default_args = {
    'owner': 'JaredF',
    'depends_on_past': False,
}

with DAG(
    'Get-Azure-File',
    default_args=default_args,
    description='DAG',
    schedule_interval=timedelta(hours=24),
    start_date=days_ago(2),
) as dag:
    current_datetime = datetime.now()
    info = "CAT"
    startpipe = BashOperator(
        task_id='Start_Pipe',
        bash_command='echo "Start Pipeline for "'
    )
 #   for firm in firmlist:
    spark_job = SparkSubmitOperator(
         task_id="spark_job",
         application="/usr/local/spark/app/read-file-azure.py",
         name=spark_app_name,
         jars="/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-2.7.3.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-2.2.0.jar",
        conn_id="spark_default",         num_executors=2,
         executor_cores=3,
         executor_memory="3G",
         verbose=1,
         conf={"spark.master":spark_master},
         application_args=[file_path],
         dag=dag)



    startpipe >> [spark_job]

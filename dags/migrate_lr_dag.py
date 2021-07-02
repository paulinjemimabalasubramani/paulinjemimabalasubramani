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
spark_app_name = "Migrate LR Tables"
airflow_app_name = "migrate_lr"
description_DAG='Migrate LR Tables'
file_path = "/usr/local/spark/resources/data/airflow.cfg"





default_args = {
    'owner': 'Seymur',
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
    ##Create SQLoperator to check schema drift
    ##if schema run data_type_translation.py

    migratelrdata = SparkSubmitOperator(
         task_id="migrate_lr",
         application="/usr/local/spark/app/migrate_lr.py", # mapped to M:\EDIP-Code\src
         name=spark_app_name,
         jars="/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar",
         conn_id="spark_default",
         num_executors=2,
         executor_cores=4,
         executor_memory="16G",
         verbose=1,
         conf={"spark.master":spark_master},
         application_args=[file_path],
         dag=dag)

    createsaverunsql = SparkSubmitOperator(
         task_id="snowflake_ddl_lr",
         application="/usr/local/spark/app/snowflake_ddl_lr.py", # mapped to M:\EDIP-Code\src
         name=spark_app_name,
         jars="/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar",
         conn_id="spark_default",
         num_executors=2,
         executor_cores=4,
         executor_memory="16G",
         verbose=1,
         conf={"spark.master":spark_master},
         application_args=[file_path],
         dag=dag)


    startpipe >> [migratelrdata] >> createsaverunsql 

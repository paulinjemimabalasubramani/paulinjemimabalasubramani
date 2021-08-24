# %% Import Libraries

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters
spark_master = "spark://spark:7077"
spark_app_name = "Upload Config Files"
airflow_app_name = "upload_config"
description_DAG = 'Upload Config Files'
file_path = "/usr/local/spark/resources/data/airflow.cfg"


default_args = {
    'owner': 'Seymur',
    'depends_on_past': False,
}


# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = None,
    start_date = None,
) as dag:
    current_datetime = datetime.now()
    info = "CAT"

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    upload_config_csv_files = SparkSubmitOperator(
         task_id = "upload_config_csv_files",
         application = "/usr/local/spark/app/upload_config_csv_files.py", # mapped to ..\EDIP-Code\src
         name = spark_app_name,
         jars = "/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar,/usr/local/spark/resources/jars/spark-xml_2.12-0.12.0.jar",
         conn_id = "spark_default",
         num_executors = 2,
         executor_cores = 2,
         executor_memory = "8G",
         verbose = 1,
         conf = {"spark.master":spark_master},
         application_args = [file_path],
         dag = dag
         )

    data_type_translation_lr = SparkSubmitOperator(
         task_id = "data_type_translation_lr",
         application = "/usr/local/spark/app/data_type_translation_lr.py", # mapped to ..\EDIP-Code\src
         name = spark_app_name,
         jars = "/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar,/usr/local/spark/resources/jars/spark-xml_2.12-0.12.0.jar",
         conn_id = "spark_default",
         num_executors = 2,
         executor_cores = 4,
         executor_memory = "16G",
         verbose = 1,
         conf = {"spark.master":spark_master},
         application_args = [file_path],
         dag = dag
         )


    startpipe >> [upload_config_csv_files] >> data_type_translation_lr 


# %%


# %% Import Libraries

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters
spark_master = "spark://spark:7077"
spark_app_name = "Migrate SalesForce Tables"
airflow_app_name = "migrate_sf"
description_DAG='Migrate SalesForce Tables'
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
    schedule_interval = '0 13 * * *',
    start_date = days_ago(1),
) as dag:
    current_datetime = datetime.now()
    info = "CAT"

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    extractdata = BashOperator(
        task_id = 'extract_salesforce_data',
        bash_command = 'python /usr/local/spark/app/extract_sf.py',
        dag = dag
    )


    migratedata = SparkSubmitOperator(
         task_id = "migrate_salesforce",
         application = "/usr/local/spark/app/migrate_sf.py", # mapped to ..\EDIP-Code\src
         name = spark_app_name,
         jars = "/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar,/usr/local/spark/resources/jars/spark-xml_2.12-0.12.0.jar",
         conn_id = "spark_master",
         num_executors = 3,
         executor_cores = 4,
         executor_memory = "16G",
         verbose = 1,
         conf = {"spark.master":spark_master},
         application_args = [file_path],
         dag = dag
         )

    startpipe >> extractdata >> migratedata



# %%


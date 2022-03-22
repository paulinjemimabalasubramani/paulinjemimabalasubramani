# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters
spark_master = "spark://spark:7077"


spark_app_name = "Load Snowflake log data to Azure Monitor"
airflow_app_name = "snowflake_logging"
description_DAG = 'Load Snowflake log data to Azure Monitor'


default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}


jars = "/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar,/usr/local/spark/resources/jars/spark-xml_2.12-0.12.0.jar,/usr/local/spark/resources/jars/spark-snowflake_2.12-2.9.1-spark_3.1.jar,/usr/local/spark/resources/jars/snowflake-jdbc-3.13.6.jar"



# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 */2 * * *', # https://crontab.guru/#0_*/2_*_*_*
    start_date = days_ago(1),
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    snowflake_logging = SparkSubmitOperator(
         task_id = "snowflake_logging",
         application = "/usr/local/spark/app/snowflake_logging.py",
         name = spark_app_name,
         jars = jars,
         conn_id = "spark_default",
         num_executors = 3,
         #executor_cores = 4,
         #executor_memory = "16G",
         #verbose = 1,
         conf = {"spark.master": spark_master},
         application_args = None,
         dag = dag
         )

    startpipe >> [snowflake_logging]


# %%


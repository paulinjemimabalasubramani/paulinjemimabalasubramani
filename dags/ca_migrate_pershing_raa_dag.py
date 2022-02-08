# %% Import Libraries

from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago



# %% Parameters
spark_master = "spark://spark:7077"
spark_executor_instances = 3
spark_master_ip = '10.128.25.82'

spark_app_name = "ca_migrate_pershing_raa"
airflow_app_name = spark_app_name
description_DAG = 'Migrate CA-Pershing Tables'

tags = ['DB:CA', 'SC:Pershing']

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}


jars = "/usr/local/spark/resources/jars/delta-core_2.12-1.0.0.jar,/usr/local/spark/resources/jars/jetty-util-9.3.24.v20180605.jar,/usr/local/spark/resources/jars/hadoop-common-3.3.0.jar,/usr/local/spark/resources/jars/hadoop-azure-3.3.0.jar,/usr/local/spark/resources/jars/mssql-jdbc-9.2.1.jre8.jar,/usr/local/spark/resources/jars/spark-mssql-connector_2.12_3.0.1.jar,/usr/local/spark/resources/jars/azure-storage-8.6.6.jar,/usr/local/spark/resources/jars/spark-xml_2.12-0.12.0.jar"


# %% Create DAG

with DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 13 * * *',
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    startpipe = BashOperator(
        task_id = 'Start_Pipe',
        bash_command = 'echo "Start Pipeline"'
    )

    add_bulk_id = BashOperator(
        task_id = 'ADD_BULK_ID_CA_MIGRATE_PERSHING_RAA',
        bash_command = 'python /usr/local/spark/app/bulk_fixed_width_files_3.py --pipelinekey CA_MIGRATE_PERSHING_RAA',
        dag = dag
    )

    CA_MIGRATE_PERSHING_RAA = SparkSubmitOperator(
         task_id = "CA_MIGRATE_PERSHING_RAA",
         application = "/usr/local/spark/app/migrate_pershing_3.py",
         name = spark_app_name,
         jars = jars,
         conn_id = "spark_default",
         num_executors = spark_executor_instances,
         executor_cores = 4,
         executor_memory = "16G",
         verbose = 1,
         conf = {"spark.master": spark_master},
         application_args = [
             '--pipelinekey', 'CA_MIGRATE_PERSHING_RAA',
             '--spark_master', spark_master,
             '--spark_executor_instances', str(spark_executor_instances),
             #'--spark_master_ip', spark_master_ip,
             ],
         dag = dag
         )


    startpipe >> add_bulk_id >> CA_MIGRATE_PERSHING_RAA





# %%


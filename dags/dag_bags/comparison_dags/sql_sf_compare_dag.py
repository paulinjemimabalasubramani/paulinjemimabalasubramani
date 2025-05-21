# %% Import Libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, src_path, migrate_data

# %% Pipeline Parameters
pipelinekey = 'SQL_SNOWFLAKE_DATA_COMPARE'
python_script = 'sql_sf_data_compare.py'

tags = ['DataQuality', 'Validation', 'SQL-Snowflake']

# Schedule to run every 6 hours
schedule_interval = '0 */6 * * *'

# %% Create DAG
dag = DAG(
    dag_id=pipelinekey.lower(),
    default_args=default_args,
    description='Compare data between SQL Server and Snowflake',
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    tags=tags,
    catchup=False,
)

# Task dependencies
with dag:
    start_pipe(dag) >> migrate_data(dag, pipelinekey, python_script) >> end_pipe(dag)

# Register DAG in the global namespace
globals()[dag.safe_dag_id] = dag

# %%
# %% Import Libraries
from airflow import DAG
from airflow.utils.dates import days_ago
from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data

# %% Pipeline Parameters
pipelinekey = 'net_new_assets_extract'
python_script = 'snowflake_outbound_extract'

tags = ['Extract']

# %% Create DAG
dag = DAG(
    dag_id=pipelinekey.lower(),
    default_args=default_args,
    description='Extract data from Snowflake',
    schedule_interval=None,
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
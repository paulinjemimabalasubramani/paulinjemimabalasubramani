# %% Import Libraries

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

from dag_modules.dag_common import default_args, start_pipe, end_pipe, migrate_data, copy_files, delete_files, src_path

# %% Pipeline Parameters

pipelinekey = 'COMM_MIGRATE_CLIENTREVENUE_INCREMENT_DAILY'
python_spark_code = 'migrate_csv_3'

tags = ['DB:COMM', 'SC:CLIENT_REVENUE']

schedule_interval = '0 7 * * *' # https://crontab.guru/

# %% COMM_MIGRATE_CLIENTREVENUE_INCREMENT_DAILY

with DAG(
    dag_id = pipelinekey.lower(),
    default_args = default_args,
    description = pipelinekey,
    schedule_interval = schedule_interval,
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    extractdata = BashOperator(
        task_id = f'DOWNLOAD_{pipelinekey}',
        bash_command = f'python {src_path}/sql_server_download.py --pipelinekey {pipelinekey}',
        dag = dag,
        #execution_timeout = timedelta(seconds=7200), # set execution timeout for this API
    )


    start_pipe(dag) >> extractdata >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)



# %% COMM_MIGRATE_CLIENTREVENUE_INCREMENT_DAILY_MANUAL

with DAG(
    dag_id = (pipelinekey+'_MANUAL').lower(),
    default_args = default_args,
    description = (pipelinekey+'_MANUAL'),
    schedule_interval = None, # Manual Trigger
    start_date = days_ago(1),
    tags = tags,
    catchup = False,
) as dag:

    start_pipe(dag) >> migrate_data(dag, pipelinekey, python_spark_code) >> delete_files(dag, pipelinekey) >> end_pipe(dag)

################ ONE TIME HISTORY PIPELINE #######################

pipeline_one_time_history_key = 'COMM_MIGRATE_CLIENTREVENUE_INCREMENT_ONE_TIME_HISTORY'
# 1130 pm to 330 am and every 36 mints interval
history_schedule_interval = '*/36 4-8 * * *'

# %% COMM_MIGRATE_CLIENTREVENUE_INCREMENT_ONE_TIME_HISTORY

with DAG(
    dag_id=(pipeline_one_time_history_key).lower(),
    default_args=default_args,
    description=(pipeline_one_time_history_key),
    schedule_interval=history_schedule_interval,
    start_date=days_ago(1),
    tags=tags,
    catchup=False,
) as dag:

    extractdata = BashOperator(
        task_id = f'DOWNLOAD_{pipeline_one_time_history_key}',
        bash_command = f'python {src_path}/sql_server_download.py --pipelinekey {pipeline_one_time_history_key}',        
        dag = dag
    )

    status_update_in_history_config_file = BashOperator(
        task_id = f'STATUS_UPDATE_IN_HISTORY_CONFIG_FILE_{pipeline_one_time_history_key}',
        bash_command = f'python {src_path}/status_update_in_history_config_file.py --pipelinekey {pipeline_one_time_history_key}',
        dag = dag
    )
    
    start_pipe(dag) >> extractdata >> migrate_data(dag, pipeline_one_time_history_key, python_spark_code) >> delete_files(dag, pipeline_one_time_history_key) >> status_update_in_history_config_file >> end_pipe(dag)


# %%



# %% Import Libraries

import logging

from airflow.models.dag import DAG
from airflow.configuration import conf
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# %% Parameters

airflow_app_name = 'airflow_log_cleanup'
description_DAG = 'Remove old logs from Airflow server'

BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")
if not BASE_LOG_FOLDER or BASE_LOG_FOLDER.strip() == "": raise ValueError("BASE_LOG_FOLDER variable is empty in airflow.cfg.")

CHILD_PROCESS_LOG_DIRECTORY = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY").rstrip("/")
if not CHILD_PROCESS_LOG_DIRECTORY or CHILD_PROCESS_LOG_DIRECTORY.strip() == "": raise ValueError("CHILD_PROCESS_LOG_DIRECTORY variable is empty in airflow.cfg.")


DEFAULT_MAX_LOG_AGE_IN_DAYS = 30

ENABLE_DELETE = True

AIRFLOW_HOSTS = "localhost" # comma separated list of host(s)

TEMP_LOG_CLEANUP_SCRIPT_PATH = "/tmp/airflow_log_cleanup.sh"

DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]

ENABLE_DELETE_CHILD_LOG = True

logging.info("ENABLE_DELETE_CHILD_LOG  " + ENABLE_DELETE_CHILD_LOG)

if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get(
            "scheduler", "CHILD_PROCESS_LOG_DIRECTORY"
        )
        if CHILD_PROCESS_LOG_DIRECTORY != ' ':
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception as e:
        logging.exception(
            "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from " +
            "Airflow Configurations: " + str(e)
        )

default_args = {
    'owner': 'EDIP',
    'depends_on_past': False,
}


# %% Create DAG

dag = DAG(
    airflow_app_name,
    default_args = default_args,
    description = description_DAG,
    schedule_interval = '0 14 * * *', # https://crontab.guru/
    start_date = days_ago(1),
    tags = ['Maintenance', 'Airflow'],
    catchup = False,
)

log_cleanup = """
echo "Getting Configurations..."

BASE_LOG_FOLDER=$1
MAX_LOG_AGE_IN_DAYS=$2
ENABLE_DELETE=$3

echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      \'${BASE_LOG_FOLDER}\'"
echo "MAX_LOG_AGE_IN_DAYS:  \'${MAX_LOG_AGE_IN_DAYS}\'"
echo "ENABLE_DELETE:        \'${ENABLE_DELETE}\'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=$(eval $1)
    echo "Process will be Deleting the following files or directories:"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting $(echo "${FILES_MARKED_FOR_DELETE}" |
        grep -v \'^$\' | wc -l) files or directories"

    echo ""
    if [ "${ENABLE_DELETE}" == "true" ]; then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ]; then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code  \'${DELETE_STMT_EXIT_CODE}\'"

                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No files or directories to Delete"
        fi
    else
        echo "WARN: You have opted to skip deleting the files or directories"
    fi
}

echo ""
echo "Running Cleanup Process..."

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

echo "Finished Running Cleanup Process"
"""

create_log_cleanup_script = BashOperator(
    task_id=f'create_log_cleanup_script',
    bash_command=f"""
    echo '{log_cleanup}' > {TEMP_LOG_CLEANUP_SCRIPT_PATH}
    chmod +x {TEMP_LOG_CLEANUP_SCRIPT_PATH}
    current_host=$(echo $HOSTNAME)
    echo "Current Host: $current_host"
    hosts_string={AIRFLOW_HOSTS}
    echo "All Scheduler Hosts: $hosts_string"
    IFS=',' read -ra host_array <<< "$hosts_string"
    for host in "${{host_array[@]}}"
    do
        if [ "$host" != "$current_host" ]; then
            echo "Copying log_cleanup script to $host..."
            scp {TEMP_LOG_CLEANUP_SCRIPT_PATH} $host:{TEMP_LOG_CLEANUP_SCRIPT_PATH}
            echo "Making the script executable..."
            ssh $host "chmod +x {TEMP_LOG_CLEANUP_SCRIPT_PATH}"
        fi
    done
    """,
    dag=dag)

for host in AIRFLOW_HOSTS.split(","):
    for DIR_ID, DIRECTORY in enumerate(DIRECTORIES_TO_DELETE):
        LOG_CLEANUP_COMMAND = f'{TEMP_LOG_CLEANUP_SCRIPT_PATH} {DIRECTORY} {DEFAULT_MAX_LOG_AGE_IN_DAYS} {str(ENABLE_DELETE).lower()}'
        cleanup_task = BashOperator(
            task_id=f'airflow_log_cleanup_{host}_dir_{DIR_ID}',
            bash_command=f"""
            echo "Executing cleanup script..."
            ssh {host} "{LOG_CLEANUP_COMMAND}"
            echo "Removing cleanup script..."
            ssh {host} "rm {TEMP_LOG_CLEANUP_SCRIPT_PATH}"
            """,
            dag=dag)

        cleanup_task.set_upstream(create_log_cleanup_script)



from datetime import timedelta, datetime
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

firmlist = ["raa","faa","sao"]


def send_metadata_file_azure(i,current_datetime):
    container_name = "ingress"
    date = str(current_datetime.year) + '_' + str(current_datetime.month) + '_' + str(current_datetime.day)
    time = str(current_datetime.hour) + 'h_' + str(current_datetime.minute) + 'm_' + str(current_datetime.second) + "s"
    tablename = "Metadata_" + i + "_" + str(date) + "_" + str(time)
    local = "./toupload"
    current_date = str(current_datetime.year) + '/' + str(current_datetime.month) + '/' + str(current_datetime.day)

    # environment variable into account.
    connect_str = "DefaultEndpointsProtocol=https;AccountName=aglakewest2;AccountKey=TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg==;EndpointSuffix=core.windows.net"
    print(connect_str)
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a local directory to hold blob data
    try:
        local_path = local
        os.mkdir(local_path)
    except:
        local_path = local
    # Create a file in the local data directory to upload and download
    local_file_name = str(tablename) + ".txt"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
    file = open(upload_file_path, 'w')
    file.write(f"Current timestamp of {current_datetime} for {i} firms \n When Complete, Json data will show up here")
    file.close()

    # Create a blob client using the local file name as the name for the blob
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="metadata/" + local_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    with open(upload_file_path, "rb") as data:
        for blob in container_client.list_blobs():
                if blob.name == local_file_name:
                    print("Deleting exisiting")
                    blob_client.delete_blob()
        try:
            print("Saving blob")
            blob_client.upload_blob(data)
        except Exception as e:
            for blob in container_client.list_blobs():
                print("blob: ", blob.name)
def send_data_file_azure(i,current_datetime):
    container_name = "ingress"
    date = str(current_datetime.year) + '_' + str(current_datetime.month) + '_' + str(current_datetime.day)
    time = str(current_datetime.hour) + 'h_' + str(current_datetime.minute) + 'm_' + str(current_datetime.second) + "s"
    tablename = "Data_" + i + "_" + str(date) + "_" + str(time)
    local = "./toupload"

    # environment variable into account.
    connect_str = "DefaultEndpointsProtocol=https;AccountName=aglakewest2;AccountKey=TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg==;EndpointSuffix=core.windows.net"
    print(connect_str)
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a local directory to hold blob data
    try:
        local_path = local
        os.mkdir(local_path)
    except:
        local_path = local
    # Create a file in the local data directory to upload and download
    local_file_name = str(tablename) + ".txt"
    upload_file_path = os.path.join(local_path, local_file_name)

    # Write text to the file
    file = open(upload_file_path, 'w')
    file.write(f"Current timestamp of {current_datetime} for {i} firms \n When Complete, Json data will show up here")
    file.close()

    # Create a blob client using the local file name as the name for the blob
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="data-ingress/" + local_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    with open(upload_file_path, "rb") as data:
        for blob in container_client.list_blobs():
                if blob.name == local_file_name:
                    print("Deleting exisiting")
                    blob_client.delete_blob()
        try:
            print("Saving blob")
            blob_client.upload_blob(data)
        except Exception as e:
            for blob in container_client.list_blobs():
                print("blob: ", blob.name)


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'JaredF',
    'depends_on_past': False,
    'email': ['NotMY@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'Table_Metadata_Send',
    default_args=default_args,
    description='A simple Cat DAG',
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(2),
) as dag:
    current_datetime = datetime.now()
    firmnum = len(firmlist)
    info = "CAT"
    startpipe = BashOperator(
        task_id='Start_Pipe',
        bash_command='echo "Start Pipeline for " + firmlist'
    )    
    for firm in firmlist:
        sendschema = PythonOperator(
        task_id='send_Metadata_' + firm,
        python_callable=send_metadata_file_azure,
        op_kwargs={'i': firm, 'current_datetime': current_datetime},
        )
        sendtable = PythonOperator(
        task_id='send_Tabledata_' + firm,
        python_callable=send_data_file_azure,
        op_kwargs={'i': firm, 'current_datetime': current_datetime},
        )


        startpipe >> [sendschema,sendtable]

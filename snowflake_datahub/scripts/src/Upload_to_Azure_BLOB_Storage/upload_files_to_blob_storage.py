import os
import sys
import time
from Azure_utils import get_storage_config, get_azure_credentials
from Upload_files_utils import handle_zip_file, upload_to_azure, validate_file
from Snowflake_utils import get_processing_info_from_snowflake

# Main function to handle file uploads
def upload_file_to_azure(source:str, file_to_upload:str):
    """Main function to handle file uploads."""
    start_time = time.time()

    print (f"source : {source} , file_to_upload : {file_to_upload}")

    if not os.path.exists(file_to_upload):
        print(f"Error: File {file_to_upload} is not found")
        sys.exit(1)

    try:
        azure_creds = get_azure_credentials()
        storage_config = get_storage_config(azure_creds)
        processing_info = get_processing_info_from_snowflake(azure_creds , source)  # Fetch once

        if file_to_upload.endswith('.zip'):
            handle_zip_file(storage_config, source, file_to_upload, processing_info)
        else:
            processing_type = validate_file(source, os.path.basename(file_to_upload), processing_info)
            if processing_type:
                upload_to_azure(storage_config, source, file_to_upload, processing_type)
   
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")


if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: python Main_Function.py <source> <file_to_upload>")
        sys.exit(1)

    source = sys.argv[1]
    file_to_upload = sys.argv[2]
    
    upload_file_to_azure(source,file_to_upload)


#Example Usage
#python upload_files_to_blob_storage.py APH "C:\\Users\\pjemima\\Downloads\\APH_Assets.zip"

import os
import zipfile
import tempfile

def validate_file(source, file_name, processing_info):

    '''Validates whether a file should be processed based on processing_info'''
    print(f"processing_info: {processing_info}")

    # Iterate only through relevant file_name_prefixes for this source
    for file_name_prefix in processing_info.keys():
        # Check if file_name_prefix is a substring of file_name
        if file_name_prefix.upper() in file_name.upper():
            file_storage_location = processing_info[file_name_prefix]["file_storage_location"]
            print(f"Matched: {file_name} -> {file_name_prefix} -> Prcoessing Type is {file_storage_location}")
            return file_storage_location  # Return processing location if valid
    
    print(f"Skipping file {file_name}: No matching entry found in Snowflake for source {source} or the file is inactive")
   
    return None  # Return None if no match found


def upload_to_azure(storage_config, source, file_to_upload, processing_type):
    """Uploads a file to Azure Blob Storage using pre-fetched processing type."""
   
    blob_service_client = storage_config["blob_service_client"]
    container = storage_config["container"]
    storage_upload_location = storage_config["storage_upload_location"]

    file_name = os.path.basename(file_to_upload)

    # Construct Blob Name
    blob_name = f"{processing_type}/{source}/{file_name}"
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)

    # Upload File to Azure Blob Storage
    with open(file_to_upload, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    print(f"File '{file_to_upload}' uploaded successfully to {storage_upload_location}/{blob_name}")


# Function to handle zip files
def handle_zip_file(storage_config, source, zip_file_path, processing_info):
    """Extracts files from a ZIP archive and uploads them based on pre-fetched processing info."""
    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            if len(zip_ref.namelist()) == 0:
                print(f"Warning: Zip file {zip_file_path} is empty. Skipping upload.")
                return
            zip_ref.extractall(temp_dir)

        for root, _, files in os.walk(temp_dir):
            for file in files:
                file_path = os.path.join(root, file)
                processing_type = validate_file(source, file, processing_info)

                if processing_type:
                    upload_to_azure(storage_config, source, file_path, processing_type)

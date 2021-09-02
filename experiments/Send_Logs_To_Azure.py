import glob
import os
import time
from modules.azure_functions import build_signature, post_data



## Update the customer ID to your Log Analytics workspace ID
customer_id = '4600d8ac-b3ed-400e-88fc-f87d24f3470c'

## For the shared key, use either the primary or the secondary Connected Sources client authentication key   
shared_key = "AgrNWMU+6Mcyv7d6nSkMEkZ8tmZtwZWlN0wlN3w/NGoyHLQ9MHHtF5To1TBVFwnWymt6V8f1F9gzAI87Chvieg=="

## The log type is the name of the event that is being submitted
log_type = 'AirflowPipelineSchedule'


dir_name = '/usr/local/airflow/logs'
# Get list of all files only in the given directory
list_of_files = filter( os.path.isfile,
                        glob.glob(dir_name + 'scheduler') )
# Sort list of files based on last modification time in ascending order
list_of_files = sorted( list_of_files,
                        key = os.path.getmtime)
# Iterate over sorted list of files and print file path 
# along with last modification time of file 
for file_path in list_of_files:
    timestamp_str = time.strftime(  '%m/%d/%Y :: %H:%M:%S',
                                time.gmtime(os.path.getmtime(file_path))) 
    print(timestamp_str, ' -->', file_path)




#post_data(customer_id, shared_key, body, log_type)


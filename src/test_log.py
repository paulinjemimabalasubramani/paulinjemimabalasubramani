# %%
from modules.azure_functions import log_saved_data, post_data

table_to_save = 'OLTP_Branch'
storage_account_name = 'test_sorage_account'
container_name = 'test_container_name'
container_folder = 'container_folder_test'
table_name = 'table_name_test'
partitionBy = 'partitionBy_test'
file_format = 'file_format_test'

#log_saved_data(table_to_save,storage_account_name,container_name,container_folder,table_name,partitionBy,file_format)

log_data = {'a':1}
log_type = 'test_log_type'

customer_id = '4600d8ac-b3ed-400e-88fc-f87d24f3470c'
shared_key = 'iWnQWu49tOZr/Io1RAbvDskx/L7BsAeXNtS/pDti9RG4IsUXBT4WeKUu18Lo/jt9c1C/aAohBxBH6zMVoMPMIw=='

print(f'\ncustomer_id: {customer_id}')
print(f'\nshared_key: {shared_key}')
print(f'\nlog_data: {log_data}\n')

post_data(customer_id, shared_key, log_data, log_type)




# %%

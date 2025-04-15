description = """

Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_NFS2_RAA',
        'source_path': r'C:\myworkdir\data\NFS',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs2_schema',
        }

# %% Import Libraries

import os,sys
class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger

@catch_error(logger)
def handle_nfs_multiline_files():
    print(f'source_path : {data_settings.source_path}')
    for root, dir, files in os.walk(data_settings.source_path):
        for file_name in files:            
            if "MAJ_TRANSFER" in file_name:
                source_file_path = os.path.join(root, file_name)
                with open(file=source_file_path, mode='rt') as f:
                    lines = f.readlines()
                    HEADER = lines[0]
                    TRAILER = lines[-1]
                    
                    TRANSFER_HEADER = HEADER.replace('ASSET TRANSFER', 'TRANSFER      ')
                    ASSET_HEADER = HEADER.replace('ASSET TRANSFER', 'ASSET         ')
                    
                    transfer_file_split_path=data_settings.transfer_file_split_path

                    if not os.path.exists(transfer_file_split_path):
                        os.makedirs(transfer_file_split_path)
                    
                    # Determine the suffix for the split files
                    date_suffix = ""
                    parts = file_name.split("_")
                    if len(parts) > 2 and parts[-1].split(".")[0].isdigit():
                        date_suffix = "_" + parts[-1].split(".")[0]

                    aca_transfer = open(os.path.join(transfer_file_split_path, f'MAJ_ASSET_TRANSFER_SUMMARY{date_suffix}.DAT'), 'w')
                    aca_asset = open(os.path.join(transfer_file_split_path, f'MAJ_ASSET_TRANSFER_DETAIL{date_suffix}.DAT'), 'w')

                    aca_transfer.write(TRANSFER_HEADER)
                    aca_asset.write(ASSET_HEADER)

                    for line in lines[1:-1]:
                        if line.startswith(' ') and line.strip() != '': continue
                        if(line[0] == '1'):
                            aca_transfer.write(line)
                        else:
                            aca_asset.write(line)

                    aca_transfer.write(TRAILER)
                    aca_asset.write(TRAILER)
                    
                    aca_transfer.close()
                    aca_asset.close()

handle_nfs_multiline_files()

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
        'pipelinekey': 'ASSETS_MIGRATE_PERSHING_RAA',
        'source_path': r'C:\myworkdir\data\pershing3',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\pershing_schema',
        }

# %% Import Libraries

import os,sys
class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger

@catch_error(logger)
def handle_pershing_multiline_files():
    print(f'source_path : {data_settings.source_path}')
    for root, dir, files in os.walk(data_settings.source_path):
        for file_name in files:            
            if (file_name =='ACA2.ACA2'):
                source_file_path = os.path.join(root, file_name)
                with open(file=source_file_path, mode='rt') as f:
                    lines = f.readlines()
                    HEADER = lines[0]
                    TRAILER = lines[-1]
                    
                    TRANSFER_HEADER = HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY')
                    ASSET_HEADER = HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL ')
                    
                    TRANSFER_HEADER = TRANSFER_HEADER.replace('BOF      PERSHING','BOFPERSHING')
                    ASSET_HEADER = ASSET_HEADER.replace('BOF      PERSHING','BOFPERSHING')

                    TRANSFER_TRAILER = TRAILER
                    ASSET_TRAILER = TRAILER.replace('TRANSFER', 'ASSET   ')

                    aca_transfer = open(os.path.join(root, 'asset_transfer_summary.ACA2'), 'w')
                    aca_asset = open(os.path.join(root, 'asset_transfer_detail.ACA2'), 'w')

                    aca_transfer.write(TRANSFER_HEADER)
                    aca_asset.write(ASSET_HEADER)

                    for line in lines[1:-1]:
                        # Char 25 will define that type of record
                        # 0 => Transfer record
                        # 1 => Equity Asset record
                        # 2 => Option Asset record
                        # 3 => Mutula Fund Asset record
                        if(line[24] == '0'):
                            aca_transfer.write(line)
                        else:
                            aca_asset.write(line)

                    aca_transfer.write(TRANSFER_TRAILER + '\n')
                    aca_asset.write(ASSET_TRAILER + '\n')
                    
                    aca_transfer.close()
                    aca_asset.close()

handle_pershing_multiline_files()

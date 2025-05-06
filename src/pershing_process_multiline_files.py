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

import os,sys,zipfile
class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger

@catch_error(logger)

def handle_pershing_multiline_files():
    print(f'source_path : {data_settings.source_path}')
    for root, dirs, files in os.walk(data_settings.source_path):
        for file_name in files:
            print(f'file_name : {file_name}')
            if 'ACA2' in file_name and file_name.upper().endswith('.ZIP'):
                zip_path = os.path.join(root, file_name)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(root)
                    print(f'Extracted: {zip_path}')

        # After extraction, process the unzipped files
        for file_name in os.listdir(root):
            if 'ACA2' in file_name and not file_name.upper().endswith('.ZIP'):
                source_file_path = os.path.join(root, file_name)
                with open(file=source_file_path, encoding='ISO-8859-1', mode='rt') as f:
                    lines = f.readlines()
                    HEADER = lines[0]
                    TRAILER = lines[-1]

                    output_files = {
                        'A': ('asset_transfer_summary_record_a', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD A'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD A')),
                        'B': ('asset_transfer_summary_record_b', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD B'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD B')),
                        'C': ('asset_transfer_summary_record_c', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD C'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD C')),
                        'D': ('asset_transfer_summary_record_d', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD D'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD D')),
                        'E': ('asset_transfer_summary_record_e', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD E'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD E')),
                        'F': ('asset_transfer_summary_record_f', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD F'), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER SUMMARY RECORD F')),
                        '1': ('asset_transfer_detail_record_1', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 1 '), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 1 ')),
                        '2': ('asset_transfer_detail_record_2', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 2 '), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 2 ')),
                        '3': ('asset_transfer_detail_record_3', HEADER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 3 '), TRAILER.replace('ACCOUNT TRANSFER', 'ASSET TRANSFER DETAIL RECORD 3 '))
                    }

                    # Determine the suffix for the split files
                    date_suffix = ""
                    parts = file_name.split("_")
                    if len(parts) > 1 and parts[-1].split(".")[0].isdigit():
                        date_suffix = "_" + parts[-1].split(".")[0]

                    file_handles = {key: open(os.path.join(root, f'{value[0]}{date_suffix}.ACA2'), 'w') for key, value in output_files.items()}

                    for key, value in output_files.items():
                        file_handles[key].write(value[1])

                    for line in lines[1:-1]:
                        # Char 25 will define that type of record
                        # 0 => Transfer record
                        # 1 => Equity Asset record
                        # 2 => Option Asset record
                        # 3 => Mutula Fund Asset record
                        char_pos_25 = line[24]
                        char_pos_03 = line[2]

                        if char_pos_25 == '0' and char_pos_03 in file_handles:
                            file_handles[char_pos_03].write(line)
                        elif char_pos_25 in ['1','2','3']:
                            file_handles[char_pos_25].write(line)

                    for key, value in output_files.items():
                        file_handles[key].write(value[2])
                        file_handles[key].close()

handle_pershing_multiline_files()

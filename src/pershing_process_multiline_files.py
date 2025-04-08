import os
from modules3.common_functions import catch_error, data_settings, logger

@catch_error(logger)
def handle_pershing_multiline_files():
    for root, dir, files in os.walk(data_settings.source_path):
        for file_name in files:
            if (file_name =='ACA2.ACA2'):
                source_file_path = os.path.join(root, file_name)
                try:
                    with open(file=source_file_path, mode='rt') as f:
                        lines = f.readlines()
                        HEADER = lines[0]
                        TRAILER = lines[-1]
                        TRANSFER_HEADER = HEADER
                        ASSET_HEADER = HEADER.replace('TRANSFER', 'ASSET   ')
                        TRANSFER_TRAILER = TRAILER
                        ASSET_TRAILER = TRAILER.replace('TRANSFER', 'ASSET   ')

                    aca_transfer = open('aca_transfer_detail.ACA2', 'w')
                    aca_asset = open('aca_asset_detail.ACA2', 'w')

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

                finally:
                    aca_transfer.close()
                    aca_asset.close()

handle_pershing_multiline_files()

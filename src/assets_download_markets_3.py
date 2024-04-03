description = """
Download Symbols vs. Price data from Yahoo

"""


# %% Parse Arguments

if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'ASSETS_MIGRATE_MARKETS',
        'source_path': r'C:\myworkdir\data\MARKETS',
        #'symbols_list': 'AGG,VBAIX,^GSPC',
        #'table_name': 'markets',
        }



# %% Import Libraries

import os, sys

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end

import urllib.request, json, csv
from datetime import datetime



# %% Parameters

url_string = lambda symbol: f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range=30y&interval=1d'

csv_encoding = 'utf-8'
csv_delimiter = ','
csv_quotechar = '"'



# %%

@catch_error(logger)
def get_symbols():
    """
    Get list of symbols
    """
    symbols = data_settings.symbols_list.split(sep=',')
    symbols = [x.upper().strip() for x in symbols if x.upper().strip()]

    return symbols



# %%

@catch_error(logger)
def fetch_data(symbol:str):
    """
    Fetch symbol data from web and clean data
    """
    logger.info(f'Fetching: {symbol}')
    contents = urllib.request.urlopen(url_string(symbol)).read()
    contents = json.loads(contents)

    timestamp = contents['chart']['result'][0]['timestamp']
    timestamp = [datetime.fromtimestamp(x).date() for x in timestamp]

    clean_data = {
        'date': timestamp,
        'symbol': [contents['chart']['result'][0]['meta']['symbol']] * len(timestamp),
        'exchange_name': [contents['chart']['result'][0]['meta']['exchangeName']] * len(timestamp),
        'instrument_type': [contents['chart']['result'][0]['meta']['instrumentType']] * len(timestamp),
    }

    for x in ['volume', 'high', 'low', 'open', 'close']:
        clean_data[x] = contents['chart']['result'][0]['indicators']['quote'][0][x]

    clean_data['adjclose'] = contents['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']

    return clean_data



# %%

@catch_error(logger)
def download_all_symbols(symbols:list[str]):
    """
    Download symbols one by one and save to file as CSV
    """
    os.makedirs(data_settings.source_path, exist_ok=True)
    output_file = os.path.join(data_settings.source_path, data_settings.table_name.strip().lower() + '.csv')

    first_flag = True
    with open(output_file, mode='w', newline='', encoding=csv_encoding) as out_file:
        for symbol in symbols:
            try:
                clean_data = fetch_data(symbol)
            except Exception as e:
                logger.error(f'Error while trying to download symbol {symbol}. Error Message: {str(e)}')
                continue

            if first_flag:
                first_flag = False
                out_writer = csv.DictWriter(out_file, delimiter=csv_delimiter, quotechar=csv_quotechar, quoting=csv.QUOTE_ALL, skipinitialspace=True, fieldnames=clean_data.keys())
                out_writer.writeheader()

            logger.info(f'Writing to file {output_file}')
            for i in range(len(clean_data['date'])):
                out_writer.writerow({x:clean_data[x][i] for x in clean_data.keys()})



# %%

if __name__ == '__main__':
    symbols = get_symbols()
    download_all_symbols(symbols)
    mark_execution_end()



# %%



if True: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description='HISTORY_CONFIG_FILE_UPDATE')

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'COMM_MIGRATE_CLIENTREVENUE_INCREMENT_ONE_TIME_HISTORY',
        'delete_files_after': 'TRUE',
        'source_path': r'C:\myworkdir\Shared\ALBRIDGE\WFS',
        }

import os, sys, csv

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error,logger, data_settings, mark_execution_end
from airflow.models import Variable, XCom

@catch_error(logger)
def status_update_in_history_config_file(ti=None):
    """
    Update the status of the processed quarter in history_date.csv to COMPLETE.    
    """

    csv_file_path = data_settings.get_value('one_time_history_csv_config_path',None)

    if not csv_file_path:
        logger.info('Skipping onetime history status since one_time_history_csv_config_path is empty')
        return   
    
    history_year_quarter = ti.xcom_pull(task_ids='COMM_MIGRATE_CLIENTREVENUE_INCREMENT_ONE_TIME_HISTORY', key='history_year_quarter')
    logger.info(f"Process completed for history_year_quarter : {history_year_quarter}")

    history_dates = []

    with open(csv_file_path, mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                history_dates.append(row)
                
    if history_dates:        
        for row in history_dates:
           if row['YEAR_QUARTER'] == history_year_quarter:
               logger.info(f"YEAR_QUARTER {history_year_quarter} to update the status")
               row['STATUS'] = 'COMPLETE'
               break
                
        with open(csv_file_path, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=history_dates[0].keys())
            writer.writeheader()
            writer.writerows(history_dates)

status_update_in_history_config_file()

# %% Close Connections / End Program

mark_execution_end()

# %%

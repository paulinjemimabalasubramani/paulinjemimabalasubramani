import csv
from modules3.common_functions import catch_error, data_settings

@catch_error(logger)
def status_update_in_history_config_file():
    """
    Update the status of the processed quarter in history_date.csv to COMPLETE.
    """
    if not data_settings.getvalue('one_time_history_csv_config_path'):
        logger.info('Skipping onetime history status since one_time_history_csv_config_path is empty')
        return   
    
    with open(data_settings.getvalue('one_time_history_csv_config_path'), mode='r', newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                history_dates.append(row)
    
    for row in history_dates:
        if row['YEAR_QUARTER'] == data_settings['year_quarter']:
            row['STATUS'] = 'COMPLETE'
            break
            
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=history_dates[0].keys())
        writer.writeheader()
        writer.writerows(history_dates)

status_update_in_history_config_file()

# %%

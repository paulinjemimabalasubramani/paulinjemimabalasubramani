# %% Import Libraries

import os, shutil, pymsteams, pyodbc, csv, re
from functools import wraps


# sp_spaceused 'AUMHistory.PositionHistoryByPlatform'


# %% Parameters

pipeline_name = 'PW1SQLDATA01_DW'

connection_string = 'DRIVER={SQL Server};SERVER=PW1SQLDATA01;DATABASE=DW;Trusted_Connection=yes;'

tables = [
    ('com.F_12B1Trails', 'select * from com.F_12B1Trails(nolock) where CONVERT(DATE, CONVERT(VARCHAR,StatementDateID), 112) >= DATEADD(WEEK, -2, GETDATE());'),
    ('com.F_Commission', 'select * from com.F_Commission(nolock) where CONVERT(DATE, CONVERT(VARCHAR,StatementDateID), 112) >= DATEADD(WEEK, -2, GETDATE());'),
    ('com.F_CommissionSplit', 'select * from com.F_CommissionSplit(nolock) where CONVERT(DATE, CONVERT(VARCHAR,StatementDateID), 112) >= DATEADD(WEEK, -2, GETDATE());'),
    'com.F_RevenuePeriod',
    'dim.D_BDPerson',
    'dim.D_CommissionSplitType',
    'dim.d_commissiontype',
    'dim.D_Product',
    'dim.D_Rep',
    'Dim.D_RepHierarchy',
]


tables = [
#    'Com.F_CommissionClientRevenue',
    'dim.D_ClientMaster',
    'com.F_RepPaymentHistory',
    'Com.AdvisorClientSegmentation',
    'Householding.HouseholdingMaster',
    'AUMHistory.PositionHistoryByPlatform',
    'dim.D_OfficeRange',
]


tables = [
    ('Com.F_CommissionClientRevenue', "select * from Com.F_CommissionClientRevenue(nolock) where left(StatementDateID, 4) in ('2023', '2024');"),
]


# %% Constants

is_dev = os.environ.get('ENVIRONMENT', '') in ['QA', 'DEV']

msteams_webhook_url = 'https://advisorgroup.webhook.office.com/webhookb2/17ec5d27-9782-46ab-9c9a-49a9cb61aab6@c1ef4e97-eeff-48b2-b720-0c8480a08061/IncomingWebhook/f0acf95ca6a44c72b579ac27fdab4b6f/4d1ebaa8-54ba-41cc-841e-1cb24f3b2eea'

batch_size = 100000

file_ext = '.txt'
zip_file_ext = '.zip'
name_regex:str = r'[\W]+'


# %% File Paths

if is_dev:
    zip_file_path = fr'C:\EDIP_Files\manual_download\{pipeline_name}\zip'
    data_file_path = fr'C:\EDIP_Files\manual_download\{pipeline_name}\data'
else:
    zip_file_path = fr'D:\EDIP_Files\{pipeline_name}'
    data_file_path = fr'./data/{pipeline_name}'



# %%

def send_failure_notification(message:str):
    """
    Send failure notifications to MS Teams
    """
    print(message)
    msteams_webhook = pymsteams.connectorcard(msteams_webhook_url)
    msteams_webhook.text(f'pipeline = {pipeline_name}; message = {message}')
    msteams_webhook.send()



# %%

def catch_error():
    """
    Wrapper/Decorator function for catching errors
    """
    def outer(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            response = None
            try:
                response = fn(*args, **kwargs)
            except (BaseException, AssertionError) as e:
                send_failure_notification(str(e))
            return response
        return inner
    return outer


# %%

def normalize_name(name:str):
    """
    Clean up name and make it standard looking
    """
    name = re.sub(r'\[|\]|\"', '', str(name).lower().strip())
    name = re.sub(name_regex, '_', str(name).strip())

    if name and name[0].isdigit():
        name = '_' + name

    return name



# %%

def create_folder_paths(list_of_paths:list):
    """
    Create folder paths if not exists
    """
    for file_path in list_of_paths:
        if not os.path.isdir(file_path):
            os.makedirs(file_path, exist_ok=True)



# %%

def table_name_to_file_name(table_name:str):
    """
    Convert table name to full path of file name
    """
    file_name = (normalize_name(table_name) + file_ext).lower()
    file_name = os.path.join(data_file_path, file_name)
    return file_name



# %%

def delete_files(folder_path, file_ext):
    """
    Delete files mathing file extension
    """
    for root, dirs, files in os.walk(folder_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            if file_path.lower().endswith(file_ext.lower()):
                try:
                    os.remove(file_path)
                except Exception as e:
                    send_failure_notification(str(e))



# %%

@catch_error()
def dump_data_to_file(file_name:str, sql_query:str):
    """
    Dump data from odbc connection to a file
    """
    with pyodbc.connect(connection_string, autocommit=False) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)

        with open(file=file_name, mode='wt', newline='') as csvfile:
            datawriter = csv.writer(csvfile, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_ALL)

            datawriter.writerow([column[0].lower() for column in cursor.description])

            k = 0
            while True:
                k += 1
                print(f'File {file_name} batch {k}')
                rows = cursor.fetchmany(batch_size)
                if len(rows) == 0: break
                for row in rows:
                    datawriter.writerow(row)

        conn.rollback()



# %%

def create_table_files():
    """
    Create csv file for each table
    """
    for table in tables:
        if isinstance(table, list) or isinstance(table, tuple):
            table_name = table[0]
            sql_query = table[1]
        elif isinstance(table, str):
            table_name = table
            sql_query = f'select * from {table_name} (nolock);'
        else:
            send_failure_notification( f'Invalid table type specified: {table}')
            continue

        if sql_query.lower().find('(nolock)')<0:
            send_failure_notification(f'(nolock) is not implemented in SQL query: {sql_query}')
            continue

        file_name = table_name_to_file_name(table_name=table_name)
        dump_data_to_file(file_name=file_name, sql_query=sql_query)



# %%

def zip_files():
    """
    zip the files
    """
    shutil.make_archive(os.path.join(zip_file_path, pipeline_name.lower()), zip_file_ext[1:], data_file_path)



# %%

@catch_error()
def main():
    """
    Main procedure
    """
    create_folder_paths(list_of_paths=[data_file_path, zip_file_path])

    delete_files(folder_path=data_file_path, file_ext=file_ext)
    create_table_files()

    delete_files(folder_path=zip_file_path, file_ext=zip_file_ext)
    zip_files()

    delete_files(folder_path=data_file_path, file_ext=file_ext)



# %%

if __name__ == "__main__":
    main()



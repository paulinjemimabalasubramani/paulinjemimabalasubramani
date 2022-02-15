description = """

Add Bulk_id to Fixed Width Files

"""


# %% Parse Arguments

if False: # Set to False for Debugging
    import argparse

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--pipelinekey', '--pk', help='PipelineKey value from SQL Server PipelineConfiguration', required=True)
    parser.add_argument('--spark_master', help='URL of the Spark Master to connect to', required=False)
    parser.add_argument('--spark_executor_instances', help='Number of Spark Executors to use', required=False)
    parser.add_argument('--spark_master_ip', help='Spark Master IP address', required=False)

    args = parser.parse_args().__dict__

else:
    args = {
        'pipelinekey': 'CA_MIGRATE_NFS_NA',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs_schema',
        'source_path': r'C:\myworkdir\Shared\NFS-CA',
        }



# %% Import Libraries

from collections import defaultdict
from email.header import Header
import os, sys, tempfile, shutil, json, csv, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, column_regex, get_csv_rows

from datetime import datetime
from distutils.dir_util import remove_tree



# %% Parameters

headerrecordclientid_map = {
    'MAJ': 'RAA',
}

table_name_map = {
    'name_addr_history': 'name_and_address',
}

master_schema_name = 'name_and_address' # to take header info
master_schema_header_columns = ['firm_name', 'headerrecordclientid']

schema_to_file_records = lambda schema_name: schema_name + '_records.csv'
schema_to_file_record_names = lambda schema_name: schema_name + '_record_names.csv'

file_has_header = True
file_has_trailer = True

HEADER_record = 'H'
DATA_record = 'D'
TRAILER_record = 'T'

data_settings.target_path = data_settings.app_data_path
if os.path.isdir(data_settings.target_path): remove_tree(directory=data_settings.target_path, verbose=0, dry_run=0)
os.makedirs(data_settings.target_path, exist_ok=True)

json_file_ext = '.json'





# %% get and pre-process schema

@catch_error(logger)
def get_nfs_schema(schema_name:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_name = schema_to_file_records(schema_name=schema_name)
    schema_file_path = os.path.join(data_settings.schema_file_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return (None, ) * 4

    schema = defaultdict(dict)
    header_schema = defaultdict(dict)
    trailer_schema = defaultdict(dict)
    for row in get_csv_rows(csv_file_path=schema_file_path):
        record_type = row['record_type'].upper().strip()
        record_number = str(row['record_number']).upper().strip()
        record_segment = str(row['record_segment']).upper().strip()
        field_name = re.sub(column_regex, '_', row['field_name'].lower().strip())
        position = row['position'].strip()
        conditional_changes = row['conditional_changes'].upper().strip()

        if not field_name or (field_name in ['', 'not_used', 'filler', '_', '__', 'n_a', 'na', 'none', 'null', 'value']) \
            or ('-' not in position) or not record_type: continue

        pos = position.split('-')
        position_start = int(pos[0])-1
        position_end = int(pos[1])
        if position_start > position_end:
            raise ValueError(f'position_start {pos[0]} > position_end {pos[1]} for field_name {field_name} in record_type {record_type} record_number {record_number} record_segment {record_segment}')

        if record_type.upper() == DATA_record.upper():
            if (field_name, conditional_changes) in schema[(record_number, record_segment)]:
                raise ValueError(f'Duplicate field_name : {(record_number, record_segment, field_name, conditional_changes)}')
            schema[(record_number, record_segment)][(field_name, conditional_changes)] = {
                'position_start': position_start,
                'position_end': position_end,
                'conditional_changes': conditional_changes,
            }
        elif record_type.upper() == HEADER_record.upper():
            if field_name in header_schema:
                raise ValueError(f'Duplicate field_name in header_schema: {field_name}')
            header_schema[field_name] = {
                'position_start': position_start,
                'position_end': position_end,
            }
        elif record_type.upper() == TRAILER_record.upper():
            if field_name in trailer_schema:
                raise ValueError(f'Duplicate field_name in trailer_schema: {field_name}')
            trailer_schema[field_name] = {
                'position_start': position_start,
                'position_end': position_end,
            }
        else:
            raise ValueError(f'Unknown record_type: {record_type}')

    record_file_name = schema_to_file_record_names(schema_name=schema_name)
    record_file_path = os.path.join(data_settings.schema_file_path, record_file_name)
    if not os.path.isfile(record_file_path):
        logger.warning(f'Schema file is not found: {record_file_path}')
        return (None, ) * 4

    record_names = defaultdict(str)
    for row in get_csv_rows(csv_file_path=record_file_path):
        record_number = str(row['record_number']).upper().strip()
        record_name = row['record_name'].upper().strip()
        record_names[record_number] = record_name

    return schema, header_schema, trailer_schema, record_names



schema, header_schema, trailer_schema, record_names = get_nfs_schema(schema_name=master_schema_name)



# %% Get Header Info from Pershing file

@catch_error(logger)
def get_header_info(file_path:str):
    """
    Get Header Info from Pershing file
    """
    with open(file=file_path, mode='rt') as f:
        HEADER = f.readline()

    header_info = dict()
    for field_name, pos in header_schema.items():
        header_info[field_name] = re.sub(' +', ' ', HEADER[pos['position_start']: pos['position_end']].strip())

    table_name = re.sub(column_regex, '_', header_info['filetitle'].lower())
    if table_name not in table_name_map:
        logger.info(f'Unknown Table Name "{table_name}" in file {file_path}')
        return

    firm_name = headerrecordclientid_map[header_info['headerrecordclientid'].upper()]

    header_info['table_name'] = '_'.join([firm_name, table_name_map[table_name]]).lower()
    header_info['firm_name'] = firm_name
    header_info['key_datetime'] = datetime.strptime(header_info['transmissioncreationdate'], r'%m%d%y')
    header_info['target_file_name'] = header_info['table_name'] + '_' + header_info['key_datetime'].strftime(r'%Y%m%d') + '.json'
    return header_info



header_info = get_header_info(file_path=r'C:\myworkdir\Shared\NFS-CA\MAJ_NABASE.DAT')



# %% Determine Start Line

@catch_error(logger)
def is_start_line(line:str):
    """
    Determine Start Line
    """
    record_segment = line[0:1]
    record_number = line[14:17]
    return record_segment == '1' and record_number == '101'



# %%







# %% Process all lines belonging to a single record

@catch_error(logger)
def process_lines(ftarget, lines:list):
    """
    Process all lines belonging to a single record
    """
    if len(lines) == 0: return

    fba = ()
    for line in lines:
        record_segment = line[0:1]
        if ord(record_segment)-ord('0') not in range(1, 5): raise ValueError(f'Invalid record_segment: {record_segment}')

        if record_segment == '1':
            firm = line[1:5]
            branch = line[5:8]
            account_number = line[8:14]
            record_number = line[14:17]
            if record_number == '101':
                fba = (firm, branch, account_number)
            else:
                if fba != (firm, branch, account_number):
                    raise ValueError(f'Values {(firm, branch, account_number)} in record {record_number} does not Match record 101 data {fba}')

        if record_number == '900': continue # record_number 900 is empty - ignore







# %% Add Header or Trailer line

@catch_error(logger)
def process_custom_line(ftarget, line:str, custom:str):
    """
    Add Header or Trailer line
    """
    #ftarget.write(add_prefix(prefix=txt.ljust(total_hash_length), line=line))



# %% Process single FWF

@catch_error(logger)
def process_single_fwf(source_file_path:str, target_file_path:str):
    """
    Process single FWF
    """
    with open(source_file_path, mode='rt', encoding='ISO-8859-1') as fsource:
        with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
            first = file_has_header
            lines = []
            for line in fsource:
                if first:
                    process_custom_line(ftarget=ftarget, line=line, custom=HEADER_record)
                    first = False
                else:
                    if is_start_line(line=line) and lines:
                        process_lines(ftarget=ftarget, lines=lines)
                        lines = []
                    lines.append(line)

            if file_has_trailer:
                if len(lines)>1: process_lines(ftarget=ftarget, lines=lines[:-1])
                process_custom_line(ftarget=ftarget, line=lines[-1], custom=TRAILER_record)
            else:
                process_lines(ftarget=ftarget, lines=lines)



# %% Main function to iterate over all the files in source_path and add bulk_id

@catch_error(logger)
def iterate_over_all_fwf(source_path:str):
    """
    Main function to iterate over all the files in source_path and add bulk_id
    """
    for root, dirs, files in os.walk(source_path):
        for file_name in files:
            source_file_path = os.path.join(root, file_name)
            file_name_noext, file_ext = os.path.splitext(file_name)
            if file_ext.lower() == '.zip':
                with tempfile.TemporaryDirectory(dir=data_settings.temporary_file_path) as tmpdir:
                    extract_dir = tmpdir
                    logger.info(f'Extracting {source_file_path} to {extract_dir}')
                    shutil.unpack_archive(filename=source_file_path, extract_dir=extract_dir, format='zip')
                    iterate_over_all_fwf(source_path=extract_dir)
                continue

            target_file_path = os.path.join(data_settings.target_path, file_name + json_file_ext)
            logger.info(f'Processing {source_file_path}')
            process_single_fwf(source_file_path=source_file_path, target_file_path=target_file_path)



iterate_over_all_fwf(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%



description = """

Convert NFS NA fixed-width files to json format

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
        'pipelinekey': 'CA_MIGRATE_NFS_RAA',
        'schema_file_path': r'C:\myworkdir\EDIP-Code\config\nfs_schema',
        'source_path': r'C:\myworkdir\Shared\NFS-CA',
        }



# %% Import Libraries

import os, sys, tempfile, shutil, json, re

class app: pass
sys.app = app
sys.app.args = args
sys.app.parent_name = os.path.basename(__file__)

from modules3.common_functions import catch_error, data_settings, logger, mark_execution_end, column_regex, get_csv_rows
from modules3.migrate_files import file_meta_exists_for_select_files
from modules3.nfs_header import get_header_info

from collections import defaultdict
from distutils.dir_util import remove_tree



# %% Parameters

get_schema_name = lambda table_name_no_firm: table_name_no_firm + '.csv'

file_has_header = True
file_has_trailer = True

HEADER_record = 'H'
DATA_record = 'D'
TRAILER_record = 'T'

data_settings.target_path = data_settings.app_data_path
if os.path.isdir(data_settings.target_path): remove_tree(directory=data_settings.target_path, verbose=0, dry_run=0)
os.makedirs(data_settings.target_path, exist_ok=True)



# %% get and pre-process schema

@catch_error(logger)
def get_nfs_schema(table_name_no_firm:str):
    """
    Read and Pre-process the schema table to make it code-friendly
    """
    schema_file_name = get_schema_name(table_name_no_firm=table_name_no_firm)
    schema_file_path = os.path.join(data_settings.schema_file_path, schema_file_name)
    if not os.path.isfile(schema_file_path):
        logger.warning(f'Schema file is not found: {schema_file_path}')
        return (None, ) * 3

    schema = defaultdict(dict)
    header_schema = defaultdict(dict)
    trailer_schema = defaultdict(dict)
    for row in get_csv_rows(csv_file_path=schema_file_path):
        record_type = row['record_type'].upper().strip()
        record_number = str(row['record_number']).upper().strip()
        record_segment = str(row['record_segment']).upper().strip()
        field_name = re.sub(column_regex, '_', row['field_name'].lower().strip())
        conditional_changes = row['conditional_changes'].upper().strip()

        if not field_name or (field_name in ['', 'not_used', 'filler', '_', '__', 'n_a', 'na', 'none', 'null', 'value']) \
            or not record_type: continue

        scale = str(row['scale']).strip()
        scale = int(scale) if scale.isdigit() else -1

        position_start = int(row['position_start'].strip()) - 1
        position_end = int(row['position_end'].strip())
        if position_start > position_end:
            raise ValueError(f'position_start {position_start} > position_end {position_end} for field_name {field_name} in record_type {record_type} record_number {record_number} record_segment {record_segment}')

        if record_type.upper() == DATA_record.upper():
            if (field_name, conditional_changes) in schema[(record_number, record_segment)]:
                raise ValueError(f'Duplicate field_name : {(record_number, record_segment, field_name, conditional_changes)}')
            schema[(record_number, record_segment)][(field_name, conditional_changes)] = {
                'position_start': position_start,
                'position_end': position_end,
                'scale': scale,
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

    return schema, header_schema, trailer_schema



# %% Determine Start Line

@catch_error(logger)
def is_start_line(line:str, header_info:dict):
    """
    Determine Start Line
    """
    table_name_no_firm = header_info['table_name_no_firm']

    if table_name_no_firm == 'name_and_address':
        record_segment = line[0:1]
        record_number = line[14:17]
        return record_segment == '1' and record_number == '101'

    if table_name_no_firm in ['position', 'activity']:
        record_segment = line[0:1]
        return record_segment == '1'

    raise ValueError(f'Unknown table name {table_name_no_firm}')



# %% Create stripped version of the values in the json-like object

@catch_error(logger)
def recursive_strip_json(obj):
    """
    Create stripped version of the values in the json-like object
    """
    if isinstance(obj, int) or isinstance(obj, float): return obj

    if isinstance(obj, str): return re.sub(' +', ' ', obj.strip())

    if isinstance(obj, list): return [recursive_strip_json(x) for x in obj]

    if isinstance(obj, dict): return {k: recursive_strip_json(v) for k, v in obj.items()}

    raise TypeError(f'Unsupported type: {type(obj)} for {obj}')



# %% Convert to Standard Record Number

@catch_error(logger)
def to_standard_record_number_name_and_address(record_number:str):
    """
    Convert to Standard Record Number
    """
    if record_number[0] == '2': record_number = record_number[0] + 'X' + record_number[2]
    if record_number[0] == '9' and record_number != '900': record_number = '901'
    return record_number


# %% Add Field to Record

@catch_error(logger)
def add_field_to_record(record:dict, field_name:str, field_value):
    """
    Add Field to Record
    """
    if field_name in record:
        record[field_name] += field_value
    else:
        record[field_name] = field_value



# %% Extract Field Value

@catch_error(logger)
def extract_field_value(line:str, pos:dict, field_name:str):
    """
    Extract Field Value from line for given pos dict
    """
    field_value = line[pos['position_start']:pos['position_end']]
    field_value = re.sub(' +', ' ', field_value.strip())

    if field_value and 'scale' in pos and pos['scale']>=0:
        if not field_value.isdigit():
            raise ValueError(f'Schema Scale mismatch for field "{field_name}" field value "{field_value}". Field Value should be all digits!')

        if pos['scale'] == 0:
            field_value = int(field_value)
        else:
            x = len(field_value) - pos['scale']
            field_value = float(field_value[:x] + '.' + field_value[x:])

        field_value = str(field_value)

    return field_value



# %% Process all lines belonging to a single record for NFS NA file

@catch_error(logger)
def process_lines_name_and_address(ftarget, lines:list, header_info:dict, schema, is_first_line:bool):
    """
    Process all lines belonging to a single record for NFS NA file
    """
    if len(lines) == 0: return

    record = {
        'header_firm_name': header_info['firm_name'],
        'headerrecordclientid': header_info['headerrecordclientid'],
        'ffr': {},
        'ffr_names': [],
        'fbsi': {},
        'legal': {},
        'booksrecords': [],
        'ipcs': [],
    }

    fba = ()
    ipcs_ids = []
    account_ids = []
    for line in lines:
        record_segment = line[0:1]
        if record_segment not in ['1', '2', '3', '4', '5']: raise ValueError(f'Invalid record_segment: {record_segment}')

        if record_segment == '1':
            firm = line[1:5].strip()
            branch = line[5:8].strip()
            account_number = line[8:14].strip()
            record_number = line[14:17].strip()
            standard_record_number = to_standard_record_number_name_and_address(record_number=record_number)
            if record_number == '101':
                fba = (firm, branch, account_number)
                record['firm'] = firm
                record['branch'] = branch
                record['accountnumber'] = account_number
            else:
                if fba != (firm, branch, account_number):
                    raise ValueError(f'Values {(firm, branch, account_number)} in record {record_number} does not Match record 101 data {fba}')

        if standard_record_number == '900': continue # record_number 900 is empty - ignore

        line_schema = schema[(standard_record_number, record_segment)]
        line_fields = dict()
        for field, pos in line_schema.items():
            field_name = field[0]
            conditional_changes = field[1]
            if conditional_changes:
                name_type = line[1:2]
                if name_type != conditional_changes: continue
            field_value = extract_field_value(line=line, pos=pos, field_name=field_name)
            line_fields = {**line_fields, field_name:field_value}

        ffr_count_flag = True
        for field_name, field_value in line_fields.items():
            if field_name in ['recordsegment', 'recordnumber', 'firm', 'branch', 'accountnumber']: continue

            if standard_record_number in ['101']:
                add_field_to_record(record=record, field_name=field_name, field_value=field_value)

            elif standard_record_number in ['104', '115']:
                name_map = {
                    '104': 'fbsi',
                    '115': 'legal',
                }
                record_name = name_map[standard_record_number]
                add_field_to_record(record=record[record_name], field_name=field_name, field_value=field_value)

            elif standard_record_number in ['102', '103', '113']:
                if record_segment in ['1', '2']:
                    if field_name == 'ffrnamecount':
                        if field_value:
                            field_value = int(field_value)
                        else:
                            field_value = 0
                        ffrnamecount = field_value
                    add_field_to_record(record=record['ffr'], field_name=field_name, field_value=field_value)
                else:
                    rs = int(record_segment) - 2
                    if ffrnamecount < rs: continue

                    if ffr_count_flag:
                        ffr_count_flag = False
                        record['ffr_names'].append({})

                    add_field_to_record(record=record['ffr_names'][-1], field_name=field_name, field_value=field_value)

            elif standard_record_number in ['2X0', '2X1', '2X2', '2X3']:
                account_id = int(record_number[1])
                if account_id not in account_ids:
                    account_ids.append(account_id)
                    record['booksrecords'].append({})
                account_ix = account_ids.index(account_id)
                add_field_to_record(record=record['booksrecords'][account_ix], field_name=field_name, field_value=field_value)

            elif standard_record_number in ['901']:
                ipcs_id = int(record_number[1:])
                if ipcs_id not in ipcs_ids:
                    ipcs_ids.append(ipcs_id)
                    record['ipcs'].append({})
                ipcs_ix = ipcs_ids.index(ipcs_id)
                add_field_to_record(record=record['ipcs'][ipcs_ix], field_name=field_name, field_value=field_value)

    if not is_first_line:
        ftarget.write(',\n')

    ftarget.write(json.dumps(recursive_strip_json(record)))



# %% Process all lines belonging to a single record for NFS Position file

@catch_error(logger)
def process_lines_position(ftarget, lines:list, header_info:dict, schema, is_first_line:bool):
    """
    Process all lines belonging to a single record for NFS Position file
    """
    if len(lines) == 0: return

    record = {
        'header_firm_name': header_info['firm_name'],
        'headerrecordclientid': header_info['headerrecordclientid'],
    }

    standard_record_number = '1' # There is only one record_number for position file

    for line in lines:
        record_segment = line[0:1]
        if record_segment not in ['1', '2', '3', '4', '5', '6', '7']: raise ValueError(f'Invalid record_segment: {record_segment}')

        line_schema = schema[(standard_record_number, record_segment)]
        line_fields = dict()
        for field, pos in line_schema.items():
            field_name = field[0]
            conditional_changes = field[1]
            if conditional_changes:
                raise ValueError(f'Unexpected conditional_changes for position file {conditional_changes}')
            field_value = extract_field_value(line=line, pos=pos, field_name=field_name)
            line_fields = {**line_fields, field_name:field_value}

        for field_name, field_value in line_fields.items():
            if field_name.startswith('recordnumber') and len(field_name) == len('recordnumber') + 1: continue
            add_field_to_record(record=record, field_name=field_name, field_value=field_value)

    if not is_first_line:
        ftarget.write(',\n')

    ftarget.write(json.dumps(recursive_strip_json(record)))



# %% Process all lines belonging to a single record for NFS Activity file

@catch_error(logger)
def process_lines_activity(ftarget, lines:list, header_info:dict, schema, is_first_line:bool):
    """
    Process all lines belonging to a single record for NFS Position file
    """
    if len(lines) == 0: return

    record = {
        'header_firm_name': header_info['firm_name'],
        'headerrecordclientid': header_info['headerrecordclientid'],
    }

    standard_record_number = '1' # There is only one record_number for activity file

    for line in lines:
        record_segment = line[0:1]
        if record_segment not in ['1', '2', '3', '4']: raise ValueError(f'Invalid record_segment: {record_segment}')

        line_schema = schema[(standard_record_number, record_segment)]
        line_fields = dict()
        for field, pos in line_schema.items():
            field_name = field[0]
            conditional_changes = field[1]
            if conditional_changes:
                raise ValueError(f'Unexpected conditional_changes for activity file {conditional_changes}')
            field_value = extract_field_value(line=line, pos=pos, field_name=field_name)
            line_fields = {**line_fields, field_name:field_value}

        for field_name, field_value in line_fields.items():
            if field_name.startswith('recordnumber') and len(field_name) == len('recordnumber') + 1: continue
            add_field_to_record(record=record, field_name=field_name, field_value=field_value)

    if not is_first_line:
        ftarget.write(',\n')

    ftarget.write(json.dumps(recursive_strip_json(record)))



# %% Process all lines belonging to a single record

@catch_error(logger)
def process_lines(ftarget, lines:list, header_info:dict, schema, is_first_line:bool):
    """
    Process all lines belonging to a single record
    """
    process_lines_map = {
        'name_and_address': process_lines_name_and_address,
        'position': process_lines_position,
        'activity': process_lines_activity,
    }

    process_lines_map[header_info['table_name_no_firm']](ftarget=ftarget, lines=lines, header_info=header_info, schema=schema, is_first_line=is_first_line)



# %% Process Header or Trailer line

@catch_error(logger)
def process_custom_line(ftarget, line:str, header_info:dict, custom:str):
    """
    Process Header or Trailer line
    """
    pass



# %% Process single FWF

@catch_error(logger)
def process_single_fwf(source_file_path:str):
    """
    Process single FWF
    """
    logger.info(f'Processing {source_file_path}')

    header_info = get_header_info(file_path=source_file_path)
    schema, header_schema, trailer_schema = get_nfs_schema(table_name_no_firm=header_info['table_name_no_firm'])

    if data_settings.key_datetime > header_info['key_datetime']:
        logger.info(f'Skipping file due to older key_datetime: {source_file_path}')
        return

    target_file_path = os.path.join(data_settings.target_path, header_info['target_file_name'])
    if file_meta_exists_for_select_files(file_path=target_file_path):
        logger.info(f'Already ingested file: {source_file_path} -> skipping')
        return

    with open(source_file_path, mode='rt', encoding='ISO-8859-1') as fsource:
        with open(target_file_path, mode='wt', encoding='utf-8') as ftarget:
            ftarget.write('[\n')

            first = file_has_header
            lines = []
            is_first_line = True
            for line in fsource:
                if first:
                    process_custom_line(ftarget=ftarget, line=line, header_info=header_info, custom=HEADER_record)
                    first = False
                else:
                    if is_start_line(line=line, header_info=header_info) and lines:
                        process_lines(ftarget=ftarget, lines=lines, header_info=header_info, schema=schema, is_first_line=is_first_line)
                        is_first_line = False
                        lines = []
                    lines.append(line)

            if file_has_trailer:
                if len(lines)>1: process_lines(ftarget=ftarget, lines=lines[:-1], header_info=header_info, schema=schema, is_first_line=is_first_line)
                process_custom_line(ftarget=ftarget, line=lines[-1], header_info=header_info, custom=TRAILER_record)
            else:
                process_lines(ftarget=ftarget, lines=lines, header_info=header_info, schema=schema, is_first_line=is_first_line)

            ftarget.write('\n]')



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

            process_single_fwf(source_file_path=source_file_path)



iterate_over_all_fwf(source_path=data_settings.source_path)



# %% Close Connections / End Program

mark_execution_end()


# %%



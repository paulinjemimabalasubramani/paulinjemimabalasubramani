#!/usr/bin/python3
import sys
import os
from modules3.common_functions import data_settings

def write_to_file(record, source_file, Number_of_entity_per_file, encoding, final=False):
    des_fh = gvar['des_fh']
    if gvar['rec_count'] % Number_of_entity_per_file == 0 and not final:
        if des_fh:
            des_fh.write(gvar['trailer'])  # Write the trailer row
            des_fh.close()
        file_num = gvar['rec_count'] // Number_of_entity_per_file
        base_dir = os.path.dirname(source_file)
        split_dir = data_settings.get_value(attr_name=f'split_dir', default_value=os.path.join(base_dir, 'split_files'))
        print(f'split_dir : {split_dir}')
        if not os.path.exists(split_dir):
            os.makedirs(split_dir)
        base_name, ext = os.path.splitext(os.path.basename(source_file))
        des_fh = open(os.path.join(split_dir, f"{base_name}_{file_num:02d}{ext}"), 'w', encoding='cp1252', errors='replace')
        des_fh.write(gvar['dod'])  # Write the replaced first line
        gvar['des_fh'] = des_fh
    gvar['rec_count'] += 1
    des_fh.write(record)

def process_source_file(source_file, Number_of_entity_per_file, identifier_start_position, identifier_end_position, linesize, encoding='cp1252'):
    with open(source_file, 'r', encoding=encoding, errors='replace') as src_fh:
        lines = src_fh.readlines()
        gvar['dod'] = lines[0]  # Read the first line separately
        gvar['trailer'] = lines[-1]  # Capture the trailer row

        buffer = ''
        prev_record = ''
        no_of_lines = 0

        for chunk in lines[1:-1]:  # Process all lines except the first and last
            split_by = chunk[identifier_start_position:identifier_end_position]
            if split_by != prev_record:
                write_to_file(buffer, source_file, Number_of_entity_per_file, encoding)
                buffer = chunk
            else:
                buffer += chunk

            prev_record = split_by
            no_of_lines += 1

        write_to_file(buffer, source_file, Number_of_entity_per_file, encoding, final=True)
        return prev_record

def split_files(source_file, Number_of_entity_per_file, identifier_start_position, identifier_end_position, linesize, encoding='cp1252'):
    global gvar
    gvar = {
        'rec_count': 0,    # For Number of Account Records Count.
        'des_fh': None,    # For Output File handle
        'dod': None,       # For Date of Data (first line)
        'trailer': None    # For Trailer Row
    }

    if not os.path.exists('split_files'):
        os.makedirs('split_files')

    total_record = process_source_file(source_file, Number_of_entity_per_file, identifier_start_position, identifier_end_position, linesize, encoding)
    if gvar['des_fh']:
        gvar['des_fh'].write(gvar['trailer'])  # Write the trailer row
        gvar['des_fh'].close()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python multiline_file_handle.py <source_file> <Number_of_entity_per_file> <identifier_start_position> <identifier_end_position> <linesize>")
    else:
        source_file = sys.argv[1]
        Number_of_entity_per_file = int(sys.argv[2])
        identifier_start_position = int(sys.argv[3])
        identifier_end_position = int(sys.argv[4])
        linesize = int(sys.argv[5])

        split_files(source_file, Number_of_entity_per_file, identifier_start_position, identifier_end_position, linesize)
#!/usr/bin/python3
import sys

def write_to_file(record, Number_of_entity_per_file, trailer_format, encoding, final=False):
    des_fh = gvar['des_fh']    
    if gvar['rec_count'] % Number_of_entity_per_file == 0 and not final:
        if des_fh:
            des_fh.write(trailer_format.format(gvar['dod'], Number_of_entity_per_file))
            des_fh.close()
        file_num = gvar['rec_count'] // Number_of_entity_per_file
        des_fh = open(f"{source_file}_{file_num:02d}", 'w', encoding=encoding)
        des_fh.write(gvar['dod'])  # Write the replaced first line
        gvar['des_fh'] = des_fh
    gvar['rec_count'] += 1
    des_fh.write(record)

def process_source_file(source_file, Number_of_entity_per_file, trailer_format, identifier_start_position, identifier_end_position, linesize, trailer, encoding='cp1252'):
    with open(source_file, 'r', encoding=encoding, errors='replace') as src_fh:
        gvar['dod'] = src_fh.readline()  # Read the first line separately

        buffer = ''
        prev_record = ''
        no_of_lines = 0

        for chunk in iter(lambda: src_fh.read(linesize), ''):
            if chunk.startswith(trailer):
                break

            split_by = chunk[identifier_start_position:identifier_end_position]
            if split_by != prev_record:
                write_to_file(buffer, Number_of_entity_per_file, trailer_format, encoding)
                buffer = chunk
            else:
                buffer += chunk

            prev_record = split_by
            no_of_lines += 1

        write_to_file(buffer, Number_of_entity_per_file, trailer_format, encoding, final=True)
        return prev_record

def split_files(source_file, Number_of_entity_per_file, trailer, trailer_format, identifier_start_position, identifier_end_position, linesize, encoding='cp1252'):
    global gvar
    gvar = {
        'rec_count': 0,    # For Number of Account Records Count.
        'des_fh': None,    # For Output File handle
        'dod': None,       # For Date of Data (first line)
        'trailer': trailer # For Trailer Row
    }

    total_record = process_source_file(source_file, Number_of_entity_per_file, trailer_format, identifier_start_position, identifier_end_position, linesize, trailer, encoding)
    if gvar['des_fh']:
        gvar['des_fh'].write(trailer_format.format(gvar['dod'], gvar['rec_count'] % Number_of_entity_per_file))
        gvar['des_fh'].close()

if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Usage: python multiline_file_handle.py <source_file> <Number_of_entity_per_file> <trailer> <trailer_format> <identifier_start_position> <identifier_end_position> <linesize>")
    else:
        source_file = sys.argv[1]
        Number_of_entity_per_file = int(sys.argv[2])
        trailer = sys.argv[3]
        trailer_format = sys.argv[4]
        identifier_start_position = int(sys.argv[5])
        identifier_end_position = int(sys.argv[6])
        linesize = int(sys.argv[7])

        split_files(source_file, Number_of_entity_per_file, trailer, trailer_format, identifier_start_position, identifier_end_position, linesize)
        
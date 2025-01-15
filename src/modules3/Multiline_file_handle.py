#!/usr/bin/python3
import sys

def write_to_file(record, Number_of_entity_per_file, trailer_format,encoding):
    des_fh = gvar['des_fh']
    if gvar['rec_count'] % Number_of_entity_per_file == 0:
        if des_fh:
            des_fh.write(trailer_format.format(gvar['dod'], Number_of_entity_per_file))
            des_fh.close()
        file_num = gvar['rec_count'] // Number_of_entity_per_file
        des_fh = open(f"{source_file}_{file_num:02d}", 'w', encoding=encoding)
        des_fh.write(gvar['header'])
        gvar['des_fh'] = des_fh
    gvar['rec_count'] += 1
    des_fh.write(record)

def process_source_file(source_file, Number_of_entity_per_file, trailer_format, rectype_start, rectype_end, seq_start, seq_end, record_start, record_end, linesize, dod_start, dod_end, trailer,encoding='cp1252'):
    with open(source_file, 'r', encoding=encoding, errors='replace') as src_fh:
        gvar['header'] = src_fh.read(linesize) # size of each line with end of a line character 
        gvar['dod'] = gvar['header'][dod_start:dod_end]

        buffer = ''
        prev_record = 1
        no_of_lines = 0

        for chunk in iter(lambda: src_fh.read(linesize), ''):
            if chunk.startswith(trailer):
                break

            c1, c2, c3 = chunk[rectype_start:rectype_end], int(chunk[seq_start:seq_end]), chunk[record_start:record_end]
            if c2 != prev_record:
                write_to_file(buffer, Number_of_entity_per_file, trailer_format,encoding)
                buffer = c1+'{:08d}'.format(c2%Number_of_entity_per_file if c2%Number_of_entity_per_file else Number_of_entity_per_file)+c3
            else:
                buffer += c1+'{:08d}'.format(c2%Number_of_entity_per_file if c2%Number_of_entity_per_file else Number_of_entity_per_file)+c3
            prev_record = c2
            no_of_lines += 1

        write_to_file(buffer, Number_of_entity_per_file, trailer_format,encoding)
        print(f"Processed {no_of_lines} lines.")
        return prev_record

def split_files(source_file, Number_of_entity_per_file, trailer, trailer_format, rectype_start, rectype_end, seq_start, seq_end, record_start, record_end, linesize, dod_start, dod_end,encoding='cp1252'):
    global gvar
    gvar = {
        'rec_count': 0,    # For Number of Account Records Count.
        'des_fh'   : None, # For Output File handle
        'header'   : '',   # For Header Row
        'dod'      : None, # For DataofDate
        'trailer'  : trailer  # For Trailer Row
    }

    total_record = process_source_file(source_file, Number_of_entity_per_file, trailer_format, rectype_start, rectype_end, seq_start, seq_end, record_start, record_end, linesize, dod_start, dod_end, trailer)
    gvar['des_fh'].write(trailer_format.format(gvar['dod'],
        Number_of_entity_per_file if total_record % Number_of_entity_per_file == 0
        else total_record % Number_of_entity_per_file))
    gvar['des_fh'].close()

if __name__ == "__main__":
    if len(sys.argv) != 14:
        print("Usage: python multiline_file_handle.py <source_file> <Number_of_entity_per_file> <trailer> <trailer_format> <rectype_start> <rectype_end> <seq_start> <seq_end> <record_start> <record_end> <linesize> <dod_start> <dod_end>")
    else:
    
        source_file = sys.argv[1]
        Number_of_entity_per_file = int(sys.argv[2])
        trailer = sys.argv[3]
        trailer_format = sys.argv[4]
        rectype_start = int(sys.argv[5])
        rectype_end = int(sys.argv[6])
        seq_start = int(sys.argv[7])
        seq_end = int(sys.argv[8])
        record_start = int(sys.argv[9])
        record_end =None if sys.argv[10] == "None" else int(sys.argv[10])
        linesize = int(sys.argv[11])
        dod_start = int(sys.argv[12])
        dod_end = int(sys.argv[13])
        
    split_files(source_file, Number_of_entity_per_file, trailer, trailer_format, rectype_start, rectype_end, seq_start, seq_end, record_start, record_end, linesize, dod_start, dod_end)
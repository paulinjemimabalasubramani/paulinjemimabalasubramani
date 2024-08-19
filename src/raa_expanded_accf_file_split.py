#!/usr/bin/python3
import sys
 
RecordsPerFile = 30000
 
def write_to_file(record):
    des_fh = gvar['des_fh']
    if gvar['rec_count'] % RecordsPerFile == 0:
        if des_fh:
            des_fh.write(gvar['trailer'].format(gvar['dod'], RecordsPerFile))
            des_fh.close()
        file_num = gvar['rec_count'] // RecordsPerFile
        des_fh = open(f"2{source_file}_{file_num:02d}", 'w', encoding='utf-8')
        des_fh.write(gvar['header'])
        gvar['des_fh'] = des_fh
    gvar['rec_count'] += 1
    des_fh.write(record)
 ##--
def process_source_file():
    linesize = 751
    with open(source_file, 'r', encoding='utf-8') as src_fh:
        gvar['header'] = src_fh.read(linesize) # size of each line with end of a line character 
        gvar['dod'] = gvar['header'][46:56]
        buffer = ''
        prev_record = 1
        no_of_lines = 0
 
        for chunk in iter(lambda: src_fh.read(linesize), ''):
            if chunk.startswith('EOF'):
                break
 
            c1, c2, c3 = chunk[:3], int(chunk[3:11]), chunk[11:]
            if c2 != prev_record:
                write_to_file(buffer)
                buffer = c1+'{:08d}'.format(c2%RecordsPerFile if c2%RecordsPerFile else RecordsPerFile)+c3
            else:
                buffer += c1+'{:08d}'.format(c2%RecordsPerFile if c2%RecordsPerFile else RecordsPerFile)+c3
            prev_record = c2
            no_of_lines += 1
 
        write_to_file(buffer)
        print(f"Processed {no_of_lines} lines.")
        return prev_record
 
def split_accf_files(s_file):
    global source_file
    global gvar
    source_file=s_file
    gvar = {
        'rec_count': 0,    # For Number of Account Records Count.
        'des_fh'   : None, # For Output File handle
        'header'   : '',   # For Header Row
        'dod'      : None, # For DataofDate
        'trailer'  : ("EOF      PERSHING CUSTOMER ACCT INFO DATA OF  {0} TO REMOTE FYY"
                     "  ENDS HERE. TOTAL DETAIL RECORDS: {1:010d}   UPDATED"
                     + " " * 624 + "Z")  # For Trailer Row
                     }
 
    total_record = process_source_file()
    gvar['des_fh'].write(gvar['trailer'].format(gvar['dod'],
        RecordsPerFile if total_record % RecordsPerFile == 0
        else total_record % RecordsPerFile))
    gvar['des_fh'].close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python raa_expanded.py <source_file>")
    else:
        source_file = sys.argv[1]
        split_accf_files(source_file)
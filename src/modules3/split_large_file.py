import os
import sys
import time
import ijson
from datetime import datetime
from xml.sax import parse
from xml.sax.saxutils import XMLGenerator
from modules3.common_functions import catch_error, logger

###########################################################################################################################################
############################################### XML CHUNK ###################################################################################
###########################################################################################################################################

@catch_error(logger)
class CycleFile:
    def __init__(self, filename, max_size):
        self.basename, self.ext = os.path.splitext(filename)
        self.index = 0
        self.max_size = max_size
        self.records_written = 0  # To track records written per file
        self.open_next_file()

    def open_next_file(self):
        if self.index > 0:
            logger.info(f"File created: {self.name()} with {self.records_written} records.")
        self.index += 1
        self.file = open(self.name(), 'w')
        self.records_written = 0  # Reset the record count for the new file

    def name(self):
        return f"{self.basename}{self.index}{self.ext}"

    def cycle(self):
        self.file.close()
        self.open_next_file()

    def write(self, str):
        self.file.write(str.decode("utf-8") if isinstance(str, bytes) else str)

    def close(self):
        logger.info(f"File created: {self.name()} with {self.records_written} records.")
        self.file.close()


@catch_error(logger)
class XMLBreaker(XMLGenerator):
    def __init__(self, first_child_element=None, max_number_records_each_split=1000, max_size=1024 * 1024, out=None, *args, **kwargs):
        super().__init__(out, encoding='utf-8', *args, **kwargs)
        self.out_file = out
        self.first_child_element = first_child_element
        self.max_number_records_each_split = max_number_records_each_split
        self.max_size = max_size
        self.context = []
        self.count = 0

    def startElement(self, name, attrs):
        super().startElement(name, attrs)
        self.context.append((name, attrs))

    def endElement(self, name):
        super().endElement(name)
        self.context.pop()
        if name == self.first_child_element:
            self.count += 1
            self.out_file.records_written += 1  # Increment record count for this file
            self.out_file.file.flush()  # Ensure the size is updated
            current_size = os.path.getsize(self.out_file.file.name)

            # Split based on record count or file size
            if self.count == self.max_number_records_each_split or current_size >= self.max_size:
                self.count = 0
                for element in reversed(self.context):
                    self.out_file.write("\n")
                    super().endElement(element[0])
                self.out_file.cycle()
                super().startDocument()
                for element in self.context:
                    super().startElement(*element)


@catch_error(logger)
def split_xml_file(input_file, split_max_size_in_mb, first_child_element, max_number_records_each_split, output_file):
    max_size = split_max_size_in_mb * 1024 * 1024
    if not os.path.exists(input_file):
        logger.error(f"XML file not found: {input_file}. Skipping...")
        return
    logger.info(f"Start processing XML file: {input_file}")
    start_time = time.time()
    file_size = os.path.getsize(input_file)
    if file_size < max_size:
        logger.info(f"File {input_file} is smaller than the max size and has been written as is.")
    else:
        logger.info(f"File {input_file} is larger than the max size and has been split.")
        parse(input_file, XMLBreaker(first_child_element, max_number_records_each_split, max_size, out=CycleFile(output_file, max_size)))
    end_time = time.time()
    logger.info(f"Finished processing XML file: {input_file}")
    logger.info(f"Processing time for split_xml_file: {end_time - start_time:.2f} seconds\n")
    
#############################################################################################################################################
#################################################### JSON CHUNK ###############################################################################
#############################################################################################################################################

@catch_error(logger)
def split_json_file_iteratively(input_file, split_max_size_in_mb):
    max_size = split_max_size_in_mb * 1024 * 1024
    if not os.path.exists(input_file):
        logger.error(f"JSON file not found: {input_file}. Skipping...")
        return
    logger.info(f"Start processing JSON file: {input_file}")
    start_time = time.time()
    file_size = os.path.getsize(input_file)
    if file_size < max_size:
        logger.info(f"File {input_file} is smaller than the max size and has been written as is.")
    else:
        logger.info(f"File {input_file} is larger than the max size and has been split.")
        part_number = 1
        current_size = 0
        with open(input_file, 'r') as f:
            parser = ijson.items(f, 'item')
            output_file = f"{os.path.splitext(input_file)[0]}_part{part_number}.json"
            outfile = open(output_file, 'wb')
            outfile.write(b'[')
            for item in parser:
                item_bytes = orjson.dumps(item)
                item_size = len(item_bytes)
                if current_size + item_size + 2 > max_size:  # +2 for the comma and brackets
                    outfile.write(b']')
                    outfile.close()
                    part_number += 1
                    current_size = 0
                    output_file = f"{os.path.splitext(input_file)[0]}_part{part_number}.json"
                    outfile = open(output_file, 'wb')
                    outfile.write(b'[')
                if current_size > 0:
                    outfile.write(b',')
                outfile.write(item_bytes)
                current_size += item_size + 1  # +1 for the comma
            outfile.write(b']')
            outfile.close()
    end_time = time.time()
    logger.info(f"Finished processing JSON file: {input_file}")
    logger.info(f"Processing time for split_json_file_iteratively: {end_time - start_time:.2f} seconds\n")

####################################################################################################################################################
######################################################## CSV/DAT/TXT CHUNK ###########################################################################
####################################################################################################################################################

@catch_error(logger)
def split_file(file_path, filename, fileType, output_dir, max_size, max_lines, encodingformat='utf-8', header=None, trailerFormat=None, trailer_record_startwith=None):
    file_size = os.path.getsize(file_path)
    if file_size < max_size:
        logger.info(f"File {file_path} is smaller than the max size and has been written as is.")
        return
    logger.info(f"File {file_path} is larger than the max size and has been split.")
    root, file_extension = os.path.splitext(file_path)
    file_count = 0
    header_line = None
    trailer_line = None

    def write_chunk(lines, count):
        output_file = f"{filename.split('.')[0]}_{count:02d}{file_extension}" if file_extension else f"{filename.split('.')[0]}_{count:02d}"
        output_file_path = os.path.join(output_dir, output_file)
        with open(output_file_path, 'w', encoding=encodingformat, errors='replace') as f:
            f.writelines(lines)

    def include_header_trailer_and_write(lines, header_line, trailerFormat, record_count, file_count):
        if header_line:
            lines.insert(0, header_line + '\n')
        if trailerFormat:
            lines.append(trailerFormat.format(record_count) + '\n')
        write_chunk(lines, file_count)

    with open(file_path, 'r', encoding=encodingformat, errors='replace') as file:
        lines = file.readlines()
        if header and lines:
            header_line = lines.pop(0).strip()
        record_count = 0
        chunk_lines = []
        for line in lines:
            if trailerFormat and line.startswith(trailer_record_startwith):
                break
            record_count += 1
            chunk_lines.append(line)
            if record_count == max_lines:
                include_header_trailer_and_write(chunk_lines, header_line, trailerFormat, record_count, file_count)
                record_count = 0
                file_count += 1
                chunk_lines = []
        if chunk_lines:
            include_header_trailer_and_write(chunk_lines, header_line, trailerFormat, record_count, file_count)


@catch_error(logger)
def split_csv_dat_txt_file(source_dir, output_dir=None, split_max_size_in_mb=20, max_lines=10000, encodingFormat='utf-8', header=None, trailerFormat=None, trailer_record_startwith=None):
    max_size = split_max_size_in_mb * 1024 * 1024
    if not os.path.exists(source_dir):
        logger.error(f"Source directory not found: {source_dir}. Skipping...")
        return
    
    # Create 'split_files' directory under source path if not provided
    if output_dir is None:
        output_dir = os.path.join(source_dir, 'split_files')
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    logger.info(f"Start processing files in source directory: {source_dir}")
    start_time = time.time()
    
    for file in os.listdir(source_dir):
        file_path = os.path.join(source_dir, file)
        if os.path.isfile(file_path):
            split_file(file_path, file, os.path.splitext(file)[1], output_dir, max_size, max_lines, encodingFormat, header, trailerFormat, trailer_record_startwith)
    
    end_time = time.time()
    logger.info(f"Finished processing files in source directory: {source_dir}")
    logger.info(f"Processing time for split_csv_dat_txt_file: {end_time - start_time:.2f} seconds\n")


################################################################################################################################################
################################################# MAIN FUNCTION ################################################################################
################################################################################################################################################

if __name__ == "__main__":
    # XML splitting
    split_xml_file('C:\\Users\\pjemima\\Downloads\\Export_Xml_Report.xml', 250, 'ResultRecord', 10000, 'C:\\Users\\pjemima\\Downloads\\Test_xml\\Export_Xml_Report.xml')

    # JSON splitting
    split_json_file_iteratively('C:\\Users\\pjemima\\Downloads\\large_sample.json', 250)

    # CSV/DAT/TXT file splitting
    # csv: split_csv_dat_txt_file(source_dir, output_dir,max_lines=500,header='RepID')  
    # dat: split_csv_dat_txt_file(source_dir, output_dir,max_lines=1000,header='H',trailerFormat='T {0}',trailer_record_startwith='T')
    # txt: split_csv_dat_txt_file(source_dir, output_dir,max_lines=1000,header='*EOF*|',trailerFormat='*BOF*|{0}',trailer_record_startwith='*BOF*|')
    
    split_csv_dat_txt_file(source_dir, output_dir, max_lines=max_lines, header=header, trailerFormat=trailerFormat, trailer_record_startwith=trailer_record_startwith)
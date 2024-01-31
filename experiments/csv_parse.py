
import os


EXPECTED_NUMBER_OF_COMMAS = 75
folder_name = r'c:\tmp'
source_file = 'sponsor.csv'
good_file = 'goodfile.csv'
bad_file = 'badfile.csv'




with open(file=os.path.join(folder_name, source_file), mode='rt', encoding='UTF-8-SIG') as fsource:
    with open(file=os.path.join(folder_name, good_file), mode='wt', encoding='UTF-8-SIG', newline='') as fgood:
        with open(file=os.path.join(folder_name, bad_file), mode='wt', encoding='UTF-8-SIG', newline='') as fbad:
            for line in fsource:
                if line.count(',') == EXPECTED_NUMBER_OF_COMMAS:
                    fgood.write(line)
                else:
                    fbad.write(line)




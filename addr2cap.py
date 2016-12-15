# -*- coding: ISO-8859-1 -*-

import time
import sys
import os
import csv
import json
import logging
import urllib.request

log = True
language = "it"
logfile = "log.csv"
csv_value_separator = ";"
requests_delay = 0
infile = "input.csv"
search_string = "SEARCH_STRING"
take_a_breath_count = 30  # Avoid flood blocks posting tranches of n rows
take_a_breath_pause = 10  # Pause between tranches

# cmd_params = []
#
# if len(sys.argv) > 1:
#     cmd_params[0] = sys.argv[1]
#     cmd_params[1] = sys.argv[2:]
# else:
#     cmd_params[0] = infile
#     cmd_params[1] = search_string
#

cmd_params_count = len(sys.argv)

try: logging.basicConfig(filename=logfile,
                         format='%(asctime)s;%(levelname)s;%(message)s',
                         datefmt='%m/%d/%Y;%H:%M:%S',
                         level=logging.INFO)  # filename=logfile
except PermissionError:
    exit('Process aborted: the log file %s is locked by another application' % logfile)


requests_count = 0


def cap_search(search_string):
    logging.info('Searching address details for "%s"' % search_string)
    url = 'http://maps.googleapis.com/maps/api/geocode/json?language=%s&address=%s' % \
          (language, str.replace(search_string, " ", "+"))
    try:
        r = urllib.request.urlopen(url)
        global requests_count
        requests_count += 1
    except urllib.request.HTTPError as err:
        logging.debug("Error retriving data: %s" % err.code)
        return err.code

    data = json.loads(r.read().decode(r.info().get_param('charset') or 'ISO-8859-1'))

    if len(data['results']) > 0:

        street = number = postal_code = city = state = "NULL"

        for field in data['results'][0]['address_components']:

            if 'route' in field['types']:
                street = str(field['short_name']).upper()
            elif 'street_number' in field['types']:
                number = str(field['short_name']).upper()
            elif 'postal_code' in field['types']:
                postal_code = str(field['short_name']).upper()
            elif 'locality' in field['types']:
                city = str(field['short_name']).upper()
            elif 'administrative_area_level_2' in field['types']:
                state = str(field['short_name']).upper()

        if number == "NULL" and street != "NULL":
            number = "SNC"

        result = {"STREET": street, "NUMBER": number, "POSTAL_CODE": postal_code, "CITY": city, "STATE": state}

        logging.info("Found: %s " % result)

    else:
        street = number = postal_code = city = state = "NULL"
        result = {}

    return result


def check_header(header, file):
    file_in_chk = open(file, 'r', encoding="ISO-8859-1")
    file_reader = csv.DictReader(file_in_chk, delimiter=csv_value_separator)
    headers_list = file_reader.fieldnames
    if header not in headers_list:
        return False
    else:
        return True


if cmd_params_count < 2:
    pass
elif cmd_params_count == 2:
    print("Missing argouments:\nUsage: addr2cap <input_file> <search_column>")
    exit()
elif cmd_params_count == 3:
    infile = sys.argv[1]
    search_string = sys.argv[2]
elif cmd_params_count > 3:
    infile = sys.argv[1]
    search_string_list = []
    i = 2
    for string_part in cmd_params_count-2:
        if check_header(string_part, infile):
            search_string_list.append(string_part)
        else:
            logging.error("Fatal error: the column %s with search string was not found in the input file's headers" %
                          string_part)
            exit()
    search_string = ' '.join(search_string_list)


if not os.path.isfile(infile):
    logging.error("Fatal error: input file '%s' not found \n ---" % infile)
    exit()

f_in_chk = open(infile, 'r', encoding="ISO-8859-1")
reader = csv.DictReader(f_in_chk, delimiter=csv_value_separator)
input_headers = reader.fieldnames
logging.info("Checking input file. Headers founded: %s" % input_headers)
rows = len(list(reader))
logging.info("Found %s rows" % rows)

if search_string not in input_headers:
    logging.error("Fatal error: the column %s with search string was not found in the input file's headers" %
                  search_string)
    exit()
else:
    logging.info("Column %s correctly founded in the input file's headers" % search_string)

f_in = open(infile, 'r', encoding="ISO-8859-1")
outfile_fields = input_headers + ["STREET", "NUMBER", "POSTAL_CODE", "CITY", "STATE"]
outfile = "%s_processed_%s.csv" % (infile[:-4], time.strftime("%Y%m%d%H%M", time.gmtime()))
f_out = open(outfile, 'w')
writer = csv.DictWriter(f_out, outfile_fields, lineterminator='\n', delimiter=';', dialect='excel', restval='')
logfile = "activity_log.txt"

dict_input = csv.DictReader(f_in, delimiter=csv_value_separator)
dict_output = []
writer.writeheader()

logging.info("Process started...")

count_ok = 0
count_ko = 0


for row in dict_input:
    try:
        result = cap_search(row[search_string])

        if len(result) > 0:
            count_ok += 1
        else:
            count_ko += 1

        print("Processing %s of %s | Found: %s | NOT Found %s |" % (dict_input.line_num-1, rows, count_ok, count_ko))
        time.sleep(requests_delay)
    except (UnicodeEncodeError, UnicodeDecodeError, UnicodeDecodeError):
        logging.error('Error on string decoding. String skipped: %s' % row)
        result = {"STREET": "NULL", "NUMBER": "NULL", "POSTAL_CODE": "NULL", "CITY": "NULL", "STATE": "NULL"}

    row.update(result)
    dict_output.append(row)

writer.writerows(dict_output)

f_out.close()

logging.info("Process ended. \n ---")

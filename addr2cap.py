# -*- coding: ISO-8859-1 -*-

'''
This script perform a massive search in Google Maps Database.
The input is a string that contains a generic or incomplete address.
The output is an array with address components, as stored in Google DB.
Input and output is in standard csv file format, compatible with excel.
The input address may be splitted into multiple columns.
Due to flood protection until the "take a breath" system is not coded, take 200 requests a time.
'''

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
default_params = ["NULL", "input.csv", "Indirizzo Corrispondenza", "Città Corrispondenza"]
requests_delay = 0
take_a_breath_count = 30  # Avoid flood blocks grouping requests by n
take_a_breath_pause = 10  # Pause between groups
requests_count = 0

try:
    logging.basicConfig(format='%(asctime)s;%(levelname)s;%(message)s',
                        datefmt='%m/%d/%Y;%H:%M:%S',
                        level=logging.DEBUG)  # filename=logfile
except PermissionError:
    exit('Process aborted: the log file %s is locked by another application' % logfile)

'''
It's possible to use the script in CLI mode, using the following parameters schema:
addr2cap.py <input_file> <search_column_1> <search_column_2> <search_column_n>
'''
if len(sys.argv) < 3:
    logging.warning("Missing argouments. Usage: addr2cap <input_file> <search_column_1> <search_column_n>")
    params = default_params
    logging.warning("Using default parameters | Input file: %s | Search string list: %s" % (params[1], params[2:]))
else:
    params = sys.argv

infile = params[1]
search_string_headers_list = params[2:]

if not os.path.isfile(infile):
    logging.error("Fatal error: input file '%s' not found \n ---" % infile)
    exit()


def cap_search(address_string):
    logging.info('Searching address details for "%s"' % address_string)
    url = 'http://maps.googleapis.com/maps/api/geocode/json?language=%s&address=%s' % \
          (language, str.replace(address_string, " ", "+"))
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
        result = {}

    return result


def check_header_list(headers_list, file):
    file_in_chk = open(file, 'r', encoding="ISO-8859-1")
    file_reader = csv.DictReader(file_in_chk, delimiter=csv_value_separator)
    file_headers_list = file_reader.fieldnames
    logging.debug("Searching for %s in headers..." % headers_list)
    for header in headers_list:
        logging.debug("Checking %s..." % header)
        if header in file_headers_list:
            logging.debug("OK")
        else:
            return False
    return True


if check_header_list(search_string_headers_list, infile):
    logging.info("Column(s) with address string correctly founded in the input file's headers")
else:
    logging.error("Fatal error: the column with search string was not found in the input file's headers")
    exit()

f_in_chk = open(infile, 'r', encoding="ISO-8859-1")
reader = csv.DictReader(f_in_chk, delimiter=csv_value_separator)
input_headers = reader.fieldnames
logging.info("Checking input file. Headers founded: %s" % input_headers)
rows_count = len(list(reader))
logging.info("Found %s rows" % rows_count)


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

    search_string_list = []
    for part in search_string_headers_list:
        search_string_list.append(row[part])
    search_string = ' '.join(search_string_list)

    try:
        result = cap_search(search_string)

        if len(result) > 0:
            count_ok += 1
        else:
            count_ko += 1

        print("Processing %s of %s | Found: %s | NOT Found %s |" % (dict_input.line_num - 1, rows_count, count_ok, count_ko))
        time.sleep(requests_delay)
    except (UnicodeEncodeError, UnicodeDecodeError, UnicodeDecodeError):
        logging.error('Error on string decoding. String skipped: %s' % row)
        result = {"STREET": "NULL", "NUMBER": "NULL", "POSTAL_CODE": "NULL", "CITY": "NULL", "STATE": "NULL"}

    row.update(result)
    dict_output.append(row)

writer.writerows(dict_output)

f_out.close()

logging.info("Process ended. \n ---")

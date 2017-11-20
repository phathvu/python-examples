#!/usr/bin/python

#  ============================================================================
#                               AXON INSIGHT AG
#  ============================================================================
#    Function Name.........: GetByUID_ws_client.py
#    Developer.............: Sunwheel team <dn-sunwheel@axonactive.vn>
#    Acronym...............: Sunwheel
#    Create date...........: 07.11.2017
#    Release...............: 1.0.0
#    Description...........: A client to invoke public SOAP Web service. The Web
#                            service accept a UID, then returns related data
#                            (e.g. company name, street name, zip code, town name, etc)
#                            in case the provided UID was found.
#                            WSDL: https://www.uid-wse.admin.ch/V3.0/PublicServices.svc?WSDL
#                            Operation: GetByUID
#                            In case UID was not found by the Web service, an additional check
#                            might be done via https://www.zefix.ch
#    Input.................: Text file contains a UID per a line. Accept code page 'windows-1252'
#    Output................: Text file, each line contains result mapping to each line in the input file.
#                            + found: UID -> OK -> 1(or 2) -> Company name -> legalform -> Street name -> House number
#                                     -> Zip code -> Town name
#                            + not found: UID -> NOK -> 1(or 2)
#                            Tab delimiters. Code page 'windows-1252'
#    Inputparameters.......: You must specify arguments with the following order:
#                            <INPUT_FILE>: Location of the input file.
#                            <OUTPUT_FILE>: (Optional) Location of the output file.
#                            <SERVICE_SOURCE>: (Optional) Service to check UID. Default value is: 1/2
#                            Valid values:
#                              + 1: run Web service only
#                              + 2: run zefix only
#                              + 1/2: run Web service first. If not found, then try with zefix
#                              + 2/1: run zefix first. If not found, then try with Web service
#                            <LIMIT_PER_MINUTE>: (Optional) Maximum UID check per a minute. Example: 120
#                            Default is do as many as possible.
#    Outputparameters......: None
#
#    Example Function call:
#       1,Show help (arguments description, example calls):
#           python GetByUID_ws_client.py -h
#
#       2,
#         python GetByUID_ws_client.py input.txt output.txt 1/2 120
#         python GetByUID_ws_client.py input.txt
#
#    Release notes:
#       07.11.2017 Sunwheel
#           First release
#       14.11.2017 Sunwheel
#           Add additional UID check service zefix.ch
#           Apply limit UID check per a minute
#  =====================================================================================================================

from zeep import Client
import zeep
from datetime import datetime
import sys
import time
import requests
import json

# basic release version
script_name = 'GetByUID_ws_client.py'
version = '1.1.0'
release_date = '14.11.2017'
encode = 'windows-1252'

# webservice wsdl
wsdl = 'https://www.uid-wse.admin.ch/V3.0/PublicServices.svc?WSDL'
client = None

# https://www.zefix.ch
zefix_api = 'https://www.zefix.ch/ZefixREST/api/v1/firm/'
zefix_http_headers = {'Content-Type': 'application/json;charset=UTF-8'}
zefix_payload = {'name': '', 'searchType': 'exact', 'maxEntries': 1, 'offset': 0}

# script arguments
input_file = ''
output_file = ''
source = '1/2'
limit = '-1'  # -1: do as many as possible

mode = ''  # 1: webservice, 2: zefix.ch

total_uid = 0  # from input file, 1 line <-> 1 uid

# result
found_uid_count = 0
not_found_uid_count = 0


def usage():
    """
    Show usage of the script
    """
    print()
    print('\t Usage: python GetByUID_ws_client.py [-h] [--help] <INPUT_FILE> <OUTPUT_FILE> <SERVICE_SOURCE> '
          '<LIMIT_PER_MINUTE>')
    print('\t -h                : help')
    print('\t INPUT_FILE        : location of the input file')
    print('\t OUTPUT_FILE       : (optional)location of the output file')
    print('\t SERVICE_SOURCE    : (Optional) Service to check UID. Default value is: 1/2')
    print('\t\t Valid values:')
    print('\t\t\t 1: run Web service only')
    print('\t\t\t 2: run zefix only')
    print('\t\t\t 1/2: run Web service first. If not found, then try with zefix')
    print('\t\t\t 2/1: run zefix first. If not found, then try with Web service')
    print('\t LIMIT_PER_MINUTE  : (Optional) Maximum UID check per a minute. Default is do as many as possible.')
    print('\n\t Example           : python GetByUID_ws_client.py input.txt output.txt 1/2 120')
    exit(2)


def show_basic_info():
    print('========================================================')
    print('Script name:', script_name)
    print('Version:', version)
    print('Release date:', release_date)
    print()
    print("Input file:", input_file)
    print("Output file:", output_file)
    print("Source of UID check:", get_source())
    print("Max UID check per a minute:", get_limit())
    print('========================================================')


def get_source():
    global source
    if source == '1':
        result = 'Public Webservice only'
    elif source == '2':
        result = 'zefix.ch only'
    elif source == '1/2':
        source = '3'
        result = 'Public Webservice -> zefix.ch'
    elif source == '2/1':
        source = '4'
        result = 'zefix.ch -> Public Webservice'
    return result


def get_limit():
    if limit == '-1':
        return 'No limit. Do as many as possible'
    else:
        return limit


def total_time(diff):
    """
    Calculate time difference (seconds) in a meaningful format (hours:minutes:seconds)
    :param diff: Difference between 2 times
    :return: String that represent difference in hours:minutes:seconds. E.g. 3 hours 4 minutes 30 seconds
    """
    hours, remainder = divmod(diff, 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{} hours {} minutes {} seconds'.format(hours, minutes, seconds)


def prepare_uid_request(uid):
    """
    Convert a string UID to a dictionary. This dictionary is used as the Web service request data
    :param uid: Example CHE239622886 or CHE-239.622.886
    :return: dictionary {'uidOrganisationIdCategorie': 'CHE', 'uidOrganisationId': 239622886}
    """
    uid_organisation_id_category = uid[:3]
    uid_organisation_id = uid[3:].replace('-', '').replace('.', '')
    uid_dict = {'uidOrganisationIdCategorie': str(uid_organisation_id_category),
                'uidOrganisationId': uid_organisation_id}
    return uid_dict


def xstr(s):
    """
    behave like the str() built-in, but return an empty string when the argument is None
    """
    if s is None:
        return ''
    return str(s)


def show_progress(progress):
    sys.stdout.write("\rDONE %.3f%%" % (float(progress) * 100 / float(total_uid)))
    sys.stdout.flush()


def init_wsdl_client():
    # initialize webservice WSDL client (supported by zeep library)
    # this will take around 10 seconds.
    global client
    print('Initializing WSDL client at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    try:
        client = Client(wsdl=wsdl)
    except requests.exceptions.HTTPError as e:
        # Stop the script if WSDL is not correct
        print('Oops! Error when connecting to the Web service WSDL')
        print('Cause: ', e)
        sys.exit(1)


def webservice_request(uid):
    global mode
    mode = '1'  # webservice
    # request data will be sent to the Web service
    uid_dict = prepare_uid_request(uid)
    try:
        result = client.service.GetByUID(uid=uid_dict)
    except zeep.exceptions.Fault:  # error from Web service
        return ''

    if not result:  # uid is not found
        return ''
    else:
        organisation_id = result[0]['organisation']['organisationIdentification']
        company_name = xstr(organisation_id['organisationName'])
        company_legal_form = xstr(organisation_id['legalForm'])
        postal_address = result[0]['organisation']['contact']['address'][0]['postalAddress'][
            'addressInformation']
        street = xstr(postal_address['street'])
        house_number = xstr(postal_address['houseNumber'])
        zip_code = xstr(postal_address['_value_1'][0]['swissZipCode'])
        town_name = xstr(postal_address['town'])
        return company_name + '\t' + \
               company_legal_form + '\t' + \
               street + '\t' + \
               house_number + '\t' + \
               zip_code + '\t' + \
               town_name
    return ''


def zefix_request(uid):
    global mode
    mode = '2'  # zefix
    zefix_payload['name'] = uid
    url = zefix_api + 'search.json'

    retry_times = 0
    # have to make 2 rest api calls to zefix to get enough data
    while retry_times < 2:
        try:
            response1 = requests.post(url=url, data=json.dumps(zefix_payload, indent=4), timeout=60,
                                      headers=zefix_http_headers)
            if response1.ok:  # uid found
                ehraid = response1.json()['list'][0]['ehraid']
                url = zefix_api + str(ehraid) + '.json'
                response2 = requests.get(url=url, timeout=60)
                if response2.ok:  # assume, ehraid found always
                    json_resp = response2.json()
                    address = json_resp['address']
                    company_name = xstr(address['organisation'])
                    company_legal_form = xstr(json_resp['legalFormId'])
                    street = xstr(address['street'])
                    house_number = xstr(address['houseNumber'])
                    zip_code = xstr(address['swissZipCode'])
                    town_name = xstr(address['town'])
                    return company_name + '\t' + \
                           company_legal_form + '\t' + \
                           street + '\t' + \
                           house_number + '\t' + \
                           zip_code + '\t' + \
                           town_name
                return ''
            return ''
        except requests.exceptions.ConnectionError as e:
            print('\nError when sending request to zefix. Will send again. UID:', uid)
            print('Detail error (from zefix):', e)
            retry_times += 1


def build_output_line(result, uid):
    global found_uid_count
    global not_found_uid_count
    tabs = '\t\t\t\t\t\t'
    if result == '':
        not_found_uid_count += 1
        output_line = uid + '\tNOK\t' + mode + tabs
    else:
        found_uid_count += 1
        output_line = uid + '\tOK\t' + mode + '\t' + result
    return output_line


def main(argv):
    """
    Main function
    """
    global input_file
    global output_file
    global source
    global limit
    global client

    # validate required arguments
    if (len(argv) == 1 and argv[0] in ('-h', '--help')) or len(argv) == 0:
        usage()

    # get all arguments
    input_file = argv[0].strip()
    if len(argv) >= 2:
        output_file = argv[1].strip()
    else:  # Define default output file
        import os
        # output file = /path/to/input_file/directory/GetByUID_ws_client_OUTPUT.txt
        output_file = os.path.dirname(input_file).join('GetByUID_ws_client_OUTPUT.txt')

    # check UID service source.
    # valid values:
    #   1: webservice only
    #   2: zefix.ch only
    #   1/2: (default)webservice first. If not found, then try with zefix.ch
    #   2/1: zefix.ch first. If not found, then try with webservice
    if len(argv) >= 3:
        source = argv[2].strip()
        if source not in ('1', '2', '1/2', '2/1'):
            print('Argument \'' + source + '\' is not allowed. Valid values: 1, 2, 1/2 or 2/1')
            sys.exit(2)

    # max UID check per a minute. Default is no limit (Do as many as possible)
    if len(argv) == 4:
        limit = argv[3].strip()

    show_basic_info()
    print('\n========================================================')
    start = time.time()

    if source != '2':
        init_wsdl_client()

    # Open output file, truncate the output file if exist
    target = open(output_file, "w", encoding=encode)
    target.truncate()

    # Get total lines (total uid) in the input file
    global total_uid
    with open(input_file, "r", encoding=encode) as f:
        total_uid = sum(1 for _ in f)
    f.close()
    print('Total UID quantity:', total_uid)
    print('Started processing requests at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    count_flush = 0  # finish process 500 uid -> flush result to the output file
    progress = 0
    count_limit = 0
    with open(input_file, "r", encoding=encode) as fr:
        start_time_limit = time.time()
        for line in fr:
            progress += 1
            line = line.strip()
            # Skip empty line
            if len(line) == 0:
                print('WARN: 1 empty uid found')
                show_progress(progress)
                continue

            # zefix only (2), or zefix -> webservice (4)
            if source == '2' or source == '4':
                result = zefix_request(line)
                if result == '' and source == '4':
                    result = webservice_request(line)
                output_line = build_output_line(result, line)

            # webservice only (1), or webservice -> zefix (3)
            if source == '1' or source == '3':
                result = webservice_request(line)
                if result == '' and source == '3':
                    result = zefix_request(line)
                output_line = build_output_line(result, line)

            target.write(output_line + '\n')
            count_flush += 1
            if count_flush == 50:
                count_flush = 0
                target.flush()
            show_progress(progress)

            count_limit += 1
            if str(count_limit) == limit:
                count_flush = 0
                target.flush()
                sleep_time = start_time_limit + 60 - time.time()  # seconds
                if sleep_time > 0:
                    print('Reached the limit. Wait for ', sleep_time, ' seconds to continue...')
                    time.sleep(sleep_time)
                count_limit = 0
                start_time_limit = time.time()

    target.flush()
    target.close()
    fr.close()

    finish = time.time()
    print('\n\nFinish at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print('Duration: ', total_time(round(finish - start)))
    print('Brief summary:')
    print(' + Total found UID:', found_uid_count)
    print(' + Total not found UID:', not_found_uid_count)
    print('========================================================')


if __name__ == "__main__":
    main(sys.argv[1:])  # sys.argv[0] is the name of the script; we don't care about that

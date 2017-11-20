#!/usr/bin/python

#  ============================================================================
#							   AXON INSIGHT AG
#  ============================================================================
#	 Function Name.........: conos_aicuu_client.py
#	 Developer.............: Sunwheel team <dn-sunwheel@axonactive.vn>
#	 Acronym...............: Sunwheel
#	 Create date...........: 20.07.2017
#	 Release...............: 1.2.0
#	 Description...........: This is a client to test CONOS AI Customer Update endpoints.
#							 Basically, it read requests data in a specific input file.
#							 Make requests to the endpoints.
#	 Input.................: All required input passed as arguments.
#	 Output................: Output file (override if it existing)
#	 Inputparameters.......: You must specify arguments with the following order:
#							 ENDPOINT: make requests to provided endpoint of CONOS AI Customer Update. It's value:
#							   1: /v1.0/person
#							   2: /v1.0/company
#							 ENVIRONMENT: one of following values 'dev', 'test', 'int' or 'prod'
#							 CLIENT_ID: Client ID, for obtaining access token
#							 CLIENT_SECRET: Client secret, for obtaining access token
#							 INPUT_FILE: location of requests input file
#							 OUTPUT_FILE: location of the output file (summary)
#							 NUMBER_THREAD: (optional) number of threads to process data (from 1 -> 8, default is 1)
#
#	 Outputparameters......:
#
#	 Example Function call:
#	   1,Show help (arguments description, example calls):
#			   python conos_aicuu_client.py -h
#
#	   2,Testing with endpoint /v1.0/person at TEST environment with clientId/clientSecret: Admin/123456. Input file
#	   was saved at data/person/input.txt. Result will be saved at data/person/output.txt. Number of threads use to
#	   run test is 5:
#			   python conos_aicuu_client.py 1 test Admin 123456 data/person/input.txt data/person/output.txt 5

#	   3,Testing with endpoint /v1.0/company at INT environment with clientId/clientSecret: Admin/123456. Input
#	   file was saved at data/company/input.txt. Result will be saved at data/company/output.txt Number of threads use
#	   to run test is 5:
#			   python conos_aicuu_client.py 2 int Admin 123456 data/company/input.txt data/company/output.txt 5
#  =====================================================================================================================
#		  Release notes:
#				  20.07.2017 Sunwheel
#					  First release
#				  20.10.2017 Sunwheel
#					  Update json interface
#					  Apply multithread processing
#				  27.10.2017 Sunwheel
#					  Handle unicode characters, e.g \u2013, \u017e, \u0161
#					  Got status code 408, 502 -> resend 2 times
#					  Make JSON attributes is correct order
#					  Revise Console log + output file
#				  31.10.2017 Sunwheel
#					  In case got status code 400 then NO kill running thread
#				  20.11.2017 Sunwheel
#					  Change mechanism to read file: load each 1000 records per time instead of loading the whole file
#					  to memory
#  =====================================================================================================================


import sys
import requests
import time
import json

import threading
from queue import Queue
from datetime import datetime
import collections

script_name = 'conos_aicuu_client.py'
version = '1.3.0'
release_date = '2017-10-31'

conos_config = dict()
encode = 'windows-1252'
endpoint = {'1': '/person', '2': '/company'}

dev_sts_url = 'http://192.168.80.13:8080/conos_oauth/v1.0'
test_sts_url = 'http://conos-oauth-test.mappuls.int/v1.0'
int_sts_url = 'https://conos-oauth-int.axoninsight.com/v1.0'
prod_sts_url = 'https://conos-oauth.axoninsight.com/v1.0'

dev_aicuu_url = 'http://192.168.80.13:8080/conos_aicuu/v1.0'
test_aicuu_url = 'http://conos-customer-update-test.mappuls.int/v1.0'
int_aicuu_url = 'https://conos-customer-update-int.axoninsight.com/v1.0'
prod_aicuu_url = 'https://conos-customer-update.axoninsight.com/v1.0'

console = ''
queueLock = threading.Lock()
workQueue = Queue()
threads = []
exitFlag = 0
num_lines = 0
count = 0
success = 0
url = ''
data_file_name = ''
headers = ''
token_expired = True
target = None
openQueue = False


# ======================================================================================================================
def obtain_access_token():
	"""
	obtain_access_token() -> Get access token

	Obtain access token from provided credential (client id + client secret).
	"""
	post_data = {'grant_type': 'client_credentials',
				 'client_id': conos_config['client_id'],
				 'client_secret': conos_config['client_secret']}

	try:
		response = requests.post(url=conos_config['sts_url'], data=post_data, timeout=60)  # 60 seconds
		if response.ok:
			return 'Bearer ' + response.json()['access_token']
		else:
			print('\nERROR: Can not obtain access token')
			print('\nResponse error: ', response.json())
			response.raise_for_status()
	except requests.exceptions.RequestException as e:
		# All exceptions that Requests explicitly raises inherit from requests.exceptions.RequestException
		print("Root cause: ", e)
		sys.exit(1)

# ======================================================================================================================
# Parse input line to JSON as a request body
def prepare_inp_json(data):
	payload = collections.OrderedDict()
	if conos_config['endpoint'] == '/person':
		payload['OBJECT_ID']= data[0]
		payload['SYNCHRONISATION'] = data[1]
		payload['SEX_CODE'] = data[2]
		payload['TITLE'] = data[3]
		payload['FIRST_NAME'] = data[4]
		payload['LAST_NAME'] = data[5]
		payload['BIRTH_YEAR'] = data[6]
		payload['BIRTH_DATE'] = data[7]
		payload['ADDITIONAL_ADDRESS'] = data[8]
		payload['STREET_NAME'] = data[9]
		payload['STREET_NUMBER'] = data[10]
		payload['ZIP'] = data[11]
		payload['CITY'] = data[12]
		payload['CANTON'] = data[13]
		payload['COUNTRY_CODE'] = data[14]
		payload['POBOX_NUMBER'] = data[15]
		payload['POBOX_ZIP'] = data[16]
		payload['POBOX_CITY'] = data[17]
		payload['MODIFICATION_DATE'] = data[18]
		payload['PHONENUM'] = data[19]
		payload['MOBILENUM'] = data[20]
		payload['EMAIL'] = data[21]


	elif conos_config['endpoint'] == '/company':
		payload['OBJECT_ID'] = data[0]
		payload['SYNCHRONISATION'] = data[1]
		payload['UID'] = data[2]
		payload['COMPANY_NAME'] = data[3]
		payload['ADDITIONAL_ADDRESS'] = data[4]
		payload['STREET_NAME'] = data[5]
		payload['STREET_NUMBER'] = data[6]
		payload['ZIP'] = data[7]
		payload['CITY'] = data[8]
		payload['CANTON'] = data[9]
		payload['COUNTRY_CODE'] = data[10]
		payload['POBOX_NUMBER'] = data[11]
		payload['POBOX_ZIP'] = data[12]
		payload['POBOX_CITY'] = data[13]
		payload['MODIFICATION_DATE'] = data[14]
		payload['PHONENUM'] = data[15]
		payload['MOBILENUM'] = data[16]
		payload['FAXNUM'] = data[17]
		payload['EMAIL'] = data[18]
		payload['URL'] = data[19]

		ceo = collections.OrderedDict()
		ceo['FUNCTION'] = 'CEO'
		ceo['OBJECT_ID'] = data[20]
		ceo['SEX_CODE'] = data[21]
		ceo['TITLE'] = data[22]
		ceo['FIRST_NAME'] = data[23]
		ceo['LAST_NAME'] = data[24]

		cfo = collections.OrderedDict()
		cfo['FUNCTION'] = 'CFO'
		cfo['OBJECT_ID'] = data[25]
		cfo['SEX_CODE'] = data[26]
		cfo['TITLE'] = data[27]
		cfo['FIRST_NAME'] = data[28]
		cfo['LAST_NAME'] = data[29]

		chro = collections.OrderedDict()
		chro['FUNCTION'] = 'CFO'
		chro['OBJECT_ID'] = data[30]
		chro['SEX_CODE'] = data[31]
		chro['TITLE'] = data[32]
		chro['FIRST_NAME'] = data[33]
		chro['LAST_NAME'] = data[34]

		contact = [ceo, cfo, chro]
		payload['CONTACT'] = contact

	return payload
# You must specify arguments with the following order:
#							 ENDPOINT: make requests to provided endpoint of CONOS AI Customer Update. It's value:
#							   1: /v1.0/person
#							   2: /v1.0/company
#							 ENVIRONMENT: one of following values 'dev', 'test', 'int' or 'prod'
#							 CLIENT_ID: Client ID, for obtaining access token
#							 CLIENT_SECRET: Client secret, for obtaining access token
#							 INPUT_FILE: location of requests input file
#							 OUTPUT_FILE: location of the output file (summary)
#							 NUMBER_THREAD: (optional) number of threads to process data (from 1 -> 8, default is 1)')

def read_arguments(argv):
	"""
	Read 6 required arguments from command line

	Then save them to a global dictionary conos_config
	"""
	if argv[0] in ('1', '2'):
		conos_config['endpoint'] = endpoint[argv[0]]
	else:
		usage()

	if argv[1] in ('dev', 'test', 'int', 'prod'):
		conos_config['environment'] = argv[1]
		conos_config['sts_url'] = eval(argv[1] + '_sts_url')
		conos_config['aicuu_url'] = eval(argv[1] + '_aicuu_url')
	else:
		usage()

	if len(argv) == 6:
		conos_config['number_threads'] = '1'
	else:
		if argv[6] in ('1', '2', '3', '4', '5', '6', '7', '8'):
			conos_config['number_threads'] = argv[6]
		else:
			usage()

	conos_config['client_id'] = argv[2]
	conos_config['client_secret'] = argv[3]
	conos_config['input_file'] = argv[4]
	conos_config['output_file'] = argv[5]


def usage():
	"""
	Show usage of the script
	"""
	print()
	print(
		'\t Usage: python conos_aicuu_client.py [-h] [--help] <ENDPOINT> <ENVIRONMENT> <CLIENT_ID> <CLIENT_SECRET> <INPUT_FILE> <OUTPUT_FILE> <NUMBER_THREAD>')
	print('\t -h             : help')
	print('\t ENDPOINT       : 1: /v1.0/person')
	print('\t                  2: /v1.0/company')
	print('\t ENVIRONMENT    : must be one of: dev, test, int, prod')
	print('\t CLIENT_ID      : used for obtaining access token')
	print('\t CLIENT_SECRET  : used for obtaining access token')
	print('\t INPUT_FILE     : location of the request input file')
	print('\t OUTPUT_FILE    : location of the output file (analyze report)')
	print('\t NUMBER_THREAD  : (optional) number of threads to process data (from 1 -> 8, default is 1)')
	print('\n\t Example        : python conos_aicuu_client.py 1 test Admin 123456 data/person/input.txt data/person/output.txt 5')
	exit(2)


def total_time(diff):
	"""
	Calculate time difference (seconds) in a meaningful format (hours:minutes:seconds)
	:param diff: Difference between 2 times
	:return: String that represent difference in hours:minutes:seconds. E.g. 3 hours 4 minutes 30 seconds
	"""
	hours, remainder = divmod(diff, 3600)
	minutes, seconds = divmod(remainder, 60)
	return '{} hours {} minutes {} seconds'.format(hours, minutes, seconds)



class AicuuThread(threading.Thread):
	def __init__(self, threadID, name, q):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.name = name
		self.q = q
	def run(self):
		make_request(self.name, self.q)


def make_request(threadName, q):
	global console
	global queueLock
	global openQueue
	global count
	global success
	global headers
	while not exitFlag:
		if openQueue:
			queueLock.acquire()
		else:
			continue
		if not workQueue.empty():
			line = q.get()
			queueLock.release()
			count += 1
			if len(line) <= 1:  # empty line still have character \n
				continue
			arr = line.strip('\n').split('\t')  # strip('\n'): remove \n at the end of each line
			payload = prepare_inp_json(arr)
			token_expired = True
			should_retry = True
			retry_times = 0
			# if token expired, re-obtain token, then make request again
			# if request time-out, or bad gateway (502), try re-send 2 times
			while token_expired or (should_retry and retry_times <= 2):
				try:
					response = requests.post(url=url, data=json.dumps(payload, ensure_ascii=False, indent=4).encode('utf-8'), headers=headers, timeout=90)  # 90 seconds
					token_expired = False
					should_retry = False
					# response.encoding = encode
					status_code = response.status_code
					if status_code == 200: # success
						success += 1
					elif status_code == 401: # Invalid token, need to re-obtain
						tmp_log = '\nToken expired. Try to get a new one '
						print(tmp_log)
						console += '\n' + tmp_log
						token_expired = True
						headers['Authorization'] = obtain_access_token()
					elif status_code == 403: # Forbidden
						tmp_log = '\nForbidden. Access denied for user ' + conos_config['client_id']
						print(tmp_log)
						console += '\n' + tmp_log
						write_output()
						console = ''
						sys.exit(1)
					elif status_code in [408, 502]:  # try again
						# 408 <-The operation timed out
						# 502 <-Bad gateway
						should_retry = True
						retry_times += 1
						#print(tmp_log)
						console += '\n\nGot status ' + str(status_code) + '. Try to re-send request: ' + line
					else:
						tmp_log = '\nGot status ' + str(status_code) + ' for this request: ' + line
						print(tmp_log)
						console += tmp_log
						response.raise_for_status()
				except requests.exceptions.RequestException as e:
					print('Root cause: ', e)
				#sys.exit(1)
			sys.stdout.write("\rDONE %.3f%%" % (float(count) * 100 / float(num_lines)))
			sys.stdout.flush()
		else:
			openQueue = False
			queueLock.release()


def create_threads():
	global console
	for i in range(0, int(conos_config['number_threads'])):
		tmp_log = 'Thread-' + str(i) + ' created'
		print(tmp_log)
		console += '\n' + tmp_log
		thread = AicuuThread(i, 'Thread-' + str(i), workQueue)
		thread.start()
		threads.append(thread)
	print('\n')

def create_queue():
	global workQueue
	global openQueue
	global num_lines
	count = 0

	with open(data_file_name, "r+", encoding=encode) as fp:
		for line in fp:
			if (count != 0) and (count % 1000 == 0):
				openQueue = True
				while openQueue:
					pass
				openQueue = False
			workQueue.put(line)
			count = count + 1
	fp.close()
	openQueue = True

def init_value():
	global url
	global data_file_name
	global headers
	global target
	global num_lines
	url = conos_config['aicuu_url'] + conos_config['endpoint']
	data_file_name = conos_config['input_file']

	headers = {'Content-Type': 'application/json; charset=utf-8', 'Authorization': obtain_access_token()}

	target = open(conos_config['output_file'], "w", encoding=encode)
	target.truncate()  # Truncating the output file.

	# Get total lines in the input file
	with open(data_file_name, "r+", encoding=encode) as f:
		num_lines = sum(1 for _ in f)
		f.close()

def show_release_version():
	global console
	console += '\n========================================================' + \
			   '\nRunning Python script for stress testing AICUU endpoints' + \
			   '\nScript name: ' + script_name + \
			   '\nVersion: ' + version + \
			   '\nRelease date: ' + release_date + \
			   '\n========================================================'


def show_input_args():
	global console
	console += '\n\n========================================================' + \
			   '\nINPUT ARGUMENTS' + \
			   '\n- Environment: ' + conos_config['environment'].upper() + \
			   '\n  + STS: ' + conos_config['sts_url'] + \
			   '\n  + AICUU: ' + conos_config['aicuu_url'] + \
			   '\n- Endpoint: ' + conos_config['endpoint'] + \
			   '\n- Credential: ' + conos_config['client_id'] + '/' + conos_config['client_secret'][:2] + 'xxxxx' + \
			   '\n- Input data path: ' + conos_config['input_file'] + \
			   '\n- Output data path: ' + conos_config['output_file'] + \
			   '\n- Total threads: ' + conos_config['number_threads'] + \
			   '\n========================================================'
	print(console)

def write_output():
	target.write(console)
	target.flush()

def main(argv):
	"""
	Main function
	"""
	if (len(argv) == 1 and argv[0] in ('-h', '--help')) or len(argv) < 6:
		usage()
	read_arguments(argv)
	show_release_version()
	show_input_args()

	global console
	tmp_log = '\n========================================================' + \
			  '\nStarted processing requests at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print(tmp_log)
	console += '\n' + tmp_log

	start = time.time()
	init_value()

	tmp_log = 'Total requests: ' + str(num_lines) + '\n'
	print(tmp_log)
	console += '\n' + tmp_log

	write_output()
	console = ''

	create_threads()
	create_queue()

	# Wait for queue to empty
	while not workQueue.empty():
		pass

	# Notify threads it's time to exit
	global exitFlag
	exitFlag = 1

	console += '\n\nDONE 100.000%'
	# Wait for all threads to complete
	for t in threads:
		t.join()
	tmp_log = '\nExiting Main Thread'
	print(tmp_log)
	console += tmp_log
	finish = time.time()

	tmp_log = '\nFinish at : ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '. Duration: ' + total_time(round(finish - start)) + \
			  '\nSuccess: ' + str(success) + \
			  '\nFailed: ' + str(num_lines - success) + \
			  '\n========================================================'
	print(tmp_log)
	console += '\n' + tmp_log

	write_output()
	target.close()

if __name__ == "__main__":
	main(sys.argv[1:])  # sys.argv[0] is the name of the script; we don't care about that
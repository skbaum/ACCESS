#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime,date
import sys
import logging
import os
import json
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from amieclient import AMIEClient

test = 0

script_name = 'dump_approvals'
#script_name = sys.argv[0][2:]

ACCESS_HOME = os.getcwd()
if ACCESS_HOME == '/home/baum/AMIEDEV':
    amie_config = ACCESS_HOME + '/amiedev.ini'
    email_receivers = ['baum@tamu.edu']
    print(" DEVELOPMENT MODE!!!!!")
elif ACCESS_HOME == '/home/baum/AMIE':
    amie_config = ACCESS_HOME + '/amie.ini'
# SHUT OFF EMAILS
    email_receivers = ['baum@tamu.edu']
    email_receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
    print(" PRODUCTION MODE!!!!")
else:
    print(" Not running in either '/home/baum/AMIE' or '/home/baum/AMIEDEV'. Exiting.")
    sys.exit()

log_file = ACCESS_HOME + '/amie.log'

#  Configuration and log file processing.
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

trans_rec_id_skip = 0

#  Command-line argument processing
diag = 0
help_list = ['-h','--h','-H','--H','-help','--help','-Help','--Help']
#  Check for the presence of a command-line argument.
if len(sys.argv) > 1:
#  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(" Program: ",script_name," - Process incoming ACCESS packets and add entries to various database tables")
        print(" Arguments: ",help_list," - this message")
        print("             None - run the program")
        print("             1 (i.e. '1' is the argument) - run the demo program with additional diagnostic output")
        print("             trans_rec_id - a transaction record ID number to skip")
        print("             All other arguments rejected.")
        print(" ")
        sys.exit()
    else:
#  Check for valid command-line arguments.
        if sys.argv[1].isdigit():
            if int(sys.argv[1]) == 1:
                diag = 1
                print("Diagnostic argument ",diag," was entered.")
            else:
#  This can be any integer other than 1.  If it doesn't match a trans_rec_id in the ACCESS queue, nothing will be skipped.
                trans_rec_id_skip = int(sys.argv[1])
                print("Transaction record number ",trans_rec_id_skip, " was entered to skip.")
        else:
            print("The supplied argument ",sys.argv[1]," is not usable.  Exiting.")
#sys.exit()

#  Email notifications
def send_email(body):
    email_sender = 'admin@faster-mgt2.hprc.tamu.edu'
#    if ACCESS_HOME == '/home/baum/AMIEDEV':
#        email_receivers = ['baum@tamu.edu']
#    elif ACCESS_HOME == '/home/baum/AMIE':
#        email_receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
    msg = MIMEMultipart()
    if ACCESS_HOME == '/home/baum/AMIEDEV':
        msg['Subject'] = 'DEV: ACCESS Packet Alert'
    elif ACCESS_HOME == '/home/baum/AMIE':
        msg['Subject'] = 'ACCESS Packet Alert'
    msg['From'] = email_sender
    msg['To'] = ",".join(email_receivers)
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP('smtp-relay.tamu.edu') as server:
        server.sendmail(email_sender, email_receivers, msg.as_string())
#        print("   Notification of packet reception sent to:  ",','.join(email_receivers))

# amiedb_call(sql,data,script_name,results)
def amiedb_call(sql,data,script_name,results):
    if diag > 0:
        print(" AMIEDB_CALL: sql = ",sql)
        print(" AMIEDB_CALL: data = ",data)
    try:
        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
    except:
        print(" AMIEDB_CALL: Error connecting to database in ",script_name)
        print(" AMIEDB_CALL: Problematic SQL: ",sql)
    cur = conn.cursor()
    try:
        cur.execute(sql,data)
        conn.commit()
        try:
            matches = cur.fetchall()
            len_matches = len(matches)
            if diag > 0:
                print(" AMIEDB_CALL: No. of matches: ",len_matches," match(es) = ",matches)
            for match in matches:
                results.append(match)
        except:
            results = []
            if diag > 0:
                print(" AMIEDB_CALL: No cur.fetchall() results to process.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("*************** DB transaction error ***************: ",exc_info=True)
#  This executes after either 'try' or 'except'.
    finally:
        if conn:
            cur.close()
            conn.close()
    return

def print_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_num = traceback.tb_lineno
    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def tstamp():
    """Function for timestamp."""
    tst = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return tst

def slurm_rand():
    """Dummy function for last part of slurm account number."""
    locran = "%0.4d" % random.randint(0,9999)
    return locran

#  Extract the value of any key in a nested JSON tree.
#  Note: This doesn't work when the value is a list.
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []
    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr
    values = extract(obj, arr, key)
    return values

null = 'NULL'

xdusage_api_key = "IVyD2kiOUX8ciuku4oyZuor7Hg4hTyPASXR7W+zLqn49CYYlBFtDmMZ5kGwSFHP5"
site_name = 'faster.tamu.xsede.org'
curl_header_v = 'curl -v -X GET -H "XA-Agent: xdusage" -H "XA-Resource: ' + site_name + '" -H "XA-API-Key: ' + xdusage_api_key + '"'
curl_header = 'curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: ' + site_name + '" -H "XA-API-Key: ' + xdusage_api_key + '"'

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']
amie_client = AMIEClient(site_name=site_con['site_name'],
                         amie_url=site_con['amie_url'],
                         api_key=site_con['api_key'])

packets = amie_client.list_packets().packets
npack = len(packets)
print(' ')
logging.info(script_name + ': processing ' + str(npack) + ' incoming packet(s)')
print(' Processing ',npack,' incoming packets with ',sys.argv[0],' at ',tstamp())
if npack == 0:
    sys.exit()

#  List of transaction types that need to be flagged for approval before processing.
approvals = ['request_project_create','request_account_create','request_project_inactivate','request_project_reactivate','request_account_inactivate','request_account_reactivate','request_user_modify','request_person_merge']
allpack = ['request_project_create','request_account_create','request_project_inactivate','request_project_reactivate','request_account_inactivate','request_account_reactivate','request_user_modify','request_person_merge','data_project_create','data_account_create','inform_transaction_complete']

no_person_id = ['request_project_inactivate','request_project_reactivate']
no_project_id = ['request_user_modify','request_person_merge']
#  Set list of allocation types for different processing modalities for 'request_project_create' packets.
allocation_types = ['new','renewal','supplement','extension','transfer','advance','adjustment']

npack_unfiltered = len(packets)
print(" Number of unfiltered packets: ",npack_unfiltered)

#  Filtering out those eternally persistent 'inform_transaction_complete' packets.
filtered_packets = []
for packet in packets:
    pdict = json.loads(packet.json())
    type_id = json_extract(pdict,'type')[0]
    if type_id != 'inform_transaction_complete':
        filtered_packets.append(packet)
#        print(" packet = ",vars(packet))
        print(" ")
        print(" type_id = ",type_id)
npack = len(filtered_packets)
print (" Number of filtered packets to process: ",npack)

#  TEST EXIT
#sys.exit()

pnum = 1
for packet in filtered_packets:

#  The 'allocation_type' variable is default set to 'none', and only set otherwise for 'request_project_create' packets.
    allocation_type = 'none'
#  This is set to 1 when an allocation type is a proxy for 'new'.
    new_proxy = 0
    trans_rec_id = packet.trans_rec_id
#  If a trans_rec_id to skip is supplied as an argument, skip over it here.
    if int(trans_rec_id) == trans_rec_id_skip:
        print("############################################################")
        print("############################################################")
        print("Skipping packet with trans_rec_id = ",trans_rec_id_skip,".")
        print("############################################################")
        print("############################################################")
        continue
    packet_rec_id = packet.packet_rec_id
    pdict = json.loads(packet.json())
    type_id = json_extract(pdict,'type')[0]
    logging.info(script_name + ": processing packet no. " + str(pnum) + " of type '" + type_id + "'.")
    print(" ")
    print("Processing packet no. ",pnum," of type '",type_id,"' with trans_rec_id = ",trans_rec_id," and packet_rec_id = ",packet_rec_id,".")
#    print("Early skipping.")
    print("type_id = ",type_id)
#  Skip 'data_account_create' packets.#  Skip 'data_account_create' packets.
    if type_id == 'data_account_create':
        print("Skipping ",type_id,"' packet.")
        continue

#  Dump packet to text file as 'xxx/xxx-trans_rec_id.json' for archival purposes (if not already dumped)
#  where 'xxx' is the first letters of the transaction type, e.g. request_project_create = rpc.
    packet_type = type_id
    pref = packet_type.split("_")[0][0] + packet_type.split("_")[1][0] + packet_type.split("_")[2][0]
    trans_rec_id = packet.trans_rec_id
    jsonfile = pref + "/" + pref + str(trans_rec_id) + ".json"
    if not os.path.isfile(jsonfile):
        with open(jsonfile, 'w') as convert_file:
            convert_file.write(json.dumps(pdict))
#            print(" Packet ",jsonfile," dumped.")
    else:
        c_time = os.path.getctime(jsonfile)
        dt_c = datetime.fromtimestamp(c_time)
#        print(" Packet '",jsonfile,"' of type '",type_id,"' previously archived on '",dt_c,"'. Skipping.")

#    print("pdict = ",pdict)
#    sys.exit()

#  If transaction type if one of seven that require approval, process the packet.
    if type_id in allpack:

# ================== PROCESSING FOR local_info TABLE ========================

# {"DATA_TYPE": "Packet", "type": "request_project_create", "body": {"AllocationType": "new", "GrantNumber": "EVB234964", "PfosNumber": "20800", "PiFirstName": "Sajjad", "PiLastName": "Afroosheh", "PiOrganization": "University of Washington", "PiOrgCode": "0037986", "RecordID": "XRAS-110647-faster.tamu.xsede.org", "StartDate": "2023-06-16T00:00:00", "EndDate": "2024-06-16T00:00:00", "ServiceUnitsAllocated": "1", "ResourceList": ["faster.tamu.xsede.org"], "BoardType": "Startup", "RequestType": "new", "Abstract": "Lorem ipsum dolor est...", "ChargeNumber": "TG-EVB234964", "PiGlobalID": "114431", "PiMiddleName": "", "PiDepartment": "", "PiTitle": "", "NsfStatusCode": "PD", "PiDnList": ["/C=US/O=National Center for Supercomputing Applications/CN=Sajjad Afroosheh 2", "/C=US/O=Pittsburgh Supercomputing Center/CN=Sajjad Afroosheh 2"],
####    "SitePersonId": [{"Site": "XD-ALLOCATIONS", "PersonID": "safroosh480"}], 
# "ProjectTitle": "Lorem Ipsum", "Sfos": [{"Number": "0"}], "AllocatedResource": "faster.tamu.xsede.org", "ProposalNumber": "EVB234964"}, "header": {"packet_rec_id": 233441142, "packet_id": 1, "transaction_id": 766, "trans_rec_id": 116811609, "expected_reply_list": [{"type": "notify_project_create", "timeout": 30240}], "local_site_name": "TGCDB", "remote_site_name": "TAMU", "originating_site_name": "TGCDB", "outgoing_flag": 1, "transaction_state": "in-progress", "packet_state": "in-progress", "packet_timestamp": "2023-06-16T20:46:03.601Z"}}

#  SKIP OVER EVERYTHING IF THIS PACKET IS ALREADY IN THE APPROVAL TABLE.
#  Find if this already exists in approval table.
        sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        in_approval = len(results)
#        print("in_approval = ",in_approval)
        if in_approval != 0:
            print("Packet of type ",type_id," with trans_rec_id ",trans_rec_id," already exists in 'approval' table.  Skipping.")
            continue
        else:
            print("Packet not already in 'approval' table.  Processing.")

#######################################################################
#  PACKET:  REQUEST_PROJECT_CREATE   OOPS
#######################################################################
        if type_id == 'request_project_create':

#  Extract information from RPC packet.
            allocation_type = packet.AllocationType
            first_name = packet.PiFirstName
            last_name = packet.PiLastName
            pi_global_id = packet.PiGlobalID
#  Skip if global_id already exists for the same 'project_id'.
#  Extract the PersonID for the SitePersonId dictionary with the X-PORTAL key value.
            site_person_id = packet.SitePersonId
            try:
                access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']
            except Exception as e:
                msg1 = "ERROR: ACCESS ID not found in RPC packet for PI " + first_name + " " + last_name + " with global ID " + pi_global_id + ". Setting to 'unknown'."
                print(msg1)
                logging.info(script_name + ": " + msg1)
                access_id = 'unknown'
            email = packet.PiEmail
            if email == None:
                email = 'unknown'
            grant_number = packet.GrantNumber
            service_units_allocated = packet.ServiceUnitsAllocated
            resource_list = packet.ResourceList[0]
            cluster = resource_list.split('.')[0]
            start_date = packet.StartDate
            end_date = packet.EndDate

#  Check for proxies for 'new' allocation type for 'renewal', 'supplement' and 'advance' types.
#  Set new_proxy to 0 until proven otherwise.
            new_proxy = 0
#  A positive 'transfer' should be treated the same as a 'supplement'.  No need to check for positivity of SU if
#  allocation type is 'transfer' and local account is presently nonexistent. 
            new_proxies = ['renewal','supplement','advance','transfer']
            allocation_type = packet.AllocationType

#  Create standard 'person_id' and 'project_id' to check for existing entry matches.
#  Check if the packet contains 'ProjectID'.  All allocation type variations that open new projects will not have it.
#  Packets that modify existing projects will have it.
            project_id = packet.ProjectID
            if project_id == None:
                project_id = 'p.' + grant_number.lower() + '.000'
                print("This RPC packet will create a new local project ",project_id,".")
            else:
                print("This RPC packet will modify the existing project ",project_id,".")
            person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + pi_global_id

#  Skip if same global_id already exists for the same 'project_id'.
#             pgi = "%" + pi_global_id
#            sql =  "SELECT first_name,last_name,person_id,project_id,uid,gid,access_id FROM local_info WHERE person_id LIKE %s"
#            data = (pgi,)
#            results = []
#            amiedb_call(sql,data,script_name,results)
#  Extract 'project_id' for previous same global ID entry.
#            if len(results) != 0:
#                proj_id = results[0][3]
#                if proj_id == project_id:
#                    print("There is already an entry in 'local' into with global ID ",pgi,".")
#                    print("first_name    last_name    person_id    project_id    uid    gid    access_id")
#                    for res in results:
#                        print(res)
#                    print("The values in the packet are:")
#                    print("first_name  last_name  person_id  project_id  access_id")
#                    print(first_name,last_name,person_id,project_id,access_id)
#                    sys.exit()
#                    continue


#  Update the start and end dates in local_info for each incoming RPC packet.
            try:
                sql = "UPDATE local_info SET start_date = %s, end_date = %s WHERE project_id = %s AND cluster = %s"
                data = (start_date,end_date,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
            except Exception as e:
                print("Unable to update start and end dates for project ",project_id," on cluster ",cluster,".")
                print("Error: ",str(e))

#  Check to see if a local project already exists in 'local_info' for this person_id/project_id/cluster combination.
#  Select 'proj_stat` to discover if this 'request_project_create' is a proxy for 'request_project_reactivate'.
            sql = "SELECT proj_stat FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            already_in_local_info = len(results)

#  Check to see if the 'request_project_create' packet is a proxy for a 'request_project_reactivate' packet.
            reactivate_project_proxy = 0
            if already_in_local_info != 0:
                print("The person_id/project_id/cluster combination is already in 'local_info'.")
#  Case: This is a proxy for a request_project_reactivate packet if proj_stat = inactive.
                proj_stat = results[0][0]
                if proj_stat == 'inactive':
                    reactivate_project_proxy = 1
                    print("This RPC packet is a proxy for a 'request_project_reactivate' packet.")
                else:
                    reactivate_project_proxy = 0
#                    print("This RPC packet is already in 'local_info'.")
            else:
                print("The person_id/project_id/cluster combination is not already in 'local_info'.  Will create entry.")

#  If the allocation type is in new_proxies AND the account doesn't already exist in 'local_info', set new_proxy = 1.
            if allocation_type in new_proxies:
                if already_in_local_info == 0:
                    print("Allocation type ",allocation_type," for ",type_id," for ",person_id," on project ",project_id," on resource ",cluster," is a proxy for 'new'.")
                    new_proxy = 1

#  Make things easier in respond.py.
            if allocation_type == 'new':
                new_proxy = 1

            print("allocation_type = ",allocation_type," new_proxy = ",new_proxy," reactivate_project_proxy = ",reactivate_project_proxy)

#  Insert a line for all RPC packets into the 'rpc_packets' table.
#  Check if this trans_rec_id is already in 'rpc_packets' table.
            sql = "SELECT * FROM rpc_packets WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
#            print("results = ",results)
            if len(results) == 0:
                already_in_rpc_packets = 0
                print("Packet ",trans_rec_id," not in 'rpc_packets' table.")
            else:
                already_in_rpc_packets = 1
                print("Packet ",trans_rec_id," in 'rpc_packets' table.")
#  Check if this is already in 'approval' table.
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
#            print("results = ",results)
            if len(results) == 0:
                already_in_approval = 0
                print("Packet ",trans_rec_id," not in 'approval' table.")
            else:
                already_in_approval = 1
                print("Packet ",trans_rec_id," in 'approval' table.")
#  Check if this person_id/project_id/cluster combination is in the 'local_info' table.
            sql = "SELECT * from local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            if len(results) != 0:
                already_in_local_info = 1
#            print("rpc_packets: ",already_in_rpc_packets," approval: ",already_in_approval," local_info = ",already_in_local_info)
            if test == 0:
#  If this packet has not been inserted into 'rpc_packets', do so.
                if already_in_rpc_packets == 0:
#  If this person_id/project_id/cluster combination is already in 'local_info', grab remote ACCESS info to put in 'rpc_packets' table.
#  If not, put NULL values in final four columns.
                    if already_in_local_info == 1:
                        print("Already in 'local_info'.  Getting ACCESS data.")
#  Set correct value of 'resource_id' for API call.
#  https://allocations-api.access-ci.org/acdb/apidoc/1.0/xdusage.html
#  curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: faster.tamu.xsede.org" -H "XA-API-Key: X" -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/resources/faster.tamu.xsede.org'
                        if cluster == 'faster':
                            resource_id = '3295'
#  curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: faster.tamu.xsede.org" -H "XA-API-Key: X" -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/resources/aces.tamu.access.org'
                        elif cluster == 'aces':
                            resource_id = '3362'
                        elif cluster == 'launch':
                            resource_id = '3659'
#  Use xdusage API to get info from ACCESS side.
#  curl -X GET -H "XA-Agent: xdusage" -H "***" -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/projects?projects=civ230007&resource_id=3295'
#
#  curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: faster.tamu.xsede.org" -H "XA-API-Key: IVyD2kiOUX8ciuku4oyZuor7Hg4hTyPASXR7W+zLqn49CYYlBFtDmMZ5kGwSFHP5" -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/projects/?projects=TRA220029&resource_id=3295'
#
#  The launch cluster doesn't yet have a 'resource_id'.  This is a temporary workaround for that.
                        if resource_id != 'unknown':
                            curl_string = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/projects?projects=" + grant_number + "&resource_id=" + resource_id + "'"
                            proj_filename = grant_number + "_" + resource_id + ".json"
                            curl_string_all = curl_string + " > " + proj_filename + " 2> err.txt"
                            os.system(curl_string_all)
                            f = open(proj_filename)
                            data = json.load(f)
                            results = data['result']
#                            print(results)
                            try:
                                cur_all = float(results[0]['current_allocation'])
                                tot_all = float(results[0]['total_allocation'])
                                charges = float(results[0]['project_charges'])
                                balance = float(results[0]['project_balance'])
#                                os.system("rm " + proj_filename)
                                print("Obtained results from xdusage API query.")
                            except:
                                already_in_local_info = 0
                                msg1 = "Failed to obtain results from xdusage API query.  Setting 'cur_all' to SU value " + str(service_units_allocated) + " in packet."
                                msg2 = "Using the RPC packet SU value to set 'cur_all' is the default procedure for newly created projects."
                                msg3 = "API query: " + curl_string
                                send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                                logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                        else:
                            print("The 'resource_id' is unknown.  The last four columns of 'rpc_packets' will be filled with 0.0 placeholder values.")
                            cur_all = 0.0
                            tot_all = 0.0
                            charges = 0.0
                            balance = 0.0
#  Insert new row in 'rpc_packets'.                    
                    notify = 'Y'
                    try:
                        print("Attempting insertion.")
                        if already_in_local_info == 1:
                            sql = "INSERT INTO rpc_packets (trans_rec_id,person_id,project_id,service_units,start_date,end_date,allocation_type,proxy,cluster,notify,access_id,ts_received,cur_all,tot_all,charges,balance) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            data = (trans_rec_id,person_id,project_id,service_units_allocated,start_date,end_date,allocation_type,new_proxy,cluster,notify,access_id,tstamp(),cur_all,tot_all,charges,balance,)
                            print("Inserting with ACCESS info.")
                        else:
                            sql = "INSERT INTO rpc_packets (trans_rec_id,person_id,project_id,service_units,start_date,end_date,allocation_type,proxy,cluster,notify,access_id,ts_received,cur_all) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            data = (trans_rec_id,person_id,project_id,service_units_allocated,start_date,end_date,allocation_type,new_proxy,cluster,notify,access_id,tstamp(),service_units_allocated)
                            print("Inserting without ACCESS info.")
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        print("Inserted line for RPC packet of type ",allocation_type," into 'rpc_packets' for ",person_id," on project ",project_id," on resource ",cluster,".")
                        print("Supposedly inserted.")
                    except Exception as e:
                        print("Problem inserting entry in 'rpc_packets'.  Exiting.")
                        print("ERROR: ",str(e))
                        sys.exit()
#  If this packet is in 'rpc_packets', continue to next packet.
                else:
                    print("The RPC packet with trans_rec_id ",trans_rec_id," and allocation type ",allocation_type," is already in 'rpc_packets'.")
#                    continue
            else:
                if len(results) == 0:
                    print("TESTING:  Line inserted in 'rpc_packets'.  Message would be:")
                    print("Inserted line for RPC packet of type ",allocation_type," into 'rpc_packets' for ",person_id," on project ",project_id," on resource ",cluster,".")
                else:
                    print("TESTING:  The RPC packet with trans_rec_id ",trans_rec_id," is already in 'rpc_packets'.  Skipping.")

#            sys.exit()

#  Need to handle different allocation types for request_project_create.
#  The AllocationType just be one of new, renewal, extension, supplement, transfer, advance or adjustment.
#
#   * new        - A new type creates a new project and creates a new allocation on a specific resource.
#   renewal    - A renewal is a continuing award for an existing project. In a sense, it "replaces" the 
#                previous year's allocation (as of the start date of the renewal). A renewal may be sent for a 
#                machine that did not previously have an allocation within the project, since a PI can renew 
#                onto different machines each year. In this case, the project may be unknown to the RP. A 
#                renewal for a resource not previously within the project should be handled by the RP as a 
#                new type, using the provided start/end dates.  
#                One can renew an allocation onto a new resource which makes it functionally new.
#   * supplement - A supplement adds SUs to an existing allocation (without changing the start or end dates). 
#                A supplement can also be received for a project unknown to the RP or a machine not 
#                previously on the project, in which case it should be handled as a new type, using the 
#                provided SUs and start/end dates. 
#   * transfer   - A transfer type occurs as one of a pair of transactions – one negative, and one positive. A 
#                negative transfer for an unknown allocation is an error. Otherwise, the amount is deducted 
#                from the allocation. A positive transfer should be treated the same as a supplement. 
#   * extension  - An extension extends the end date of an allocation. An extension should only be received 
#                for an existing allocation.
#   * advance    - An advance is an allocation given in anticipation of a new or renewal allocation. It should 
#                be treated as a supplement.
#   adjustment - An adjustment modifies an existing allocation for other reasons. It should be handled as a 
#                transfer type; but unlike a transfer, there is only one transaction. Currently an adjustment 
#                transaction is used to deduct usage charged against an advance from a renewal allocation.
#                handled as a new type, using the provided SUs and start/end dates.

            print("Starting ALLOCATION block.")
            print("  allocation_type = ",allocation_type," new_proxy = ",new_proxy)

            print("new_proxies = ",new_proxies)
            print("new_proxy = ",new_proxy)
            print("allocation_type = ",allocation_type)
#            sys.exit()

#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            if allocation_type == 'new' or (allocation_type in new_proxies and new_proxy == 1):
#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
                print("Processing RPC with allocation type '",allocation_type,"' as 'new' type.")
#                print("Request from PI ",first_name," ",last_name," with email ",email," global ID ",pi_global_id," and grant no. ",grant_number)
#            print(" sua = ",service_units_allocated)
                person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + pi_global_id
                remote_site_login = person_id
#                project_id = 'p.' + grant_number.lower() + '.' + cluster[:3]
                project_id = 'p.' + grant_number.lower() + '.000'
                proj_stat = 'pending'
                acct_stat = 'pending'
# --------------------------------------------------------------------------------------------
#  Must check all previous random strings to avoid duplicate.
#  Select all 'slurm_acct' entries for PIs, strip out random number parts, and compare to new random number.
                try:
                    newrand = slurm_rand()
# Changed selection of PIs from person_id = pi.* to pi = 'Y'.
                    sql = "SELECT slurm_acct FROM local_info WHERE pi = 'Y'"
                    data = ()
                    results = []
                    amiedb_call(sql,data,script_name,results)
# Deal with initial condition of no previous 'slurm_acct' numbers.
                    if len(results) != 0:
                        prn = []
                        for slno in results[0]:
                            prn.append(int(slno[-4:]))
                        no_match = 1
                        while no_match == 1:
                            if newrand in prn:
                                no_match = 1
                                newrand = slurm_rand()
                            else:
                                no_match = 0
                    if cluster == 'faster':
                        cluster_id = '14'
                    elif cluster == 'aces':
                        cluster_id = '15'
                    elif cluster == 'launch':
                        cluster_id = '16'
                    else:
                        print(" Unknown cluster: ",cluster)
                        continue
                    slurm_acct = cluster_id + str(int(pi_global_id) + 500000) + newrand
                except Exception as e:
                    print(" Problem with creating random string for slurm_acct number.")
                    print(" ERROR: ",str(e))
# ---------------------------------------------------------------------------------------------

                print("CREATED slurm_acct NUMBER = ",slurm_acct)

#  Find out if this project_id already exists in 'local_info'.
#  Look for unique project_id/person_id/pi/cluster combination in 'local_info'.
#  The PI could have more than one project, so we also have to include the 'project_id'.
                print("person_id = ",person_id," cluster = ",cluster)
                sql = "SELECT * FROM local_info WHERE project_id = %s AND person_id = %s AND pi = 'Y' and cluster = %s"
                data = (project_id,person_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                project_id_exists = len(results)
                print("project_id_exists = ",project_id_exists," project_id = ",project_id)
                if project_id_exists == 0:
                    print("Project for packet ",trans_rec_id," for PI ",person_id," on project ",project_id," on cluster ",cluster," does not exist in 'local_info'. Will create.")
                else:
                    print("Project for packet ",trans_rec_id," for PI ",person_id," on project ",project_id," on cluster ",cluster," does exist in 'local_info'. Skipping.")
#                sys.exit()

#  If 'project_id' isn't already in 'local_info', create the appropriate UID and GID.
#
#  1.  Find out if this person_id is already in 'local_info'.
#  2.  Find combined maximum of 'uid' and 'gid' columns.
#  3.  Set gid as that maximum plus one.
#  4.  Set uid as:
#    a.  The new gid plus one if the 'person_id' isn't in 'local_info'.
#    b.  The previously set uid for this 'person_id' if they are already in 'local_info'.

                if project_id_exists == 0:
#  Find if this person already exists in 'local_info'.
                    sql = "SELECT * FROM local_info WHERE person_id = %s"
                    data = (person_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    person_id_exists = len(results)
                    try:
#  For new project, find combined max of UID and GID columns.
#  The next GID will be MAX+1, and the PI UID will be MAX+2.
#  Find maximum UID in local_info.
                        sql = "SELECT MAX(uid) FROM local_info;"
                        data = ()
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        max_uid = results[0][0]
#  Find maximum GID in local_info.
                        sql = "SELECT MAX(gid) FROM local_info;"
                        data = ()
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        max_gid = results[0][0]
#  Deal with DB initial condition.
                        if max_uid == None:
                            max_uid = 50000
                        if max_gid == None:
                            max_gid = 50000
#  Find maximum of GID and UID to set next GID.
                        maxid = max(max_uid,max_gid)
                        gid = maxid + 1
#  Next UID value depends on if this 'person_id' is already in 'local_info'.
#  If person_id isn't already in 'local_info', set the UID as GID + 1.
                        if person_id_exists == 0:
#  Set next GID value.
                            uid = gid + 1
#  If person_id is already in 'local_info', find their UID and set this UID as the same one.
#  This might grab multiple results, but all should have the same UID.
                        else:
                            sql = "SELECT uid FROM local_info WHERE person_id = %s"
                            data = (person_id,)
                            results = []
                            amiedb_call(sql,data,script_name,results)
                            uid = results[0][0]
#  Set the 'pi' column to 'Y' for this PI.
                        pi = 'Y'
                    except Exception as e:
                        print(" ACCESS ERROR: Problem creating UID and/or GID for new project. Skipping packet.")
                        print(" ERROR: ",str(e))
                        continue
#  Create 'local_info' entry for this RPC packet.
                    print("BEFORE local_info")
#                    sys.exit()
                    try:
                        if test == 0:
                            print("Inserting 'local_info' entry.")
                            override = 'N'
                            sql = "INSERT INTO local_info (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                            data = (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units_allocated,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override)
                            results = []
                            amiedb_call(sql,data,script_name,results)
                        else:
                            print("TESTING:  Insertion into 'local_info' not done for test run.")
#  Email notification of request_project_create packet.
                        msg1 = "Approval required for " + type_id + " packet for PI " + first_name + " " + last_name + " (" + str(email) + ")."
                        msg2 = "The local ID is " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + "."
                        send_email(msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    except Exception as e:
                        print(" type_id = ",type_id)
                        print(" first_name = ",first_name)
                        print(" last_name = ",last_name)
                        print(" email = ",email)
                        print(" person_id = ",person_id)
                        print(" project_id = ",project_id)
                        msg1 = "Error adding " + type_id + " packet for PI " + first_name + " " + last_name + " (" + email + ") with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + " to 'local_info' table.  Skipping packet."
                        msg2 = "Error message: " + str(e)
                        if test == 0:
                            send_email(msg1 + "\r\n" + msg2)
                            print(msg1 + "\r\n" + msg2)
                            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        else:
                            print("TESTING:  Email message not sent for test.  Message would be:")
                            print(msg1 + "\r\n" + msg2)
                        continue
#                else:
#                        print(" Project ",project_id," under ",person_id," for resource ",cluster," already exists in 'local_info'.  Skipping packet.")
#                        continue

#                print("Exiting after creating 'local_info' entry.")
#                sys.exit()
#  The 'extension' allocation type extends the end date of an allocation, so only 'end_date' is modified.
#  For an extension the service units allocated should not be changed.

#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            elif allocation_type == 'extension':
#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

#   extension  - An extension extends the end date of an allocation. An extension should only be received 
#                for an existing allocation.

                new_end_date = end_date
#  Attempt to find 'end_date' for 'grant_number' from 'local_info'.
                sql = "SELECT end_date,acct_stat FROM local_info where person_id = %s AND project_id = %s AND cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  Process 'extension' request if account exists locally.
                if len(results) != 0:
                    (previous_end_date,acct_stat) = results[0]
#                    previous_end_date = results[0][0]
                    msg1 = "Request to extend the end date of project " + project_id + " under " + person_id + " on " + cluster + " from " + str(previous_end_date) + " to " + str(new_end_date) + " requires approval."
                    if acct_stat == 'inactive':
                        msg2 = "The project status is 'inactive', therefore this is also a proxy for 'request_project_reactivate'."
                    else:
                        msg2 = "The project status is 'active'."
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    else:
                        print("TESTING:  Not sending email about approving 'extension' allocation type.  Message would be:")
                        print(msg1)
#  Skip request if a local account does not exist.
                else:
                    msg1 = "No entry in 'local_info' for RPC 'extension' request for " + person_id + " on " + project_id + " on " + cluster + "."
                    msg2 = "Cannot extend account that does not exist.  Skipping packet with trans_rec_id " + trans_rec_id + "."
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING:  Not sending email about problematic 'extension' allocation type. Message would be:")
                        print(msg1 + "\r\n" + msg2)
                    continue

#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            elif allocation_type == 'transfer' or 'supplement' or 'advance' and new_proxy == 0:
#            elif allocation_type == 'transfer' or allocation_type == 'supplement' or allocation_type == 'advance' and new_proxy == 0:
#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

                print("Entering transfer/supplement/advance with new_proxy = 0.")
                print("allocation_type = ",allocation_type," new_proxy = ",new_proxy)

#  transfer   - A transfer type occurs as one of a pair of transactions – one negative, and one positive. A 
#                negative transfer for an unknown allocation is an error. Otherwise, the amount is deducted 
#                from the allocation. A positive transfer should be treated the same as a supplement, which
#                adds SUs to an existing allocation (without changing the start or end dates).
#  supplement - A supplement adds SUs to an existing allocation (without changing the start or end dates).
#                A supplement can also be received for a project unknown to the RP or a machine not
#                previously on the project, in which case it should be handled as a new type, using the
#                provided SUs and start/end dates.
#   advance    - An advance is an allocation given in anticipation of a new or renewal allocation. It should 
#                be treated as a supplement.

#  Check if this already exists in approval table.
                sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                in_approval = len(results)
                print("in_approval = ",in_approval,"results = ",results)

#  Check if this account is already in 'local_info'.
                sql = "SELECT * FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                in_local_info = len(results)

#  Check if this is already in 'approval' table.
                if in_approval == 0:
                    print("in_approval = ",in_approval)

#  Find the previous SU allocation for this non-proxy transfer request.
                    sql = "SELECT first_name,last_name,email,service_units,start_date,end_date,access_id FROM local_info WHERE person_id = %s AND project_id = %s and cluster = %s"
                    data = (person_id,project_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    try:
                        (first_name,last_name,email,previous_units,previous_start_date,previous_end_date,access_id) = results[0]
                    except Exception as e:
                        msg1 = "Problem extracting from 'local_info' for RPC 'transfer' request for " + person_id + " on project " + project_id + " on cluster " + cluster + "."
                    print("previous_units = ",previous_units)

                    transfer_units = service_units_allocated

#  If the transfer amount is positive, send message about this and previous amounts as well as new and old start and end dates.
                    if float(service_units_allocated) > 0.0:
                        msg1 = "RPC " + allocation_type + " request to add " + str(transfer_units) + " SU requires approval."
#                        msg1 = "RPC " + allocation_type + " request to add " + str(transfer_units) + " SU to previous allocation of " + str(previous_units) + " SU requires approval."
                        msg2 = "Request for PI " + first_name + " " + last_name + "(" + email + ") with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on cluster " + cluster + "."
#                        msg3 = "Previous start and end dates: " + str(previous_start_date) + " - " + str(previous_end_date) + "."
                        msg4 = "Start and end dates in packet: " + str(start_date) + " - " + str(end_date) + "."
                        email_message = msg1 + "\r\n" + msg2 + "\r\n" + msg4
#                        email_message = msg1 + "\r\n" + msg2 + "\r\n" + msg3 + "\r\n" + msg4
                        if allocation_type == 'advance':
                            msg5 = "This is an 'advance' given in anticipation of a new or renewal allocation, and is treated as a 'supplement'."
                            email_message = email_message + "\r\n" + msg5
                        if test == 0:
                            print(email_message)
                            send_email(email_message)
                            logging.info(email_message)
                        else:
                            print("TESTING:  Not sending email about approving ",allocation_type," allocation type. The message sent would be:")
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3 + "\r\n" + msg4)

#  If the transfer amount is negative and no previous account, send email error mesage and skip to next packet.
                    else:
                        if in_local_info == 0:
                            msg1 = "Could not add " + str(transfer_units) + " SU in RPC " + allocation_type + " request to non-existent local account for " + first_name + " " + last_name + " on grant " + grant_number + ". Skipping packet."
                            if test == 0:
                                send_email(msg1)
                                print(msg1)
                                logging.info(script_name + ": " + msg1)
                            else:
                                print(" TESTING:  Not sending email about denied negative transfer for 'transfer/supplement/advance' allocation type. The message will be:")
                                print(msg1)
                            continue
#  If the transfer amount is negative and previous account, send message about this amount and previous amount.
                        else:
                            msg1 = "RPC " + allocation_type + " request to subtract " + str(transfer_units) + " SU requires approval."
#                            msg1 = "RPC " + allocation_type + " request to add " + str(transfer_units) + " SU from account with previous allocation of " + str(previous_units) + " SU requires approval."
                            msg2 = "Request for PI " + first_name + " " + last_name + "(" + email + ") with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + "."
                            if test == 0:
                                print(msg1 + "\r\n" + msg2)
                                send_email(msg1 + "\r\n" + msg2)
                                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                            else:
                                print("TESTING:  Not sending email about negative transfer for 'transfer/supplement/advance' allocation type. The message would be:")
                                print(msg1 + "\r\n" + msg2)

                else:
                    print("This ",type_id," packet with trans_rec_id ",trans_rec_id," is already in the 'approval' table.  Skipping.")

#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            elif allocation_type == 'renewal' and new_proxy == 0:
#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
                print(" Processing RPC with 'renewal' allocation type for existing project '",project_id,"' under '",person_id,"' on resource '",cluster,"'.")

#  A different start date represents a different allocation.  This will happen after a project has gotten a renewal.
#  You could see a transfer with a new start date that represents a new allocation and should not be added to the
#  previous allocation.  The other project information is always the latest available when the packet is generated
#  and should be updated locally as needed.

#  For transfer, supplement , and adjustment packets the dates don't change and you add the service units
#  allocated to the current allocation with the same start date.

#            new_proxies = ['renewal','supplement','advance','transfer']

#   renewal    - A renewal is a continuing award for an existing project. In a sense, it "replaces" the
#                previous year's allocation (as of the start date of the renewal). A renewal may be sent for a
#                machine that did not previously have an allocation within the project, since a PI can renew
#                onto different machines each year. In this case, the project may be unknown to the RP. A
#                renewal for a resource not previously within the project should be handled by the RP as a
#                new type, using the provided start/end dates.
#                One can renew an allocation onto a new resource which makes it functionally new.

#  PROBLEM:  What do we do if a 'renewal' request is sent before the end date of the previous allocation?
#            It replaces the previous allocation as of the start date of the renewal.  How do we delay overwriting
#            the service units column value if the new start date has not yet arrived?
#            Add a 'notify' column to 'rpc_packets' and initialize it with 'Y', which is changed to 'N' in 'respond.py' after
#            the initial notification of delay message is sent.  No additional notification will be sent until the
#            arrival of the new start date.

#  Find if this already exists in approval table.
                sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                in_approval = len(results)

                if in_approval == 0:
#  Extract date portion of new allocation start date.
                    new_start_date = start_date.date()
#  Obtain today's date.
                    today = date.today()
#  If new allocation start date is today or before today, process.
                    msg1 = " An RPC 'renewal' packet for " + person_id + " on project " + project_id + " on resource " + cluster + " is enqueued."
                    if new_start_date <= today:
                        msg2 = " New start date " + str(new_start_date) + " is earlier than or equal to today's date " + str(today) + ".  Ready for approval."
#  If new allocation start date is later than today, put an entry in 'rpc_packets', send an email, and wait until the start date arrives.
                    else:
                        msg2 = " New start date " + str(new_start_date) + " is later than today " + str(today) + ". Postponing processing even upon approval."
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING: No email sent about approving RPC 'renewal' packet.  The message will be:")
                        print(msg1 + "\r\n" + msg2)
                else:
                    print("This ",type_id," packet with trans_rec_id ",trans_rec_id," is already in the 'approval' table.  Skipping.")


#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
            else:
#  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$

                print(" The PI ",person_id," on project ",project_id," on resource ",cluster," already exists.  This is a proxy reactivation request for an inactive project.")
                if diag > 0:
                    print(" DUMP_APPROVALS: The 'local_info' table already contains ",person_id," with project_id ",project_id,". Skipping insertion.")

##########################################################################
#  PACKET:  REQUEST_ACCOUNT_CREATE
##########################################################################
        elif type_id == 'request_account_create':

# -----------------------------------------
#  Extract available packet information.
# -----------------------------------------
            first_name = packet.UserFirstName 
            last_name = packet.UserLastName
#  Extract the PersonID for the SitePersonId dictionary with the X-PORTAL key value.
            site_person_id = packet.SitePersonId
            access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']
            user_global_id = packet.UserGlobalID
            email = packet.UserEmail
            resource_list = packet.ResourceList[0]
            cluster = resource_list.split('.')[0]
            grant_number = json_extract(pdict,'GrantNumber')[0]
#  Create the 'person_id' since it is not in the 'request_account_create' packet.
            person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + user_global_id
            remote_site_login = person_id
#  The 'project_id' should be in the packet
            project_id = packet.ProjectID
#  Changing project_id suffix from whatever to .000 to handle the fallout from a failed experiment.
            if project_id.split(".")[2] != '000':
                new_proj_id = project_id.split(".")[0] + "." + project_id.split(".")[1] + ".000"
                print("Changing 'project_id' from ",project_id," to ",new_proj_id,".")
                project_id = new_proj_id
####################
#  GLOBALID CHECK  #
####################
#  Create the 'project_id' again as a check against the one in the packet.
#  Changed this on Mar. 4, 2024.

            acct_stat = 'pending'
#            print("Request from ",first_name," ",last_name," with local ID ",person_id," on project ",project_id," with ACCESS ID ",access_id," on resource ",cluster,".")

####################
#  GLOBALID CHECK  #
####################
#            gidcheck = person_id[4:]
#            if gidcheck != user_global_id:
#                print("Global ID in packet ",user_global_id," not equal to global ID in 'local_info' ",gidcheck,". Skipping.")
#                continue
#            print(cluster,project_id,person_id,gidcheck)
#
##  Check for UserGlobalID/user_global_id already in 'local_info'.  Send email if already there.
#            sql = "SELECT * FROM local_info WHERE cluster = %s AND project_id = %s AND person_id = %s"
##            sql = "SELECT first_name,last_name,email,person_id,project_id,access_id,override FROM local_info WHERE cluster = %s AND project_id = %s AND person_id = %s"
##
##            sql = "SELECT first_name,last_name,email,person_id,project_id,access_id,override FROM local_info WHERE cluster = %s AND project_id = %s AND person_id LIKE '%%{}'".format(user_global_id)
#            data = (cluster,project_id,person_id,)
#            results = []
#            amiedb_call(sql,data,script_name,results)
##            print("GLOBALID CHECK:")
##            print(results)
##            print(len(results))
##            sys.exit()
##            (t_fn,t_ln,t_email,t_person,t_project,t_access,override) = results[0]
#            if len(results) != 0:
#                msg1 = "ACCESS WARNING: Global ID " + user_global_id + " in RAC packet already in 'local_info' " + str(len(results)) + " times for project "+project_id+" on "+cluster+". Skipping packet."
##                msg2 = " Table: " + t_fn + "  " + t_ln + "  " + t_email + "  " + t_person + "  " + t_project + "  " + t_access
#                msg2 = "Packet: " + first_name + "  " + last_name + "  " + email + "  " + person_id + "  " + project_id + "  " + access_id
#                print(msg1 + "\r\n" + msg2)
#                send_email(msg1 + "\r\n" + msg2)
#                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
##                sys.exit()
#                continue

#            print("Global check shows that person not already in 'local_info'.  Proceeding.")
##            sys.exit()

####################
#  OVERRIDE CHECK  #
####################
#  Check if user in RAC packet is flagged as suspended, send an email message about the attempt, change the override value from
#  D to DD to prevent further emails, and continue to next packet.
#            if override == 'D':
#                msg1 = "Previous user " + person_id + " on project " + project_id + " on suspended list has requested account reactivation."
#                msg2 = "They will be flagged to be permanently skipped with no further email messages."
#                print(msg1 + "\r\n" + msg2)
#                send_email(msg1 + "\r\n" + msg2)
#                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
#                sql = "UPDATE local_info SET override = 'DD' where person_id = %s"
#                data = (person_id,)
#                results = []
#                continue
#                amiedb_call(sql,data,script_name,results)
#  If a notification email has already been sent, continue to next packet with no further email message.
#            elif override == 'DD':
#                continue

#  Check for person_id/project_id/cluster combination in local_info.
            sql = "SELECT * FROM local_info WHERE person_id = %s and project_id = %s and cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            already_in_local_info = len(results)
            print("person_id = ",person_id," project_id = ",project_id," cluster = ",cluster)
            print("already_in_local_info = ",already_in_local_info)
#            print(" ACCOUNT: already_in_local_info = ",already_in_local_info)
#
#  This is either a request for a new person_id/project_id/cluster account or a proxy request to reactivate an already existing account.
#
#  If person_id/project_id/cluster combination not in local_info, add another entry for the different person_id/project_id/cluster combination.
            if already_in_local_info == 0:
#  Find out if this person_id is already in 'local_info' on another project and/or resource.
                sql = "SELECT * FROM local_info WHERE person_id = %s"
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                additional_person_id_entry = len(results)
#                print("results = ",results)
#                print("additional_person_id_entry = ",additional_person_id_entry)

#  Assign both GID and UID values for this new 'local_info' entry as is appropriate.
#
#  1.  Find and assign the 'gid' of the already created project via the 'project_id' and 'cluster' combination.
#  2.  Find if this 'person_id' is already in 'local_info'.
#  3.  Assign 'uid':
#    a.  As the maximum of the uid and gid columns plus one if the 'person_id' is not in 'local_info'.
#    b.  As the previous 'uid' assigned to the 'person_id'.

                try:
#  GID ASSIGNMENT
#  Find the GID of the project via the project_id/cluster combination and assign it to this person_id.
#                    print("project_id = ",project_id," cluster = ",cluster)
################################################
#  CHECKING FOR CLUSTER MISMATCHES IN PACKETS.
################################################
                    matchcheck = 0
                    if matchcheck == 1:
#  Check for '.000' suffix on project_id.
                        if project_id.split(".")[2] != '000':
#  Check that the 'ResourceList' and 'ProjectID' values specify the same resource.
                            if project_id.split(".")[2] != cluster[3:0]:
                                msg1 = "Problem with 'request_account_create' packet for " + first_name + " " + last_name + " with ACCESS ID " + access_id + "."
                                msg2 = "The 'ResourceList' (" + cluster + ") and 'ProjectID' (" + project_id + ") values do not agree.  Skipping packet."
                                print(msg1 + "\r\n" + msg2)
#                                send_email(msg1 + "\r\n" + msg2)
#                                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                                continue
################################################
#                    sys.exit()
### EXIT
#                    sys.exit()
                    try:
                        sql = "SELECT gid from local_info WHERE project_id = %s AND cluster = %s"
                        data = (project_id,cluster,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        gid = results[0][0]
                        print("gid = ",gid)
#                        sys.exit()
                    except Exception as e:
                        print("Problem fetching gid value. Skipping.")
                        print("ERROR: ",str(e))
                        continue
#                        sys.exit()
#                    sys.exit()
#  UID ASSIGNMENT
#  If this person_id isn't in local_info, create a new UID entry.
                    if additional_person_id_entry == 0:
#  Find the maximum uid.
                        sql = "SELECT MAX(uid) FROM local_info;"
                        data = ()
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        uid_max = results[0][0]
#  Find the maximum gid.
                        sql = "SELECT MAX(gid) FROM local_info;"
                        data = ()
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        gid_max = results[0][0]
#  Find the maximum of both uid and gid.
                        maxid = max(uid_max,gid_max)
                        uid = maxid + 1
#  If the person_id already exists in local_info, the previously assigned UID for that person_id will be used again.
                    elif additional_person_id_entry != 0:
                        print(" ACCOUNT:  Using previous UID for old person_id entry with new project_id.")
                        sql = "SELECT uid FROM local_info WHERE person_id = %s"
                        data = (person_id,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        uid = results[0][0]
                        print(" ACCOUNT:  Previous UID = ",uid)
#                    print("WHOOOOOAA!!!")
### EXIT
#                    sys.exit()
#  Check to see that this uid number does not exist in the gid column.  If it does, send an email and exit.
                    sql = "SELECT * FROM local_info WHERE gid = %s"
                    data = (uid,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    if len(results) != 0:
                        msg1 = "ERROR: The 'uid' " + str(uid) + " assigned to user " + person_id + " on project " + project_id + " exists in the 'gid' column.  Skipping."
                        print(msg1)
                        send_email(msg1)
                        logging.info(script_name + ": " + msg1)
                        continue
                except Exception as e:
                    msg1 = "Problem assigning UID and/or GID to new account for user " + person_id + " in project " + project_id + " on cluster " + cluster + ". Skipping."
                    msg2 = "ERROR message: " + str(e)
                    print(msg1 + "\r\n" + msg2)
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    continue
#  Obtain the rest of the info required to populate a new 'local_info' entry.
#  Find 'slurm_acct', 'service_units', 'start_date', 'end_date' and 'proj_stat' from 'local_info' given the 'project_id' and 'cluster'.
                sql = "SELECT slurm_acct,service_units,start_date,end_date,proj_stat FROM LOCAL_INFO WHERE project_id = %s AND cluster = %s"
                data = (project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (slurm_acct,service_units,start_date,end_date,proj_stat) = results[0]
                pi = 'N'

                try:
                    if test == 0:
                        override = 'N'
                        sql = "INSERT INTO local_info (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                        data = (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override)
                        results = []
                        amiedb_call(sql,data,script_name,results)
#                        print("Inserted ",person_id," and ",project_id," into 'local_info'.")
#                        sys.exit()
                    else:
                        print(" TESTING:  Not making insertion of 'request_account_create' entry into 'local_info' for testing run.")
#  Email notification of request_account_create packet.
#  PI info added on 2023-09-01.
                    msg1 = "Approval required for '" + type_id + "' packet for user " + first_name + " " + last_name + " (" + email + ")."
                    msg2 = "The local ID is " + person_id + " on project " + project_id + " on resource " + cluster + " with Access ID " + access_id + "."
#  Extract the PI info for this project_id/cluster combination.
                    try:
                        sql = "SELECT first_name,last_name,email,access_id FROM local_info WHERE project_id = %s AND cluster = %s AND pi = 'Y'"
                        data = (project_id,cluster,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        (pi_first_name,pi_last_name,pi_email,pi_access_id) = results[0]
                        msg3 = "The PI is " + pi_first_name + " " + pi_last_name + " (" + pi_email + ") with ACCESS ID " + pi_access_id + "."
                        pi_info = 1
                    except Exception as e:
                        print("PI information extraction problem. Not including in 'request_account_create' message.")
                        print("ERROR: ",str(e))
                        pi_info = 0
                    if test == 0:
                        if pi_info == 0:
                            send_email(msg1 + "\r\n" + msg2)
                            print(msg1 + "\r\n" + msg2)
                            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        else:
                            send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                            logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                    else:
                        print(" TESTING: No 'local_info' insertion or email about RAC packet. Email message would be:")
                        print(msg1)
#  If there is an error adding this user to 'local_info', send a message informing of the error and its details, and continue to next packet.
                except Exception as e:
                    msg1 = "Error adding " + type_id + " packet for user " + first_name + " " + last_name + " (" + email + ") with local ID " + person_id + " on project " + project_id + " on resource " + cluster + " with Access ID " + access_id + " to 'local_info' table.  Skipping to next packet."
                    msg2 = "Error message: " + str(e)
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING:  Not sending error message email about RAC packet.  Email message would be:")
                        print(msg1 + "\r\n" + msg2)
                    print(" Skipping to next packet.")
                    continue

#  PROXY REQUEST HANDLING   
#  If a person_id/project_id/cluster combination is already in 'local_info' and 'acct_stat' set to 'inactive' (or 'active'), process a proxy request.
            else:
#  Reset the 'approval' table column 'supplemental' to 'reactivate' if this is a proxy request.
                sql = "UPDATE approval SET supplemental = 'reactivate' WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  Check the account status (acct_stat) if the user is already in 'local_info'.  If it is 'inactive', this is a proxy request to reactivate.
                sql = "SELECT acct_stat,proj_stat FROM local_info WHERE person_id = %s and project_id = %s and cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                acct_stat = results[0][0]
                proj_stat = results[0][1]
                print("acct_stat = ",acct_stat," proj_stat = ",proj_stat)
                msg1 = "Approval required for " + type_id + " packet for user " + first_name + " " + last_name + " (" + email + ")."
                msg2 = "Local user ID is " + person_id + " on project " + project_id + " on resource " + cluster + " with Access ID " + access_id + "."
#  If the project is set to 'active' we can change the account status to 'active'.
                if proj_stat == 'active':
#  If the account status is 'inactive' send an email to request approval.
                    if acct_stat == 'inactive':
                        msg3 = "This is a proxy 'request_account_reactivate' request for an existing account that is presently set to 'inactive'."
                        msg4 = "Approval will set this account to 'active'."
#  If the account status is 'active' send an email to request approval along with a warning that something may be wrong.
                    else:
                        msg3 = "This is a proxy 'request_account_reactivate' request for an existing account that is already set to 'active'."
                        msg4 = "Approval will reset this account to 'active', although this redundancy may indicate some sort of problem."
#  If the project is set to 'inactive' we will and can not change the account status to 'active'.
                else:
                    msg3 = "This is a proxy 'request_account_reactivate' request for an existing account."
                    msg4 = "PROBLEM: The project is presently set to 'inactive' so the account will not be reactivated.  This packet will be skipped and sit in limbo forever."
#                    continue
                if test == 0:
#TEMPO                    send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3 + "\r\n" + msg4)
                    print(msg1 + "\r\n" + msg2+ "\r\n" + msg3 + "\r\n" + msg4)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3 + "\r\n" + msg4)
                else:
                    print(" TESTING: No proxy 'request_account_reactivate' request information sent. Email message would be:")
                    print(msg1 + "\r\n" + msg2 + "\r\n" + msg3 + "\r\n" + msg4)

#######################################################
#  PACKET:  REQUEST_USER_MODIFY
#######################################################

#  QUESTION:  DO WE NEED TO MODIFY ALL person_id/project_id COMBINATIONS FOR THIS person_id?
#  There is no ProjectID or ResourceList in 'request_user_modify' packets.

        elif type_id == 'request_user_modify':

            trans_rec_id_check = packet.trans_rec_id
            packet_rec_id_check = packet.packet_rec_id
            if trans_rec_id_check != trans_rec_id:
                print("The 'trans_rec_id' values don't match: ",trans_rec_id_check," vs ",trans_rec_id,".")
            if packet_rec_id_check != packet_rec_id:
                print("The 'packet_rec_id' values don't match: ",packet_rec_id_check," vs ",packet_rec_id,".")

#  TEMPORARY OUTPUT FILE TO DIAGNOSE USER MODIFY PROBLEM.
            fmod = open("mod-" + str(trans_rec_id) + ".log", "w")

#  Extract the 'person_id' from the packet.
            person_id = packet.PersonID
#  The resource/cluster is not available in RUM packets, so we must set it as 'NA'.
            cluster = 'NA'
#  First, check if this packet has already been processed by 'dump_approvals.py'.
            try:
                sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  Packet already processed and in 'approval' table.  Skip.
                if len(results) != 0:
                    print(" Packet info for trans_rec_id ",trans_rec_id," for user ",person_id," already processed.  Skipping.")
                    continue
#  Problem finding packet in 'approval' table.  Skip.
            except Exception as e:
                print("Problem finding 'trans_rec_id' ",trans_rec_id," in 'approval' table. Skipping.")
                continue

#  Process if not already in 'approval' table.
            if len(results) == 0:
#  Extract 'access_id' from 'local_info' for informational purposes.
                sql = "SELECT access_id FROM local_info WHERE person_id = %s"
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                access_id = results[0][0]
                print(" access_id = ",access_id)
#                sys.exit()
#  Ascertain which parts of the packet 'body' section are to be changed.
#  Extract the body portion of the packet wherein modifications are performed.
                body = packet._original_data['body']
                print(" body = ",body)
#                sys.exit()FRED
                print("**********************************")
                print(" List of 'data_tbl' matches for 'request_user_modify' body keys for user ",person_id,".")
                print("**********************************")
                fmod.write("**********************************\n")
                fmod.write(" List of 'data_tbl' matches for 'request_user_modify' body keys for user " + person_id + ".\n")
                try:
                    fmod.write(" The 'trans_rec_id = " + str(trans_rec_id) + " and 'packet_rec_id'  = " + str(packet_rec_id))
                except:
                    fmod.write("Either 'trans_rec_id' or 'packet_rec_id' not available to print.")
                fmod.write("**********************************\n")
#  Loop over all the keys in the body portion.
                no_of_changes = 0
                for key in body:
#                    print(" key = ",key)
#  Only check the key values that are strings.  No lists, tuples or dictionaries allowed.
                    if (isinstance(body[key],str)):
#  Extract the new value for this key.
                        new_value = body[key]
#                        print("new value = ",new_value)
#  Extract the old value for this key.
                        sql = "SELECT value FROM data_tbl WHERE person_id = %s AND tag like %s"
                        data = (person_id,'%' + key,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
#                        print("results = ",results)
#  Find out if the key in the packet has a match in 'data_tbl`.
                        does_key_match = len(results)
#                        print("does_key_match = ",does_key_match)
#  If the same key is in both the packet and 'data_tbl', print them both to the change table.
                        if does_key_match != 0:
#  Compare the old and new value for each key.
                            try:
                                old_value = results[0][0]
                                print(" key = ",str(key)," new value = ",str(new_value)," old value = ",str(old_value))
                                fmod.write(" key = " + str(key) + " new value = " + str(new_value) + " old value = " + str(old_value) + "\n")
                            except Exception as e:
#                                print(" Tag '",key,"' not found in 'data_tbl'. Skipping.")
                                continue
                            subtag = 'NULL'
                            seq = 0
#  If the packet key has no match in 'data_tbl', set 'old_value' = NA.
                        else:
                            old_value = 'NA'
                            subtag = 'NULL'
                            seq = 0
                            print(" key = ",str(key)," new value = ",str(new_value)," old value = ",str(old_value))
                            fmod.write(" key = " + str(key) + " new value = " + str(new_value) + " old value = " + str(old_value) + "\n")
#  Store changed key values in 'data_change_tbl' table for use by 'approvals.py'.
                        if new_value != old_value or old_value == 'NA':
                            no_of_changes = no_of_changes + 1
                            print(" CHANGE: ",str(key)," has changed from ",str(old_value)," to ",str(new_value),".")
                            fmod.write(" ***CHANGE " + str(key) + " has changed from " + str(old_value) + " to " + str(new_value) + ".\n")
                            if test == 0:
                                try:
#body =  {'ActionType': 'replace', 'PersonID': 'u.sm122876', 'BusinessPhoneNumber': '6469619913', 'FirstName': 'Shashi', 'LastName': 'Mishra', 'NsfStatusCode': 'PD', 'Organization': 'SUNY at Binghamton', 'OrgCode': '0028365', 'Country': '9US', 'Email': 'smishra9@binghamton.edu', 'AcademicDegree': [{'Field': 'Physics', 'Degree': 'doctorate'}]}
#                                    tvals = str(trans_rec_id) + " " + str(packet_rec_id) + " " + person_id + " " + str(key) + " " + subtag + " " + str(seq) + " " + str(old_value) + " " + str(new_value) + " " + tstamp()
#                                    print(tvals)
#                                         117089387 234295525 u.sm122876 ActionType NULL 0 NA replace 2025-02-12 09:18:02
                                    sql = "INSERT INTO data_changes_tbl (trans_rec_id,packet_rec_id,person_id,tag,subtag,seq,old_value,new_value,ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                    data = (trans_rec_id,packet_rec_id,person_id,key,subtag,seq,old_value,new_value,tstamp())
                                    results = []
                                    amiedb_call(sql,data,script_name,results)
#                                    sys.exit()
                                except Exception as e:
                                    msg1 = "ACCESS ERROR: Problem inserting 'request_user_modify_entry' into 'data_changes_tbl'."
                                    msg2 = "Transaction " + str(trans_rec_id) + " for " + str(person_id) + " on " + project_id + " with ACCESS ID " + access_id + "."
                                    msg3 = "ERROR: ",str(e)
                                    if test == 0:
                                        send_email(msg1 + "\r\n" + msg2+ "\r\n" + msg3)
                                        print(msg1 + "\r\n" + msg2+ "\r\n" + msg3)
                                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2+ "\r\n" + msg3)
                                    else:
                                        print(" TESTING:  Not sending notice of 'request_user_modify' problem.  The notice would be:")
                                        print(msg1 + "\r\n" + msg2+ "\r\n" + msg3)
                                    continue
                            else:
                                print("Test run.  Not inserting changes into 'data_changes_tbl' for trans_rec_id = ",trans_rec_id,".")

                print("**********************************")
                print(" The 'request_user_modify' packet contained ",no_of_changes," changes to the user's 'data_tbl' information.")
                print("**********************************")
#                sys.exit()
#  Send message about user modification request.
                msg1 = "Approval required for " + type_id + " request for " + person_id + " with access ID " + access_id + "."
                if test == 0:
                    send_email(msg1)
                    print(msg1)
                    logging.info(script_name + ": " + msg1)
                else:
                    print(" TESTING:  Not sending notice to approve 'request_user_modify' request.  The notice would be:")
                    print(msg1)
#  Send message about some error bollixing up the processing of this packet.
#        except Exception as e:
#            msg1 = "ACCESS ERROR processing " + type_id + " packet for user " + person_id + " with trans_rec_id " + str(trans_rec_id) + ". Skipping."
#            msg2 = "ERROR: " + str(e)
#            if test == 0:
##                send_email(msg1 + "\r\n" + msg2)
#                print(msg1 + "\r\n" + msg2)
#                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
#            else:
#                print(" TESTING:  Not sending notice of 'request_user_modify' problem.  The notice would be:")
#                print(msg1 + "\r\n" + msg2)
#            continue

########################################################
#  PACKET:  REQUEST_PERSON_MERGE
########################################################
#
#  The packet contains:
#    KeepGlobalID - the global ID to be retained
#    KeepPersonID - the local person_id to be retained
#    KeepPortalLogin - the local remote_site_login to be retained
#    DeleteGlobalID - the global ID to be deleted
#    DeletePersonID - the local person_id to be deleted
#    DeletePortalLogin - the local remote_site_login to be deleted
# MERGE

#{"DATA_TYPE": "Packet", "type": "request_person_merge", "body": {"KeepGlobalID": "23948", "KeepPersonID": "u.mr23948", "DeleteGlobalID": "314262", "DeletePersonID": "u.mr314262", "KeepPortalLogin": "rahmanmd", "DeletePortalLogin": "mrahman11"}, "header": {"packet_rec_id": 234634602, "packet_id": 0, "transaction_id": 10812, "trans_rec_id": 117181153, "expected_reply_list": [{"type": "inform_transaction_complete", "timeout": 30240}], "local_site_name": "TGCDB", "remote_site_name": "TAMU", "originating_site_name": "TGCDB", "outgoing_flag": 1, "transaction_state": "in-progress", "packet_state": "in-progress", "packet_timestamp": "2025-08-05T08:01:58.467Z"}}

        elif type_id == 'request_person_merge':
            print("Skipping 'request_person_merge' packet for now.")
            continue

            print("Processing ",type_id," request to merge different IDs for the same person.")
            keep_person_id = packet.KeepPersonID
#  Set 'person_id' as 'keep_person_id' for 'approval' table entry.
            person_id = keep_person_id
            delete_person_id = packet.DeletePersonID
            keep_global_id = packet.KeepGlobalID
            delete_global_id = packet.DeleteGlobalID
            keep_portal_login = packet.KeepPortalLogin
            delete_portal_login = packet.DeletePortalLogin

# {"DATA_TYPE": "Packet", "type": "request_person_merge", "body": {
#  "KeepGlobalID": "23948",
#  "KeepPersonID": "u.mr23948",
#  "DeleteGlobalID": "314262",
#  "DeletePersonID": "u.mr314262",
#  "KeepPortalLogin": "rahmanmd",
#  "DeletePortalLogin": "mrahman11"},\
#"header": {"packet_rec_id": 234634602, "packet_id": 0, "transaction_id": 10812, "trans_rec_id": 117181153, "expected_reply_list": [{"type": "inform_transaction_complete", "timeout": 30240}], "local_site_name": "TGCDB", "remote_site_name": "TAMU", "originating_site_name": "TGCDB", "outgoing_flag": 1, "transaction_state": "in-progress", "packet_state": "in-progress", "packet_timestamp": "2025-08-05T08:01:58.467Z"}}

            try:
#  Check if already in 'approval' table.
                sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                if len(results) == 0:
#  Check if there are entries in 'local_info' for both person_id/portal_login sets.
#  The global_id values are part of the person_id values and don't need to be separately checked.
                    sql = "SELECT * FROM local_info WHERE person_id = %s AND access_id = %s"
                    data = (keep_person_id,keep_portal_login,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    no_keep = len(results)
                    if no_keep != 0:
                        msg2 = "There are " + str(no_keep) + " entries to be retained in 'local_info'."
                        retain = 1
                    else:
                        retain = 0

                    sql = "SELECT * FROM local_info WHERE person_id = %s AND access_id = %s"
                    data = (delete_person_id,delete_portal_login,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    no_delete = len(results)
                    if no_delete !=0:
                        msg3 = "There are " + str(no_delete) + " entries to be deleted in 'local_info'."
                        delete = 1
                    else:
                        delete = 0
                    
#  If one or both of the usernames to be merged do not exist in 'local_info', skip processing.
                    if retain == 1 and delete == 1:
                        msg1 = "Approval required to merge local user IDs " + keep_person_id + " (keep) and " + delete_person_id + " (delete)."
                        if test == 0:
                            send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                            logging.info(script_name + ": " + msg1+ "\r\n" + msg2 + "\r\n" + msg3)
                        else:
                            print(" TESTING:  Not sending notice to approve 'request_user_merge' packet.  The notice would be:")
                            print(msg1 +  "\r\n" + msg2 + "\r\n" + msg3)
                    else:
                        msg1 = "Request to merge local user IDs " + keep_person_id + " (keep) and " + delete_person_id + " (delete)."
                        if retain == 0:
                            msg2 = "There are no entries to retain in 'local_info' for this user."
                        if delete == 0:
                            msg3 = "There are no entries to delete in 'local_info' for this user."
                        continue
                else:
                    print("Transaction ",trans_rec_id," already in 'approval' table.  Skipping.")
                    continue

            except Exception as e:
                msg1 = "Problem with " + str(type_id) + " request for user " + str(person_id) + " with trans_rec_id " + str(trans_rec_id) + ". Skipping."
                msg2 = "ERROR: " + str(e)
                continue
                if test == 0:
                    send_email(msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                else:
                    print(" TESTING:  Not sending notice of 'request_user_merge' processing problem.  The notice would be:")
                    print(msg1 + "\r\n" + msg2)
##### EXIT #####
            sys.exit()

############################################################
#  PACKET:  REQUEST_PROJECT_REACTIVATE & REQUEST_PROJECT_INACTIVATE & REQUEST_ACCOUNT_REACTIVATE & REQUEST_ACCOUNT_INACTIVATE
############################################################
#
#  These packets may not contain all of 'person_id', 'project_id' and 'resource_list', so we must set them to 'unknown' where appropriate.
#
#  request_account_inactivate - 'person_id', 'project_id', 'resource_list'
#  request_project_inactivate - 'project_id', 'resource_list'
#  request_account_reactivate - ?
#  request_project_reactivate - ?

        elif type_id == 'request_project_reactivate' or type_id == 'request_project_inactivate' or type_id == 'request_account_reactivate':
#        elif type_id == 'request_project_reactivate' or 'request_project_inactivate' or 'request_account_reactivate' or 'request_account_inactivate':
#  Check to see if account in 'request_account_reactivate' packet is suspended.
            if type_id == 'request_account_reactivate':
                sql = "SELECT acct_stat FROM local_info WHERE person_id = %s"
                person_id = packet.PersonID
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                if results[0][0] == 'suspended':
                    msg1 = "Account for " + person_id + " is suspended.  Skipping 'request_account_reactivate' packet."
                    send_email(msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    continue
#SHIVA
#  Attempt to extract 'project_id' and 'person_id' from packet.  Skip packet if problem.
            try:
#  If person_id not in packet, set to 'unknown'.
                try:
                    person_id = packet.PersonID
                except:
                    person_id = 'unknown'
#  If project_id not in packet, set to 'unknown'.
                try:
                    project_id = packet.ProjectID
                except:
                    project_id = 'unknown'
#  If resource_list not in packet, set cluster to 'unknown'.
                try:
                    resource_list = packet.ResourceList[0]
                    cluster = resource_list.split('.')[0]
                except:
                    cluster = 'unknown'
#  Need to find 'person_id' of the PI for 'request_project_*activate` since it is not included in the packet.
                if person_id == 'unknown':
                    try:
                        sql = "SELECT person_id FROM local_info WHERE project_id = %s AND cluster = %s AND pi = 'Y'"
                        data = (project_id,cluster,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        person_id = results[0][0]
#                        print("The PI ID is ",person_id," for the ",type_id," packet.")
                    except Exception as e:
                        msg1 = "Problem attempting to find required PI info for " + type_id + " packet for project " + project_id + " for trans_rec_id " + trans_rec_id + ". Skipping packet."
                        msg2 = "ERROR message: " + str(e)
                        print(msg1 + "\r\n" + msg2)
                        send_email(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        continue
            except Exception as e:
                msg1 = "Problem extracting required info 'project_id', 'person_id' and/or 'resource_list' from 'local_info' for " + type_id + " packet for trans_rec_id = " + trans_rec_id + ".  Skipping packet."
                msg2 = "ERROR message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue
#  Check for person_id/project_id/cluster combination in 'local_info' table.
            try:
                sql = "SELECT first_name,last_name,email,access_id from local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  If entry in 'local_info' table, send email message about 'request_*_*activate' packet needing approval.
                if len(results) != 0:
                    print("len_results != 0")
                    (first_name,last_name,email,access_id) = results[0]
                    print("results = ",results)
                    if access_id == None:
                        access_id = 'unknown'
                    print("access_id = ",access_id)
                    msg1 = "Approval required for '" + type_id + "' packet for " + first_name + " " + last_name + "(" + email + ")."
                    msg2 = "Local PI user " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + "."
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING:  Not sending project/account inactivation/reactivation request for approval.  Message would be:")
                        print(msg1 + "\r\n" + msg2)
#  If entry not in 'local_info', skip to next packet because of non-existent project.
                else:
                    print("This '",type_id,"' packet is for a project that doesn't exist in the 'local_info' table.  Skipping to next packet.")
#                    print(" No entry in 'local_info' for " + person_id + " on project " + project_id + " on resource " + cluster + ".  Skipping packet.")
                    continue
            except Exception as e:
                msg1 = "ACCESS ERROR: Problem processing " + type_id + " packet for user " + person_id + " on account " + project_id + " with trans_rec_id " + str(trans_rec_id) + ". Skipping packet."
                msg2 = "ERROR message: " + str(e)
                if test == 0:
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                else:
                    print(" TESTING:  Not send error message about inactivation/reactivation requests.  Message would be:")
                    print(msg1 + "\r\n" + msg2)
                continue

############################################################
#  PACKET:  REQUEST_ACCOUNT_INACTIVATE
############################################################
        elif type_id == 'request_account_inactivate':
#  These are needed for the section below where 'approval' table entries are created.
            resource_list = packet.ResourceList[0]
            cluster = resource_list.split('.')[0]
            person_id = packet.PersonID
            project_id = packet.ProjectID

#            print(" Approval not needed for 'request_account_inactivate''.  Skipping.")
#  See if trans_rec_id is in approval table.
#            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
#            data = (trans_rec_id,)
#            results = []
#            amiedb_call(sql,data,script_name,results)
#            if len(results) == 0:
#                print("The trans_rec_id is not in the 'approval' table.  Skipping.")
#                continue
#  Set approval_status to 'approved' so this won't appear when running the 'approvals.py' script.
#            try: 
#                sql = "UPDATE approval SET approval_status = 'approved' WHERE trans_rec_id = %s"
#                data = (trans_rec_id,)
#                results = []
#                amiedb_call(sql,data,script_name,results)
#            except Exception as e:
#                print("Oops.")
#  Check the approval_status we just set.
#            sql = "SELECT approval_status FROM approval WHERE trans_rec_id = %s"
#            data = (trans_rec_id,)
#            results = []
#            print("results = ",results)
#            amiedb_call(sql,data,script_name,results)
#            print("Approval status for 'request_account_inactivate' successfully reset to 'approved' for trans_rec_id = ",trans_rec_id,".")
#            continue
############################################################
#  PACKET:  REQUEST_PROJECT_INACTIVATE
############################################################
#        elif type_id == 'request_project_inactivate':
#            print(" Approval not needed for 'request_project_inactivate'.  Skipping.  Run 'respond.py' again.")
#            continue
############################################################
#  PACKET:  DATA_PROJECT_CREATE
############################################################
        elif type_id == 'data_project_create':
            print(" Approval not needed for 'data_project_create'.  Skipping.  Run 'respond.py' again.")
            continue
############################################################
#  PACKET:  DATA_ACCOUNT_CREATE
############################################################
        elif type_id == 'data_account_create':
            print(" Approval not needed for 'data_account_create'.  Skipping.  Run 'respond.py' again.")
            continue
############################################################
#  PACKET:  INFORM_TRANSACTION_COMPLETE
############################################################
#  Change 'state_id' to 'completed' if needed; do nothing otherwise.
        elif type_id == 'inform_transaction_complete':
#  Check value of 'state_id'.
            sql = "SELECT state_id FROM packet_tbl WHERE packet_rec_id = %s"
            data = (str(packet_rec_id),)
            results = []
            amiedb_call(sql,data,script_name,results)
            try:
                state_id = results[0][0]
#  If 'state_id' is 'in-progress', change to 'completed'.
                if state_id == 'in-progress':
                    if test == 0:
                        sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                        data = (str(packet_rec_id),)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        print(" Changed 'state_id' to 'completed' in 'packet_tbl' for packet_rec_id = ",packet_rec_id," and trans_rec_id = ",trans_rec_id)
                    else:
                        print(" TESTING:  Not sending message about changing 'state_id' to 'completed'.  Message would be:")
                        print(" Changed 'state_id' to 'completed' in 'packet_tbl' for packet_rec_id = ",packet_rec_id," and trans_rec_id = ",trans_rec_id)
            except Exception as e:
                print(" The state_id value extracted from packet_tbl for packet_rec_id = ",packet_rec_id," is ",results," and unusable.")
                print(" ERROR: ",str(e))
#  PACKET:  UNKNOWN
        else:
            person_id = packet.PersonID
            project_id = packet.ProjectID
            print(" Unknown packet type: ",type_id)

# =====================================================================================
#  DUMP INFO INTO TABLES transaction_tbl, packet_tbl, expected_reply_tbl and approval.
# =====================================================================================

# ================== PROCESSING FOR transaction_tbl TABLE INSERTION ========================

        originating_site_name = packet.originating_site_name
        transaction_id = packet.transaction_id
        local_site_name = packet.local_site_name
        remote_site_name = packet.remote_site_name
        state_id = packet.transaction_state

        try:
#  Find out if this trans_rec_id already exists in 'transaction_tbl'.
            sql = "SELECT * FROM transaction_tbl WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            if diag > 0:
                print(" DUMP_APPROVALS: trans_rec_id = ",trans_rec_id," transaction_tbl: results = ",results," len(results) = ",len(results))

#  Add this trans_rec_id to 'transaction_tbl'.
            if len(results) == 0:
                if test == 0:
                    sql = """INSERT INTO transaction_tbl (trans_rec_id,originating_site_name,transaction_id,local_site_name,remote_site_name,state_id,ts) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
                    data = (trans_rec_id, originating_site_name, transaction_id, local_site_name, remote_site_name, state_id, tstamp())
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print("   This ",type_id," with trans_rec_id ",trans_rec_id," is now added to 'transaction_tbl'.")
                else:
                    print(" TESTING:  Not performing transaction table insertion.  Message would be:")
#                    print("   This ",type_id," with trans_rec_id ",trans_rec_id," is now added to 'transaction_tbl'.")
                if diag > 0:
                    print(" DUMP_APPROVALS: transaction_tbl: results = ",results)
            else:
                if diag > 0:
                    print("   This ",type_id," with trans_rec_id ",trans_rec_id," has already been added to 'transaction_tbl'.")
#  If 'transaction_tbl' entry cannot be inserted, cry havoc and release the dogs of war.  Send a message, too.
        except Exception as e:
            msg1 = "ACCESS ERROR creating 'transaction_tbl' table entry."
            if type_id in no_person_id:
                msg2 = "Packet " + type_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            elif type_id in no_project_id:
                msg2 = "Packet " + type_id + " for user " + person_id + " with trans_rec_id " + trans_rec_id
            else:
                msg2 = "Packet " + type_id + " for user " + person_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            msg3 = "ERROR: " + str(e)
            if test == 0:
                send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            else:
                print(" TESTING:  Not sending error message about transaction table insertion.  Message would be:")
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)

# ================== PROCESSING FOR packet_tbl TABLE INSERTION ========================

        if diag > 0:
            print(" DUMP_APPROVALS: ********************** Processing 'packet_tbl' insertion")
        packet_rec_id = packet.packet_rec_id
        packet_id = packet.packet_id
        type_id = json_extract(pdict,'type')[0]
        version = '0.6.0'
        packet_state_id = packet.packet_state
        outgoing_flag = packet.outgoing_flag

        try:
#  Find out if this packet_rec_id already exists in 'packet_tbl'.
            sql = "SELECT * FROM packet_tbl WHERE packet_rec_id = %s"
            data = (str(packet_rec_id),)
            if diag > 0:
                print(" DUMP_APPROVALS: sql = ",sql," data = ",data)
            results = []
            amiedb_call(sql,data,script_name,results)
            if diag > 0:
                print(" DUMP_APPROVALS: packet_rec_id = ",packet_rec_id," packet_tbl: results = ",results," len(results) = ",len(results))

#  Add this 'packet_rec_id' to 'packet_tbl'.
            if len(results) == 0:
                if test == 0:
                    sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
                    data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state_id,outgoing_flag,tstamp())
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print("   This ",type_id," with packet_rec_id ",packet_rec_id," is now added to 'packet_tbl'.")
                else:
                    print(" TESTING:  No insertion into 'packet_tbl'.  Message would be:")
#                    print("   This ",type_id," with packet_rec_id ",packet_rec_id," is now added to 'packet_tbl'.")
                if diag > 0:
                    print(" DUMP_APPROVALS: data = ",packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state_id,outgoing_flag,tstamp())
                    print(" DUMP_APPROVALS: Inserted packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
            else:
                if diag > 0:
                    print("   This ",type_id," with packet_rec_id ",packet_rec_id," has already been added to 'packet_tbl'.")
        except Exception as e:
            msg1 = "ACCESS ERROR in 'dump_approvals' with 'packet_tbl' table."
            if type_id in no_person_id:
                msg2 = "Packet " + type_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            elif type_id in no_project_id:
                msg2 = "Packet " + type_id + " for user " + person_id + " with trans_rec_id " + trans_rec_id
            else:
                msg2 = "Packet " + type_id + " for user " + person_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            msg3 = "ERROR: " + str(e)
            if test == 0:
                send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            else:
                print(" TESTING:  Message about error with 'packet_tbl' insertion not sent.  Message would be: ")
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)

# ================== PROCESSING FOR expected_reply_tbl TABLE INSERTION ========================
#  The contents of the 'local_info' and 'approval' tables most likely render 'expected_reply_tbl' moot, but here it is anyway.

        if diag > 0:
            print(" DUMP_APPROVALS: ********************** Processing 'expected_reply_tbl' insertion")

        try:
#  Find out if this 'packet_rec_id' already exists in 'expected_reply_tbl'.
            sql = "SELECT * FROM expected_reply_tbl WHERE packet_rec_id = %s"
            data = (str(packet_rec_id),)
            results = []
            amiedb_call(sql,data,script_name,results)

#  Add this 'packet_rec_id` to 'expected_reply_tbl'.
            if len(results) == 0:
                if test == 0:
                    timeout = 60
                    sql = """INSERT INTO expected_reply_tbl (packet_rec_id,type_id,timeout) VALUES (%s, %s, %s);"""
                    data = (packet_rec_id,type_id,timeout)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print(" Added 'expected_reply_tbl' entry for packet #",packet_rec_id,".")
                else:
                    print(" TESTING:  Message not sent about adding entry to 'expected_reply_tbl'.  Message would be:")
                    print(" Added 'expected_reply_tbl' entry for packet #",packet_rec_id,".")
            else:
                if diag > 0:
                    print(" dump_approvals: The 'expected_reply_tbl' table already contains 'packet_rec_id' = ",packet_rec_id,". Skipping insertion.")
        except Exception as e:
            msg1 = "ACCESS ERROR in 'dump_approvals' with 'expected_reply_tbl' table."
            if type_id in no_person_id:
                msg2 = "Packet " + type_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            elif type_id in no_project_id:
                msg2 = "Packet " + type_id + " for user " + person_id + " with trans_rec_id " + trans_rec_id
            else:
                msg2 = "Packet " + type_id + " for user " + person_id + " on project " + project_id + " with trans_rec_id " + trans_rec_id
            msg3 = "ERROR: " + str(e)
            if test == 0:
                send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            else:
                print(" TESTING:  Not send error message about 'expected_reply_tbl' transaction. Message would be:")
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)

# ================== PROCESSING FOR approval TABLE ========================

#  !!!!! NOTE  !!!!! - filter out non-approval packets here rather than above.

#  Populate the 'approval' table only with packet types in the 'approvals' list.
        if type_id in approvals:
#            print(" Processing ",type_id," packet for user ",person_id," for 'approval' table.")
#            if diag > 0:
#                print(" DUMP_APPROVALS: ********************** Processing 'approval' insertion")

            approval_status = 'unapproved'
#  Keep trying until the 'approval' table entry has been successfully created.
#  A total of 3 attempts should be enough to handle the transient and rare "[Errno 104] Connection reset by peer" error.
            no_of_attempts = 3
            attempts = 0
            success = 'N'
            while success == 'N':
                attempts = attempts + 1
                if attempts > no_of_attempts:
                    msg1 = "Skipping creation of 'approval' table entry with trans_rec_id = " + str(trans_rec_id) + " after " + str(no_of_attempts) + " failed attempts."
                    print(msg1)
                    break
#                    continue
                    
                try:
#  Find out if this 'trans_rec_id' already exists in 'approval' table.
                    sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
                    data = (trans_rec_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#  Create 'approval' table entry if none exists.
                    if len(results) == 0:
                        if test == 0:
                            if allocation_type == 'new':
                                alloc_proxy = 'new-1'
                            else:
                                alloc_proxy = allocation_type + "-" + str(new_proxy)
                            if type_id in no_person_id:
                                person_id = 'NA'
                            elif type_id in no_project_id:
                                project_id = 'NA'
                            sql = "INSERT INTO approval (trans_rec_id,type_id,person_id,project_id,approval_status,ts_received,supplemental,cluster) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (trans_rec_id) DO NOTHING;"
                            data = (trans_rec_id,type_id,person_id,project_id,approval_status,tstamp(),alloc_proxy,cluster,)
                            results = []
                            amiedb_call(sql,data,script_name,results)
                            logging.info(script_name + ": Done processing 'local_info', 'approval', 'transaction_tbl', 'packet_tbl' and 'expected_reply_tbl' tables'")
#                            print(" DUMP_APPROVALS: Inserted info into 'approval' table for '",type_id,"' packet for user ",person_id,".")
                        else:
                            print(" TESTING:  Not adding entry to approval table.  Message would be:")
                            print(" DUMP_APPROVALS: Inserted info into 'approval' table for '",type_id,"' packet for user ",person_id,".")
#  Entry in 'approval' already exists.  Move on.
                    else:
                        if diag > 0:
                            print(" DUMP_APPROVALS: The 'approval' table already contains 'trans_rec_id' = ",packet_rec_id,". Skipping insertion.")
                            logging.info(script_name + ": Skipped processing DB tables for already existent entry.")
#  All the above indicate a successful insertion into the 'approval' table, so we change the value of 'success'.
                    success = 'Y'
#  Problem with 'approval' table insertion.  Create error message and send.
                except Exception as e:
                    msg1 = "Error creating 'approval' table entry."
                    if type_id in no_person_id:
                        msg2 = "Packet " + type_id + " on project " + project_id + " with trans_rec_id " + str(trans_rec_id) + "."
                    elif type_id in no_project_id:
                        msg2 = "Packet " + type_id + " for user " + person_id + " with trans_rec_id " + str(trans_rec_id) + "."
                    else:
                        msg2 = "Packet " + type_id + " for user " + person_id + " on project " + project_id + " with trans_rec_id " + str(trans_rec_id) + "."
                    msg3 = "ERROR: " + str(e)
#  There are different error messages for different packet types.
#  Send message for 'request_project_create' packets including the 'allocation_type' value.
                    if type_id == 'request_project_create':
                        msg_rpc = "This 'request_project_create' packet contains allocation type '" + allocation_type + "'."
                        if test == 0:
                            send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg_rpc + "\r\n" + msg3)
                            logging.info(script_name + ": " + msg_rpc + "\r\n" + msg2 + "\r\n" + msg3) 
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg_rpc + "\r\n" + msg3)
                        else:
                            print(" TESTING:  Not sending error message about 'approval' table transaction.  Message would be:")
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg_rpc + "\r\n" + msg3)
#  Send message for packets other than 'request_project_create'.
                    else:
                        if test == 0:
                            send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                            logging.info(script_name + ": " + msg2 + "\r\n" + msg2 + "\r\n" + msg3)
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                        else:
                            print(" TESTING:  Not sending error message about 'approval' table transaction.  Message would be:")
                            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
#            else:

        if type_id == 'request_account_inactivate':
            print("Approval not needed for 'request_account_inactivate'.")
#  See if trans_rec_id is in approval table.
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            if len(results) == 0:
                print("The trans_rec_id is not in the 'approval' table.  Skipping.")
                continue
#  Set approval_status to 'approved' so this won't appear when running the 'approvals.py' script.
            try:
                sql = "UPDATE approval SET approval_status = 'approved' WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                print("Approval status for 'request_account_inactivate' successfully reset to 'approved' for trans_rec_id = ",trans_rec_id,".")
            except Exception as e:
                print("Problem setting approval_status to approved in approve table.  Skipping.")
#  Check the approval_status we just set.
#            sql = "SELECT approval_status FROM approval WHERE trans_rec_id = %s"
#            data = (trans_rec_id,)
#            results = []
#            print("results = ",results)
#            amiedb_call(sql,data,script_name,results)
#            print("Approval status for 'request_account_inactivate' successfully reset to 'approved' for trans_rec_id = ",trans_rec_id,".")
#            continue
#XXXXX
#        sys.exit()

    pnum = pnum + 1




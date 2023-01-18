#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
import sys
import json
import logging
import os.path
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from amieclient import AMIEClient
from pprint import pprint

diag = 0

script_name = sys.argv[0][2:]

#  Ascertain whether this is a production of development environment.
#  Find out which directory we're in.
ACCESS_HOME = os.getcwd()
if ACCESS_HOME == '/home/baum/AMIEDEV':
    amie_config = ACCESS_HOME + '/amiedev.ini'
    email_receivers = ['baum@tamu.edu']
    print(" DEVELOPMENT MODE!!!!!")
elif ACCESS_HOME == '/home/baum/AMIE':
    amie_config = ACCESS_HOME + '/amie.ini'
    email_receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
    print(" PRODUCTION MODE!!!!")
else:
    print(" Not running in either '/home/baum/AMIE' or '/home/baum/AMIEDEV'. Exiting.")
    sys.exit()

log_file = ACCESS_HOME + '/amie.log'
#print(" log_file = ",log_file)

#  Ascertain if we're running in production or development mode.
#ACCESS_DEV = '/home/baum/AMIEDEV'
#ACCESS_HOME = '/home/baum/AMIE'
#cwd = os.getcwd()
#if cwd == ACCESS_HOME:
#    amie_config = ACCESS_HOME + '/amie.ini'
#    email_receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
#    print(" PRODUCTION MODE!!!!")
#elif cwd == ACCESS_DEV:
#    amie_config = ACCESS_DEV + '/amiedev.ini'
#    email_receivers = ['baum@tamu.edu']
#    print(" DEVELOPMENT MODE!!!!!")
#else:
#    print(" Not running in either ",ACCESS_DEV," or ",ACCESS_HOME,".  Exiting.")
#    sys.exit()

#  Set up logging.
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

#  Command-line argument processing
help_list = ['-h','--h','-H','--H','-help','--help','-Help','--Help']
#  Check for the presence of a command-line argument.
if len(sys.argv) > 1:
#  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(" Program: ",script_name," - demonstrate the use of a function for PostgreSQL access via Python/psycopg2")
        print(" Arguments: ",help_list," - this message")
        print("             None - run the program")
        print("             1 (i.e. '1' is the argument) - run the demo program with additional diagnostic output")
        print("             All other arguments ingloriously rejected.")
        print(" ")
        sys.exit()
    else:
#  Check for the only presently valid command-line argument "1".
        if sys.argv[1] == '1':
#            print(" Command-line argument is ",sys.argv[1])
            diag = int(sys.argv[1])
            print(" Printing diagnostics.")
        else:
#  Kick all invalid arguments ingloriously out of the script.
            print(" ")
            print(" The command-line argument entered '",sys.argv[1],"' is meaningless.  Exiting.")
            print(" Try one of ",help_list," for available valid arguments.")
            print(" ")
            sys.exit()

#  Set up email notifications
def send_email(body):
    email_sender = 'admin@faster-mgt2.hprc.tamu.edu'
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
        print(" Notification sent to:  ",','.join(email_receivers))

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

def slurm_acct():
    locran = "%0.12d" % random.randint(0,999999999999)
    return locran

#  Define a suitably short time stamp macro.
def tstamp():
    tst = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return tst

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

def amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value):
    try:
        t = 0
        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        cursor = conn.cursor()
        try:
            statement = """INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
            data = (person_id,project_id,packet_rec_id, tag, subtag, seq, value)
            cursor.execute(statement,data)
            conn.commit()
#            print(" Inserted data for packet_rec_id ",packet_rec_id," for tag ",tag," into data_tbl.")
        except:
            t = t + 1
            print(" Data for packet_rec_id ",packet_rec_id," tag ",tag," already exists in data_tbl.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to AMIEDB table data_tbl: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return

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

sendpackets = 'yes'
#sendpackets = 'no'

# Establish a log file.
logging.basicConfig(filename='amie.log', level=logging.INFO)
logging.info(' Running ' + script_name + ' on ' + tstamp())

#post_config = "/home/baum/AMIE2/post_config.ini" 

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Create the AMIE client.
amie_client = AMIEClient(site_name=site_con['site_name'],
                         amie_url=site_con['amie_url'],
                         api_key=site_con['api_key'])

print(' ')
print(' Running ',script_name,' to process approved incoming packets at ',tstamp())
print(' ')

#  Get all the available packets.
packets = amie_client.list_packets().packets
npack = len(packets)
print(' Number of unfiltered packets to process: ',npack)
if (npack == 0):
    print(" No packets to process.  Exiting.")
    sys.exit()

#  Filter out those eternally persistent 'inform_transaction_complete' packets.
filtered_packets = []
for packet in packets:
    pdict = json.loads(packet.json())
    type_id = json_extract(pdict,'type')[0]
    if type_id != 'inform_transaction_complete':
        filtered_packets.append(packet)
#        print(" packet = ",vars(packet))
#        print(" ")
#        print(" type_id = ",type_id)
npack = len(filtered_packets)
print (" Number of filtered packets to process: ",npack)

#  Looping through packets that aren't 'inform_transaction_complete' packets.
pnum = 1
for packet in filtered_packets:

#  Read cases from incoming stream.
#  The json.loads construct decodes the JSON into Python data structures.
    pdict = json.loads(packet.json())

#    print(" pdict check = ",pdict)
#    type = packet.type
#    print(" type = ",type)
#    sys.exit()

#  Check if packet has been locally approved before sending any return packets.

#  Yoink trans_rec_id and grant_number out of packet to compare to 'approval' table data.
#    type_id = packet.type
    type_id = json_extract(pdict,'type')[0]
    trans_rec_id = packet.trans_rec_id
    if diag > 0:
        print(" Dictionary for ",type_id," packet with trans_rec_id = ",trans_rec_id,".")
        print(' pdict = ',pdict)
    packet_timestamp = json_extract(pdict,'packet_timestamp')[0]
#    try:
#        grant_number = packet.GrantNumber
#        print(" &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& = ",grant_number)
#        grant_number = json_extract(pdict,'GrantNumber')[0]
#    except:
#        print(" No grant number in packet.")
    print("##################################################################################################")
    print(" PROCESSING PACKET NO. ",pnum)
    print("##################################################################################################")
    print(' Packet info: trans_rec_id = ',trans_rec_id,' type_id = ',type_id,' packet_timestamp = ',packet_timestamp)

# ********************* CHECK REQUESTS AGAINST APPROVAL STATUS *************************************

#  Process packet  unless:
#  1. packet is not in the 'approval' tablea
#  2. packet is in 'approval' table but 'unapproved'
#  3. packet is set to 'completed' in 'transaction_tbl' (this will filter out the eternally lingering 'inform_transaction_complete' packets)

#  Check if packet is in 'approval' table.
    sql = "SELECT approval_status FROM approval WHERE trans_rec_id = %s"
    data = (trans_rec_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print(" No entry in 'approval' table for trans_rec_id = ",trans_rec_id,". Skipping.")
        pnum = pnum + 1
        continue
    else:
        approval_status = results[0][0]
#  Check if packet is 'approved' in 'approval' table.
        if approval_status != 'approved':
            pnum = pnum + 1
            print(" Added to pnum for unapproved packet to make ",pnum)
            print(" Approval status for Transaction #",trans_rec_id," is: ",approval_status,". Moving to next packet.")
            continue
#  Skip the packet if it is of type 'inform_transaction_complete'.
        else:
            if type_id == 'inform_transaction_complete':
                print(" Skipping 'inform_transaction_complete' packet.")
                pnum = pnum + 1
                continue
            else:
#  If packet approved go forth and process it.
                print(" Approval status for Transaction #",trans_rec_id," is: ",approval_status,". Starting reply process.")
#  Add 'ts_reply' and 'reply_status' values to 'approval' table.
                try:
                    sql = 'UPDATE approval SET ts_reply = %s, reply_status = %s WHERE trans_rec_id = %s'
                    data = (tstamp(), '1', trans_rec_id)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                except:
                    print(" Could not add info to 'approval' table for trans_rec_id = ",trans_rec_id)
                    pnum = pnum + 1
                    print(" Added to pnum for approved packet to make: ",pnum)

#  Dump packet to text file as 'trans_rec_id.json' for testing purposes.
    pref = type_id.split("_")[0][0] + type_id.split("_")[1][0] + type_id.split("_")[2][0]
#  Extract trans_rec_id from packet.
    trans_rec_id = packet.trans_rec_id
    jsonfile = ACCESS_HOME + '/' + pref + '/' + pref + str(trans_rec_id) + '.json'
#    print(" ***** JSON file dump: ",jsonfile)
    if not os.path.isfile(jsonfile):
        with open(jsonfile, 'w') as convert_file:
            convert_file.write(json.dumps(pdict))

#  Extract 'data' dictionary from packet.            
    body = packet._original_data['body']

    logging.info(' Processing transaction ID ' + str(trans_rec_id) + ' for ' + type_id + ' with timestamp ' + str(packet_timestamp) + ' on ' + tstamp())

#  The incoming 'request_project_create` packet contains information to be used by a local
#  site to create a project number (ProjectID) and an account (PiPersonID) for the PI on
#  the project.  This should result in the creation of a local site project and ID as well as
#  an account for the PI with a local person ID.

# ********************* PROCESS request_project_create REQUESTS *************************************

# PACKET TYPE
    if type_id == 'request_project_create':
        print(" Processing 'request_project_create' packet.")
        print(" Extracting information required for 'notify_project_create' response to 'request_project_create'.")
        try:
            grant_number = packet.GrantNumber
        except:
            print(" No grant number in 'request_project_create' packet. Exiting.")
            sys.exit()

#        print(" Exiting request_project_create prematurely.")
#        sys.exit()

# =============== CHECK local_info TABLE FOR PROJECT EXISTENCE  =============================

#  If project number for project not yet in 'local_info', then skip and kick back to 'dump_approvals'.
        sql = "SELECT grant_number from local_info WHERE grant_number = %s"
        data = (grant_number,)
        results = []
        amiedb_call(sql,data,script_name,results)

        if (len(results) == 0):
            print(" Grant #",grant_number," not in 'local_info'. Run 'dump_approvals.py'. Skipping this packet.")
            pnum = pnum + 1
            continue

# =============== EXTRACT ESSENTIAL INFO FROM local_info TABLE ===========================

        print(" Extracting local_info for grant_number = ",grant_number)
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            cur = conn.cursor()
            sql = "SELECT * FROM local_info WHERE grant_number = %s"
            data = (grant_number,)
            cur.execute(sql,data)
            for row in cur.fetchall():
                (fn,ln,email,pi_person_id,project_id,pi_remote_site_login,gn,sa,service_units_allocated,start_date,ed,ps,accts,uid,gid,cluster,access_id,pi) = row
#                print(" pi_person_id = ",pi_person_id)
            cur.close()
            conn.close()
            print(" Extracted essential reply info for grant #",grant_number," from 'local_info'.")
#            print("All = ",fn,ln,email,pi_person_id,project_id,pi_remote_site_login,gn,sa,service_units_allocated,start_date,ed,ps,accts,uid,gid,cluster_access_id,pi)
#            print(" pi_person_id = ",pi_person_id)
        except:
            print(" Failed to extract essential reply info for grant #",grant_number," from 'local_info'.")

#        sys.exit()

# =============== POPULATE data_tbl TABLE =============================

        packet_rec_id = packet.packet_rec_id

#  ADD INFORMATION TO TABLE data_tbl
#  This cycles through all the entries in the 'body' portion of the JSON packet and transmogrifies
#  them into entries for the 'data_tbl` table.

        print(" Populating 'data_tbl' with contents of 'request_project_create' packet.")

        doit = 'yes'
        if (doit == 'yes'):
            for key in body:
# ############################################
#  The key value is a string.
# ############################################
                if (isinstance(body[key],str)):
                    if diag > 0:
                        print(" Key value for ",key," is a string.")
                        print(" Key value: ",body[key])
                        print(" ")
                    tag = key
                    subtag = 'NULL'
                    seq = 0
                    value = body[key]
                    if diag > 0:
                        print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                    amiedb_data_tbl(pi_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
# ##############################################
#  The key value is a list.
# ##############################################
                elif (isinstance(body[key],list)):
                    seq = 0
                    tag = key
                    for item in body[key]:
#                        print(" item = ",item)
# ==============================================
#  The key value is a list of dictionaries.
# ==============================================
                        if (isinstance(item,dict)):
                            if diag > 0:
                                print(" Key value for ",key," is a list of dictionaries.")
                                print(" Key value: ",body[key])
#                            seq = 0
                            for key2 in item:
#                                print(" key2 = ",key2," seq = ",seq)
                                subtag = key2
                                value = item[key2]
                                if diag > 0:
                                    print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                                amiedb_data_tbl(pi_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                            seq = seq + 1
# ============================================
#  The value is a list of strings.
# ============================================
                        else:
                            if diag > 0:
                                print(" Key value for ",key," is a list of strings.")
                                print(" Key value: ",body[key])
                            subtag = 'NULL'
                            value = body[key]
                            value = body[key][seq]
                            if diag > 0:
                                print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                            amiedb_data_tbl(pi_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                            seq = seq + 1
#                        print(" ")
# ############################################
#  The key value is a dictionary.
# ############################################
                elif (isinstance(body[key],dict)):
                    if diag > 0:
                        print(" Key value for ",key, " is a dictionary.")
                        print(" Key value = ",body[key])
#  Key value =  {'RecordID': 'rac.271873'}
                    seq = 0
                    tag = key
                    for key2 in body[key]:
                        subtag = key2
                        value = body[key][key2]
                        if diag > 0:
                            print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                        amiedb_data_tbl(pi_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
#                    print(" ")
                    seq = seq + 1
#  The key value is an unknown data type.
                else:
                    print(" Unknown data type for key: ",key)

# SP: 
##   add code to find the PI from the local database (or create the person in the local database)
###  and set pi_person_id, pi_login
###  add code to create the project for the grant_number (if project doesn't exist), or apply the action specified by allocation_type
###  set the project_id to the local id for the project (if it isn't already set from the RPC)
#    set the project state to active (if it is inactive), as the XDCDB will not send RPCs for inactive projects
#
# NOTE: if the record_id is not null, you should track it (associate it with the packet_rec_id).
# If a second RPC gets sent with the same record_id, the second RPC should not be processed,
# but the data from the first RPC sent in the reply NPC

# -------------------- SEND notify_project_create PACKET ------------------------

        pfos_number = json_extract(pdict,'PfosNumber')[0]
        pi_org_code = json_extract(pdict,'PiOrgCode')[0]
        project_title = json_extract(pdict,'ProjectTitle')[0]
        resource_list = body['ResourceList']
#        service_units_allocated = json_extract(pdict,'ServiceUnitsAllocated')[0]
#        start_date = json_extract(pdict,'StartDate')[0]

#  CONSTRUCT A notify_project_create PACKET FOR A REPLY
        try:
            npc = packet.reply_packet()
            npc.ProjectID = project_id
            npc.PiPersonID = pi_person_id
            npc.PiRemoteSiteLogin = pi_remote_site_login
            npc.GrantNumber = grant_number
            npc.PfosNumber = pfos_number
            npc.PiOrgCode = pi_org_code
            npc.ProjectTitle = project_title
            npc.ResourceList = resource_list
            npc.ServiceUnitsAllocated = service_units_allocated
            npc.StartDate = start_date

            logging.info(' Created notify_project_create package.')
            print(" Successfully constructed 'notify_project_create' packet.")

#  Dump packet to archive subdirectory.
            try:
                pref = type_id.split("_")[0][0] + type_id.split("_")[1][0] + type_id.split("_")[2][0]
                dumpfile = ACCESS_HOME + '/' + pref + '/' + pref + str(trans_rec_id) + ".json"
                with open(dumpfile, 'w') as f:
                    print(vars(npc), file=f)
                print(" Wrote file: ",dumpfile," to archive.")
            except:
                print(" Failed to write file ",dumpfile," to archive.")
#  Check to see if the database 'person_id' and 'project_id' match those in the packet to be sent.
            print(" ")
            print('\033[1m' + '****************************************************************' + '\033[0m')
            print('\033[1m' + 'CROSS-CHECK THE ProjectID AND PiPersonID VALUES' + '\033[0m')
            print(" ")
#            print(" The 'local_info' values: ProjectID: ",project_id,", PiPersonID: ",pi_person_id,", access ID: ",access_id," UID: ",uid,":")
            pid = '\033[1m' + 'ProjectID: ' + '\033[0m'
            ppid = '\033[1m' + 'PiPersonID: ' + '\033[0m'
            print(" The 'local_info' values: ",pid,project_id,ppid,pi_person_id)
            print(" ")
            print(" The packet values: ",vars(npc))
            print(" ")
            status = input(" Do the 'PiPersonID' and 'ProjectID' values in 'local_info' match those in the packet dump? ('no' to skip sending packet, anything else to continue)")
            if status == 'no':
                continue
            print('\033[1m' + '****************************************************************' + '\033[0m')
            print(" ")

        except:
            logging.error(' Failed to construct notify_project_create packet.')
            print(" Failed to construct 'notify_project_create' packet.")

        logging.info(' Sending notify_project_create response on ' + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        if (sendpackets == 'yes'):
#  Attempt to send the notify_project_create packet.
            try:
                amie_client.send_packet(npc)
                msg = "Successfully sent 'notify_project_create' packet for " + fn + " " + ln + " with access ID " + access_id + " and local user ID " + pi_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                print(msg)
                sent = 'y'
            except Exception as e:
                msg1 = "Error sending 'notify_project_create' packet for " + fn + " " + ln + " with access ID " + access_id + " and local user ID " + pi_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                msg2 = "Error message: " + str(e)
                print(msg1)
                print(msg2)
                send_email(msg1 + "\r\n" + msg2)

            if sent == 'y':
#  Change 'proj_stat' and 'acct_stat' to 'active' in 'local_info' table.
                try:
                    proj_stat = 'active'
                    acct_stat = 'active'
                    sql = "UPDATE local_info SET proj_stat=%s, acct_stat=%s WHERE grant_number = %s"
                    data = (proj_stat,acct_stat,grant_number)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'proj_stat' and 'acct_stat' to 'active' in 'local_info' table for trans_rec_id = ",trans_rec_id,".")
                except:
                    print_psycopg2_exception(err)
                    print(" Problem with changing 'proj_stat' and 'acct_stat' to 'active' in 'local_info' table for trans_rec_id = ",trans_rec_id,".")

#  Change 'state_id' to 'completed' in 'packet_tbl' table.
                try:
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
                except:
                    print(" Failed to change 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
#                print(" Finishing packet_tbl change.")
        else:
            print(" Test run. No 'notify_project_create' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS data_project_create REQUESTS *************************************

# PACKET TYPE
    if type_id == 'data_project_create':

#  ADD THE LIST OF DNs TO A LOCAL TABLE
# the data_project_create(DPC) packet has two functions:
# 1. to let the site know that the project and PI account have been setup in the XDCDB
# 2. to provide any new DNs for the PI that were added after the RPC was sent
# NOTE: a DPC does *not* have the resource. You have to get the resource from the RPC for the trans_rec_id

#  The XDCB replies to a 'notify_project_create' packet with a 'data_project_create' packet.
#  This contains all the DNs

        type_id = packet.type_id
        trans_rec_id = packet.trans_rec_id
        packet_rec_id = packet.packet_rec_id
        packet_id = packet.packet_id
        person_id = packet.PersonID
        project_id = packet.ProjectID
        type_id = json_extract(pdict,'type')[0]
#        first_name = packet.PiFirstName
#        last_name = packet.PiLastName
#        pi_global_id = packet.GlobalID
#        grant_number = packet.GrantNumber
#        type_id = packet.type
        version = '0.6.0'        
        packet_state = packet.packet_state
        outgoing_flag = packet.outgoing_flag

        try:
#  Add packet_rec_id of this packet to packet_tbl.
            sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (packet_rec_id) DO NOTHING;"""
            data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state,outgoing_flag,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Inserted packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
        except:
            print(" Failed to insert packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")

# This DnList must be added to/merged with the DnList already in the local data_tbl for this PI.

#  Extract new DNs from the data_project_create packet.
        dnlist = packet.DnList
        new_dnlist = []
        for dn in dnlist:
            tup = (person_id,project_id,packet_rec_id,'PiDnList','NULL',0,dn)
            new_dnlist.append(tup)
#        print(" ***** data_project_create - created PiDnList: *******")
#        nt = 1
#        for fu in new_dnlist:
#            print(nt,fu)
#            nt =  nt + 1

#  Extract previous DNs from data_tbl.
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
        cur = conn.cursor()
        sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
        data = (person_id, 'PiDnList')
        try:
            cur.execute(sql,data)
            old_dnlist = cur.fetchall()
            conn.commit()
            cur.close()
            conn.close()
#            print(" Extracted new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Could not extract new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")

        tmplist = []
        for old in old_dnlist:
            l_old = list(old)
            l_old[5] = 0
            l_old[2] = packet_rec_id
            old = tuple(l_old)
            tmplist.append(old)
        old_dnlist = tmplist

#        print("***** data_project_create - old PiDnList from data_tbl *****")
#        nt = 0
#        for old in old_dnlist:
#            print(nt,old)
#            nt = nt + 1

        if old_dnlist != new_dnlist:
#            print(" Old and new lists differ.  Merging.")
            mod_dnlist = []
            for nlist in new_dnlist:
                if not any(element == nlist for element in old_dnlist):
                    old_dnlist.append(nlist)
        else:
            print(" Old and new lists identical.  No merging.")

#  Redo the ordinal seq numbers for the merged list.
        nl = 0
        tmplist = []
        for old in old_dnlist:
            l_old = list(old)
            l_old[5] = nl
            old = tuple(l_old)
            tmplist.append(old)
            nl = nl + 1
        old_dnlist = tmplist

#        print("***** data_project_create - merged PiDnList *****")
#        nt = 0
#        for old in old_dnlist:
#            print(nt,old)
#            nt = nt + 1

#   5. Delete the new DN list from data_tbl.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
        cur = conn.cursor()
        try:
            sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
            data = (person_id, 'PiDnList')
            cur.execute(sql, data)
            conn.commit()
#            print(" Deleted all PiDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to delete all PiDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

        try:
            for data in old_dnlist:
                sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                cur.execute(sql,data)
                conn.commit()
                nl = nl + 1
#            print(" Inserted all merged PiDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to insert all merged PiDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")

        cur.close()
        conn.close()

# This DnList must be added to/merged with the DnList already in the local data_tbl for this PI.
#        dnlist = packet.DnList

        print(" Processing 'data_project_create' packet for 'trans_rec_id' = ",trans_rec_id)
#  Update transaction_tbl table value of state_id to 'completed'.
        try:
            sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Set 'state_id' to 'completed' in 'transaction_tbl' for trans_rec_id = ",trans_rec_id)
        except:
            print(" Failed to set 'state_id' to 'completed' in 'transaction_tbl' for trans_rec_id = ",trans_rec_id)

#  Update packet_tbl table value of state_id to 'completed'.
#        print(" ***** data_project_create:  starting packet_tbl update")
        try:
            packet_rec_id = packet.packet_rec_id
            sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
            data = (str(packet_rec_id),)
#            print(" data = ",data)
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Updated 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")
        except:
            print(" Failed to update 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")

#  Construct the InformTransactionComplete(ITC) success packet
        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

        # send the ITC
        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(itc)
                sent = 'y'
                print(" Sent 'inform_transaction_complete' packet for 'trans_rec_id' = ",trans_rec_id,".")
#  Send EMAIL notification that request_project_create transaction is complete.
            except:
                print(" Problem with sending 'inform_transaction_complete' packet for 'trans_rec_id' = ",trans_rec_id,".")
            if (sent == 'y'):
#  Extract first and last name from local_info using person_id.
                try:
                    sql = "SELECT first_name,last_name FROM local_info WHERE person_id LIKE %s"
                    data = (person_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    (first_name,last_name) = results[0]
                except:
                    print(" First and last names not found in 'local_info' via 'person_id' search.")
                    first_name = 'unknown'
                    last_name = 'unknown'
#  Send email notification for completion of sending 'inform_transaction_complete' packet.
                try:
                    body = "The " + type_id + " transaction for " + first_name + " " + last_name + " with ID " + person_id + " and project ID " + project_id + " has completed."
                    send_email(body)
                    print(" Sent email confirmation for ",type_id," completion for ",first_name,last_name," with ID ",person_id," on project ",project_id,".")
                except:
                    print(" Problem with sending confirmation for ",type_id," completion for ",person_id," on project ",project_id,".")
        else:
            print(" Test run. No 'inform_transaction_complete' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS request_account_create REQUESTS *************************************

#  This packet asks a local site to create an account for a user on a project that already exists.

# PACKET_TYPE
    if type_id == 'request_account_create':

        print(" Starting 'request_account_create' processing.")

        grant_number = packet.GrantNumber
        project_id = packet.ProjectID

# =============== CHECK local_info TABLE FOR PROJECT EXISTENCE  =============================
#  If grant number for project not yet in 'local_info' do not create account for non-existent project.
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            cur = conn.cursor()
            sql = "SELECT * from local_info WHERE grant_number = %s"
            data = (grant_number,)
            cur.execute(sql,data)
            status = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as err:
            print(' An exception has occurred: ',error)
            print(' Exception type: ',type(error))

        if (len(status) == 0):
            print(" Project #",grant_number," not yet created.  Cannot create account yet.")
            pnum = pnum + 1
            continue

#        pnum = pnum + 1

# =============== MODIFY local_info TABLE =============================

        user_global_id = packet.UserGlobalID
        user_first_name = packet.UserFirstName
        user_last_name = packet.UserLastName
        user_person_id = 'u.' + user_first_name[0].lower() + user_last_name[0].lower() + user_global_id
        user_remote_site_login = user_person_id

        site_person_id = packet.SitePersonId
        access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']

#  Checking for 'request_account_create' being used as a proxy for 'request_account_reactivate'.
        print(" Checking for 'request_account_create' being used as a proxy for 'request_account_reactivate'.")
        try:
            sql = "SELECT * FROM local_info WHERE person_id = %s"
            data = (user_person_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
#  If acct_stat is either 'pending' or 'inactive', change to 'active'.  Do nothing if it is already set to 'active'.
            if len(results) != 0:
                (fn,ln,email,pid,project_id,remote_site_login,gn,sa,service_units_allocated,start_date,ed,ps,accts,uid,gid,cluster,access_id,pi) = results[0]
                if (accts == 'inactive' or accts == 'pending'):
                    print(" Account status of user ",user_person_id," is 'inactive'.  Proxy request.  Setting to 'active'.")
                    acct_stat = 'active'
                    try:
                        sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s"
                        data = (acct_stat, user_person_id)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        print(" Changed ",user_person_id," account to ",acct_stat)
                    except:
                        print(" Unable to change user ",user_person_id," account to ",acct_stat)
                else:
                    print(" Account status of user ",user_person_id," is 'active'.  Not a proxy request.")
            else:
                print(" No matching name in 'local_info'.  Something is wrong.  Exiting.")
                sys.exit()
        except:
            print(" Problem with 'local_info' query for 'project_id' = ",project_id," for 'request_account_create'.")

# =============== CHECK data_tbl TO SEE IF USER INFO IS ALREADY THERE ===============

        try:
            sql = "SELECT * FROM data_tbl WHERE person_id = %s"
            data = (user_person_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            numper = len(results)
            print(" User ",user_person_id," is already in the 'data_tbl' table.  Skipping table population. ")
        except:
            print(" Problem with searching for user ",user_person_id," in 'data_tbl' table.")

# =============== POPULATE data_tbl TABLE =============================

        packet_rec_id = packet.packet_rec_id

#  Only add entries to data_tbl if this user has no entries in that table.
        if (numper == 0):

            print(" Populating 'data_tbl' with contents of 'request_account_create' packet for ",user_person_id," on project ",project_id,".")
            print(" ")

            for key in body:
# ############################################
#  The key value is a string.
# ############################################
                if (isinstance(body[key],str)):
                    if diag > 0:
                        print(" Key value for ",key," is a string.")
                        print(" Key value: ",body[key])
                        print(" ")
                    tag = key
                    subtag = 'NULL'
                    seq = 0
                    value = body[key]
                    if diag > 0:
                        print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                    amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
# ##############################################
#  The key value is a list.
# ##############################################
                elif (isinstance(body[key],list)):
                    seq = 0
                    tag = key
                    for item in body[key]:
#                        print(" item = ",item)
# ==============================================
#  The key value is a list of dictionaries.
# ==============================================
                        if (isinstance(item,dict)):
                            if diag > 0:
                                print(" Key value for ",key," is a list of dictionaries.")
                                print(" Key value: ",body[key])
#                            seq = 0
                            for key2 in item:
#                                print(" key2 = ",key2," seq = ",seq)
                                subtag = key2
                                value = item[key2]
                                if diag > 0:
                                    print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                                amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                            seq = seq + 1
# ============================================
#  The value is a list of strings.
# ============================================
                        else:
                            if diag > 0:
                                print(" Key value for ",key," is a list of strings.")
                                print(" Key value: ",body[key])
                            subtag = 'NULL'
                            value = body[key]
                            value = body[key][seq]
                            if diag > 0:
                                print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                            amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                            seq = seq + 1
                        print(" ")
# ############################################
#  The key value is a dictionary.
# ############################################
                elif (isinstance(body[key],dict)):
                    if diag > 0:
                        print(" Key value for ",key, " is a dictionary.")
                        print(" Key value = ",body[key])
#  Key value =  {'RecordID': 'rac.271873'}
                    seq = 0
                    tag = key
                    for key2 in body[key]:
                        subtag = key2
                        value = body[key][key2]
                        if diag > 0:
                            print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                        amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                    print(" ")
                    seq = seq + 1
#  The key value is an unknown data type.
                else:
                    print(" Unknown data type for key: ",key)
#                print(" key = ",key," value = ",body[key]," type = ",tp)

#  Add SQL code to add the user to local_info if the user isn't already in there.
#  Then add an account for the User on the specified project (project_id) on the resource
#  RACs are also used to reactivate accounts, so if the account already exists, just set it active

#  The `notify_account_create` reply packet must contain:
#    - ProjectID - extracted from incoming packet
#    - UserPersonID - created upon receipt of packet
#    - UserRemoteSiteLogin - created upon receipt of packet
#    - ResourceList - extract from incoming packet

#  Construct a NotifyAccountCreate(NAC) packet.
        print(' Constructing the NAC packet.')
        nac = packet.reply_packet()
        nac.ProjectID = project_id
        nac.UserPersonID = user_person_id
        nac.UserRemoteSiteLogin = user_remote_site_login
        nac.ResourceList = packet.ResourceList

#        print(" nac = ",vars(nac))
#        sys.exit()

        print(" ProjectID = ",project_id)
        print(" UserPersonID = ",user_person_id)
        print(" UserRemoteSiteLogin = ",user_remote_site_login)
        print(" ResourceList = ",packet.ResourceList)

#  It is somewhat ambiguous in the docs as to the necessity of the following in the reply.
#  That is, the docs tell us that a table lists tags that could be included, and that the ones in bold must be included.
#  In the table the subtags for these are in bold but not the tags.  Go figure.
#        resource_login = [{'Resource': 'test-resource1.tamu.xsede', 'Login': user_remote_site_login}]
#        nac.ResourceLogin = resource_login
#        nac.AcademicDegree = packet.AcademicDegree
#        print(" ResourceLogin = ",resource_login)
#        print(" AcademicDegree = ",packet.AcademicDegree)


# 

# send the NAC
        if (sendpackets == 'yes'):
            try:
# Send the notify_account_create (NAC) packet.
                amie_client.send_packet(nac)
                print(" Sent 'notify_account_create' packet for 'trans_rec_id' = ",trans_rec_id,".")
                nac_send = 'y'
                print(' ************** trans_rec_id = ',trans_rec_id)
                try:
                    msg = " Successful sending 'notify_account_create' packet for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                    print(" msg = ",msg)
                except:
                    try:
                        print(" user_first_name = ",user_first_name)
                    except:
                        user_first_name = "name_1"
                    try:
                        print(" user_last_name = ",user_last_name)
                    except:
                        user_last_name = "name_2"
                    try:
                        print(" access_id = ",access_id)
                    except:
                        access_id = "access_id_1"
                    print(" Error causing short form of email message to be sent.")
                    msg = " Successful sending 'notify_account_create' packet for " + user_first_name + " " + user_last_name + " with access ID " + access_id
                    print(" Short form: msg = ",msg)
#  Print error message if sending NAC packet fails. DOODY
            except Exception as e:
                msg1 = "Error sending 'notify_account_create' packet for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                msg2 = "Error message: " + str(e)
                print(" Error message: ",str(e))
                print(" Exiting respond.py script.")
                send_email(msg1 + "\r\n" + msg2)
                print(" Skipping to next packet.")
                if npack == pnum-1:
                    print(" No more packets.  Exiting.")
                continue
#                nac_send = 'n'
        else:
            print(" Test run. No 'notify_account_create' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        if nac_send == 'y':
#  Change 'state_id' to 'completed' in 'packet_tbl' table.
            try:
                sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                data = (str(packet_rec_id),)
                results = []
                amiedb_call(sql,data,script_name,results)
                print(" Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
            except:
                print(" Failed to change 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS data_account_create REQUESTS *************************************

# PACKET TYPE
    if type_id == 'data_account_create':

#  The XDCB replies to a 'notify_account_create' packet with a 'data_account_create' packet.
#  This contains all the DNs

        trans_rec_id = packet.trans_rec_id
        packet_rec_id = packet.packet_rec_id
        packet_id = packet.packet_id
        person_id = packet.PersonID
        project_id = packet.ProjectID
        type_id = json_extract(pdict,'type')[0]
#        first_name = packet.PiFirstName
#        last_name = packet.PiLastName
#        user_global_id = packet.UserGlobalID
#        grant_number = packet.GrantNumber
#        type_id = packet.type
        version = '0.6.0'
        packet_state = packet.packet_state
        outgoing_flag = packet.outgoing_flag

        print(" Processing 'data_account_create' packet for 'trans_rec_id' = ",trans_rec_id)
        print(" person_id = ",person_id," project_id = ",project_id)

        try:
#  Add packet_rec_id of this packet to packet_tbl.
            sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (packet_rec_id) DO NOTHING;"""
            data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state,outgoing_flag,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Inserted packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
        except:
            print(" Failed to insert packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")

# ************* NOT DONE YET ******************
# This DnList must be added to/merged with the DnList already in the local data_tbl for this PI.
#        dnlist = packet.DnList

#  Extract new DNs from the data_account_create packet.
        dnlist = packet.DnList
        new_dnlist = []
        for dn in dnlist:
            tup = (person_id,project_id,packet_rec_id,'UserDnList','NULL',0,dn)
            new_dnlist.append(tup)

#  Extract previous DNs from data_tbl.
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
        cur = conn.cursor()
        sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
        data = (person_id, 'UserDnList')
        try:
            cur.execute(sql,data)
            old_dnlist = cur.fetchall()
            conn.commit()
            cur.close()
            conn.close()
#            print(" Extracted new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Could not extract new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")

        osize = len(old_dnlist)
#        print(" old_dnlist size = ",osize)
        tmplist = []
        for old in old_dnlist:
            l_old = list(old)
            l_old[5] = 0
            l_old[2] = packet_rec_id
            old = tuple(l_old)
            tmplist.append(old)
        old_dnlist = tmplist

        if old_dnlist != new_dnlist:
            mod_dnlist = []
            for nlist in new_dnlist:
                if not any(element == nlist for element in old_dnlist):
                    old_dnlist.append(nlist)
        else:
            print(" Old and new lists identical.  No merging.")

#  Redo the ordinal seq numbers for the merged list.
        nl = 0
        tmplist = []
        for old in old_dnlist:
            l_old = list(old)
            l_old[5] = nl
            old = tuple(l_old)
            tmplist.append(old)
            nl = nl + 1
        old_dnlist = tmplist

#   5. Delete the new DN list from data_tbl.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
        cur = conn.cursor()
        try:
            sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
            data = (person_id, 'UserDnList')
            cur.execute(sql, data)
            conn.commit()
            print(" Deleted all UserDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to delete all UserDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

        try:
            for data in old_dnlist:
                sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                cur.execute(sql,data)
                conn.commit()
                nl = nl + 1
            print(" Inserted all merged UserDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to insert all merged UserDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")

        cur.close()
        conn.close()

#        sys.exit()

#  ADD THE LIST OF DNs TO A LOCAL TABLE

        # the data_account_create(DPC) packet has two functions:
        # 1. to let the site know that the account for the given ProjectID has been setup in the XDCDB
        # 2. to provide any new DNs for the user that were added after the RPC was sent
        # NOTE: a DPC does *not* have the resource. You have to get the resource from the RPC for the trans_rec_id

#  Construct the InformTransactionComplete(ITC) success packet
        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

# send the ITC
        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(itc)
                itc_sent = 'y'
                print(" Sent 'inform_transaction_complete' packet for 'data_account_create' for 'trans_rec_id' = ",trans_rec_id,".")
            except:
                print(" Problem with sending 'inform_transaction_complete' packet for 'data_account_create' for 'trans_rec_id' = ",trans_rec_id,".")
#  Send EMAIL notification that request_project_create transaction is complete.
            if (itc_sent == 'y'):
#  Update transaction_tbl table value of state_id to 'completed'.
                try:
                    sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
                    data = (trans_rec_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Updated 'state_id' for 'data_account_create' packet for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
                except:
                    print(" Failed to update 'state_id' for 'data_account_create' packet for 'trans_rec_id' = ",trans_rec_id," to completed.")
#  Change 'state_id' to 'completed' in 'packet_tbl' table.
                try:
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
                except:
                    print(" Failed to change 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")                
#  Extract first and last name from local_info using person_id.
                try:
                    sql = "SELECT first_name,last_name,access_id FROM local_info WHERE person_id LIKE %s"
                    data = (person_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    (first_name,last_name,access_id) = results[0]
                except:
                    print(" First and last names not found in 'local_info' via 'person_id' search.")
                    first_name = 'unknown'
                    last_name = 'unknown'
#  Send email to notify sending of 'inform_transaction_complete' packet.
                try:
                    body = "The " + type_id + " transaction for " + first_name + " " + last_name + " with access ID " + access_id + ", person ID " + person_id + " and project ID " + project_id + " has completed."
                    send_email(body)
                    print(" Sent email confirmation for ",type_id," completion for ",first_name,last_name + " with " + person_id," on project ",project_id,".")
                except:
                    print(" Problem with sending email confirmation for ",type_id," completion for ",person_id," on project ",project_id,".")
        else:
            print(" Test run. No 'inform_transaction_complete' packet sent for 'data_account_create' for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS request_project_inactivate REQUESTS *************************************

# PACKET TYPE
    if type_id == 'request_project_inactivate':

        print(" Processing packet type:  'request_project_inactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource = packet.ResourceList[0]
        project_id = packet.ProjectID
        print(' %%%%%%%%%%%%%%%%%%%%%%% project_id to inactivate: ',project_id)

# SP: inactivate the project - DONE
# SP: inactivate all accounts on the project - DONE

        npi = packet.reply_packet()
        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(npi)
                print(" Sent 'notify_project_inactivate' packet for 'trans_rec_id' = ",trans_rec_id,".")
                proj_stat = 'inactive'
                acct_stat = 'inactive'
                try:
#  Change 'proj_stat' to 'inactive'.
                    try:
                        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
                    except:
                        print(" Error connecting to 'local_info' when processing '",type_id,"' packet.")
                    cur = conn.cursor()
                    try:
                        sql = "UPDATE local_info SET proj_stat = %s WHERE project_id = %s"
                        data = (proj_stat,project_id)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        print(" Changed 'proj_stat' to 'inactive' in 'local_info' for project_id = ",project_id)
#  Send email notification of project becoming inactive.
                        try:
                            body = "Project ID " + project_id + " has been set to inactive."
                            send_email(body)
                            print(" Sent email confirmation for project ",project_id," being set to inactive.")
                        except:
                            print(" Unable to send email confirmation for project ",project_id," being set to inactive.")
                    except:
                        print(" Error attempting to change 'proj_stat' for ",type_id)
                        print_psycopg2_exception(err)
#  Change all 'acct_stat' instances for this project to 'inactive'.
                    try:
                        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
                    except:
                        print(" Error connecting to 'local_info' when processing '",type_id,"' packet.")
                    cur = conn.cursor()
                    try:
                        sql = "UPDATE local_info SET acct_stat = %s WHERE project_id = %s"
                        data = (acct_stat,project_id)
                        cur.execute(sql, data)
                        conn.commit()
                        cur.close()
                        conn.close()
                        print(" Changed all 'acct_stat' instances to 'inactive' in 'local_info' for project_id = ",project_id)
                    except:
                        print(" Error attempting to change 'acct_stat' for ",type_id)
                        print_psycopg2_exception(err)
                    print(" Changed 'proj_stat' to 'inactive' in 'local_info' for project_id = ",project_id)
                except:
                    print(" Problem with changing 'proj_stat' or 'acct_state' to 'inactive' in 'local_info' table.")
            except:
                print(" Problem with sending 'notify_project_inactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        else:
            print(" Test run. No 'notify_project_inactivate' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

# ********************* PROCESS request_project_reactivate REQUESTS *************************************

# SP: reactivate the project - DONE
# SP: reactivate only PI account on project - DONE

# PACKET TYPE
    if type_id == 'request_project_reactivate':

        trans_rec_id = packet.trans_rec_id
        print(" Processing packet type:  'request_project_reactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource = packet.ResourceList[0]
        project_id = packet.ProjectID
        pi_person_id = packet.PersonID

# SP: reactivate the project and the PI account on the project (but no other accounts)

        npr = packet.reply_packet()
        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(npr)
                print(" Sent 'request_project_reactivate' packet for 'trans_rec_id' = ",trans_rec_id,".")
                proj_stat = 'active'
                acct_stat = 'active'
                try:
                    conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
                    cursor = conn.cursor()
#  Set PI project status to 'active'.
                    sql = "UPDATE local_info SET proj_stat = %s WHERE project_id = %s"
                    data = (proj_stat,project_id)
                    cursor.execute(sql, data)
#  Set PI account status to 'active'.
                    sql = "UPDATE local_info SET acct_stat = %s WHERE project_id = %s"
                    data = (acct_stat,project_id)
                    cursor.execute(sql, data)
                    conn.commit()
                    cursor.close()
                    conn.close()
                    print(" Changed 'proj_stat' to 'active' in 'local_info' table.")
                except:
                    print(" Problem with changing 'proj_stat' to 'active' in 'local_info' table.")
            except:
                print(" Problem with sending 'notify_project_reactivate' for 'trans_rec_id' = ",trans_rec_id,".")
        else:
            print(" Test run. No 'request_project_reactivate' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

# ********************* PROCESS request_account_inactivate REQUESTS *************************************

# PACKET TYPE
    if type_id == 'request_account_inactivate':

# SP: send a 'notify_account_inactivate' packet - DONE
# SP: inactivate a single account - DONE

#  TYPICAL RAI PACKET:
#pdict =  {'DATA_TYPE': 'Packet', 'type': 'request_account_inactivate', 'body': {'ProjectID': 'p.ibt228285.000', 'PersonID': 'u.ae57432', 'ResourceList': ['test-resource1.tamu.xsede'], 'Comment': 'amie_test', 'AllocatedResource': 'test-resource1.tamu.xsede'}, 'header': {'packet_rec_id': 222153030, 'packet_id': 1, 'transaction_id': 93, 'trans_rec_id': 111135872, 'expected_reply_list': [{'type': 'notify_account_inactivate', 'timeout': 30240}], 'local_site_name': 'TGCDB', 'remote_site_name': 'TAMU', 'originating_site_name': 'TGCDB', 'outgoing_flag': 1, 'transaction_state': 'in-progress', 'packet_state': 'in-progress', 'packet_timestamp': '2022-03-25T21:36:05.173Z'}}

#RAT
        print(" Processing packet type:  'request_account_inactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource = packet.ResourceList[0]
        project_id = packet.ProjectID
        person_id = packet.PersonID
        packet_rec_id = packet.packet_rec_id

#  Set 'acct_stat' to inactive in 'local_info'.
        acct_stat = 'inactive'
        try:
#  Changed to add project_id since a given person_id can be on more than one project.
            sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s AND project_id = %s"
#            sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s"
            data = (acct_stat,person_id,project_id)
            results = []
            amiedb_call(sql,data,script_name,results)
            stat_change = 'yes'
            print(" Changed 'acct_stat' to 'inactive' in 'local_info' table for project ID #",project_id,".")
        except:
            print(" Problem with changing 'acct_stat' to 'inactive' in 'local_info' table for project ID #",project_id,".")

#  Send packet ONLY if 'acct_stat' is successfully changed to 'inactive'.
        if stat_change == 'yes':
            nai = packet.reply_packet()
            try:
                amie_client.send_packet(nai)
                print(" Sent 'notify_account_inactivate' packet for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")
                sent_nai = 'yes'
            except:
                print(" Problem with sending 'notify_account_inactivate' for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")
        if (sent_nai == 'yes' and stat_change == 'yes'):
#  Update tables and send email ONLY if both packet sending and 'acct_stat' changing are successful.
            try:
                sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
            except:
                print(" Failed to update 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
#  Update packet_tbl table value of state_id to 'completed'.
            try:
                sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                data = (str(packet_rec_id),)
                print(" data = ",data)
                results = []
                amiedb_call(sql,data,script_name,results)
                print(" Updated 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")
            except:
                print(" Failed to update 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")
#  Send email notification of account becoming inactive.
#  Find first and last names from person_id.
            try:
                sql = "SELECT first_name,last_name FROM local_info WHERE person_id LIKE %s"
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (first_name,last_name) = results[0]
            except:
                print(" First and last names not found in 'local_info' via 'person_id' search.")
                first_name = 'unknown'
                last_name = 'unknown'
            try:
                body = "User " + first_name + " " + last_name + " with ID " + person_id + " on Project ID " + project_id + " has been set to inactive."
                send_email(body)
                print(" Sent email confirmation for user ",first_name,last_name," with ID ",person_id," on project ",project_id," being set to inactive.")
            except:
                print(" Unable to send email confirmation for user ",first_name,last_name," with ID ",person_id," on project ",project_id," being set to inactive.")

# ********************* PROCESS request_account_reactivate REQUESTS *************************************

# PACKET TYPE
    if type_id == 'request_account_reactivate':

# SP: send a 'notify_account_reactivate' packet - DONE
# SP: reactivate a single account - DONE

        print(" Processing packet type:  'request_account_reactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource = packet.ResourceList[0]
        project_id = packet.ProjectID
        person_id = packet.PersonID

        npr = packet.reply_packet()
        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(npr)
                print(" Sent 'notify_account_reactivate' packet for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")
                acct_stat = 'active'
#  Reactivate account.
                try:
                    sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s"
                    data = (acct_stat,person_id)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'acct_stat' to 'active' in 'local_info' table for project ID #",project_id,".")
                except:
                    print(" Problem with changing 'acct_stat' to 'active' in 'local_info' table for project ID #",project_id,".")
            except:
                print(" Problem with sending 'notify_account_reactivate' for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")

        else:
            print(" Test run. No 'notify_account_reactivate' packet sent for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")

# ********************* PROCESS request_user_modify REQUESTS *************************************

# For replace, if a tag is present, the value associated with the tag at the local site 
# must be replaced with the value in the packet; if a tag is not present, then the value 
# associated with the tag at the local site must be deleted (or replaced with a null value). 
# An exception is made for the DnList tag. If the DnList tag is present, DNs listed must be 
# added to the grid mapfile; DNs in the grid mapfile which are not listed must be preserved. 
# If the DnList tag is not present, all DNs in the grid mapfile must be preserved.
#
# For delete, the only optional tag will be DnList which will specify DNs that must be deleted 
# from the grid mapfile. Only the specific DN strings listed should be deleted. Other DNs that some 
# applications may consider to be equivalent, but are not listed should not be deleted.

# PACKET TYPE
    if type_id == 'request_user_modify':

        person_id = packet.PersonID
        action_type = packet.ActionType

#  Do what needs to be done for both 'delete' (completely) and 'replace' (partially).

#  Extract the old DnList from 'data_tbl' for the user info to be deleted/replaced.
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
        cur = conn.cursor()
        sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
#  The DnList may be for a Pi (pi.*) or a User (u.*).
        if person_id.split('.')[0] == 'u':
            data = (person_id,'UserDnList')
        else:
            data = (person_id,'PiDnList')
        try:
            cur.execute(sql,data)
            conn.commit()
#  Store old DnList in variable 'old_dnlist'.
            old_dnlist = cur.fetchall()
            print(" Extracted old DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")
            print(" ")
            cur.close()
            conn.close()
#  Extract the ProjectID from data_tbl since it is not contained within the 'request_user_modify' packet.
            if len(old_dnlist) != 0:
                (x1,project_id,x2,x3,x4,x5,x6) = old_dnlist[0]
            else:
                project_id = 'NA'
                print(" No DnList in old 'data_tbl' info.  Defining 'project_id' as 'NA'.")
        except:
            print_psycopg2_exception(err)
            print(" Could not extract old DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")

#  For delete, the only optional tag will be DnList which will specify DNs that must be deleted from the grid mapfile.

#  For action_type = 'delete' for 'request_user_modify':
#   1. Extract the list of DNs to be deleted from the incoming packet as delete_dnlist.
#   2. Place the old_dnlist - delete_dnlist tuple list into mod_dnlist.
#   3. Remove the old_dnlist entries from data_tbl.
#   4. Add the mod_dnlist entries to data_tbl.

        if action_type == 'delete':
            print(" PROCESSING delete ACTION for request_user_modify.")

            mod_dnlist = []
#  Extract from the packet the list of DNs to be deleted.
            delete_dnlist = packet.DnList
#  Delete the delete_dn_list tuples from old_dn_list.
#  Loop through old_dnlist.
            for olist in old_dnlist:
#  Compare each tuple in old_dnlist to all the tuples in delete_dnlist to see if they match.
                if not any(element == olist for element in delete_dnlist):
#  If there is no match append the old_dnlist tuple to mod_dnlist.
#  This should create a mod_dnlist that contains all the entries in old_dnlist not contained in delete_dnlist.
                    mod_dnlist.append(olist)
            print(" ")
            print(" Comparing DN lists for 'delete' packet:")
            print(" old_dnlist = ",old_dnlist)
            print(" delete_dnlist = ",delete_dnlist)
            print(" mod_dnlist = ",mod_dnlist)

#  Delete the old_dnlist entries from 'data_tbl'.

            print(" DELETING old_dnlist FROM data_tbl for ",person_id)
            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            try:
                sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
                if person_id.split('.')[0] == 'u':
                    data = (person_id,'UserDnList')
                    print(" Deleting UserDnList entries in data_tbl.")
                else:
                    data = (person_id,'PiDnList')
                    print(" Deleting PiDnList entries in data_tbl.")
                cur.execute(sql, data)
                conn.commit()
                print(" Deleted all *DnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
            except:
                print_psycopg2_exception(err)
                print(" Unable to delete all *DnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

#  Add the mod_dnlist entries to 'data_tbl'.

            print(" ADDING mod_dnlist TO data_tbl FOR ",person_id)
            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            nl = 0
            try:
                for data in mod_dnlist:
                    print(" data = ",data)
                    sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                    cur.execute(sql,data)
                    conn.commit()
                    nl = nl + 1
                cur.close()
                conn.close()
                print(" Inserted merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")
            except:
                print_psycopg2_exception(err)
                print(" Failed to insert merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")

            cur.close()
            conn.close()

#  For action_type = 'replace' for 'request_user_modify':
#   1. Delete all the previous data_tbl entries for person_id.
#   2. Add all the new data_tbl entries for person_id.
#   3. Extract the new DN list from data_tbl.
#   4. Merge the new and old DN lists.
#   5. Delete the new DN list from data_tbl.
#   6. Add the merged DN list to data_tbl.

        else:
            print(" PROCESSING replace ACTION for request_user_modify.")
#        if action_type == 'replace':

            packet_rec_id = packet.packet_rec_id

#   1. Delete all the previous data_tbl entries for person_id.

            print(" DELETING data_tbl ENTRIES FOR ",person_id)
            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            try:
                sql = "DELETE FROM data_tbl WHERE person_id = %s"
                data = (person_id,)
                cur.execute(sql, data)
                conn.commit()
                print(" Deleted all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
            except:
                print_psycopg2_exception(err)
                print(" Unable to delete all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

#   2. Add all the new data_tbl entries for person_id.

            print(" ADDING NEW data_tbl ENTRIES FOR ",person_id)
            for key in body:
                if (isinstance(body[key],str)):
                    tag = key
                    subtag = 'NULL'
                    seq = 0
                    value = body[key]
#                    print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                    amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                else:
#        elif ((isinstance(body[key],list)):
                    seq = 0
                    tag = key
                    for item in body[key]:
                        if (isinstance(item,dict)):
                            seq = 0
                            for key2 in item:
                                subtag = key2
                                value = item[key2]
#                                print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                                amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                                seq = seq + 1
                        else:
                            subtag = 'NULL'
                            value = body[key][seq]
#                            print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                            amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
                            seq = seq + 1

#   3. Extract the new DN list from data_tbl.

            print(" EXTRACTING new_dnlist FROM data_tbl ",person_id)
            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
#  The DnList may be for a Pi or a User.
            data = (person_id, 'DnList')
            try:
                cur.execute(sql,data)
#  Store new DnList in variable 'new_dnlist'.
                new_dnlist = cur.fetchall()
                conn.commit()
                cur.close()
                conn.close()
                print(" Extracted new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")
                print(" ")
#                ndn = 1
#                for nd in new_dnlist:
#                    print(ndn,nd)
#                    ndn = ndn + 1
#                print(" new_dnlist = ",new_dnlist)
#                print(" length = ",len(new_dnlist))
#                print(" ")
            except:
                print_psycopg2_exception(err)
                print(" Could not extract new DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")

#   3b.  Change DnList to UserDnlist/PiDnList for consistency.
#        (Must change tuple to list, change item assignment, and change list back to tuple because tuples are immutable.)

            tmplist = []
            for new in new_dnlist:
#                print(" before new = ",new)
                l_new = list(new)
#                print(" list new = ",l_new)
                if person_id.split('.')[0] == 'u':
#                    print(" Changing to UserDnList.")
                    l_new[3] = 'UserDnList'
                    l_new[5] = 0
                else:
                    print(" Changing to UserDnList.")
                    l_new[3] = 'PiDnList'
                    l_new[5] = 0
                new = tuple(l_new)
                tmplist.append(new)
            pri = new[2]
            new_dnlist = tmplist
            print(" after new = ",new)
            print(" ***** Modified new DnList:")
            ndn = 0
            for nd in new_dnlist:
                print(ndn,nd)
                ndn = ndn + 1

#  Change all seq values to 0 for renumbering of merged lists, and set packet_rec_id to that of the new list.
            tmplist = []
            for old in old_dnlist:
                l_old = list(old)
                l_old[5] = 0
                l_old[2] = pri
                old = tuple(l_old)
                tmplist.append(old)
            old_dnlist = tmplist
            print(" ***** Modified old DnList:")
            ndn = 0
            for nd in old_dnlist:
                print(ndn,nd)
                ndn = ndn + 1

#   4. Merge the new and old DN lists.

###            for old in old_dnlist:
###                new_dnlist.append(old)
#            print(" ***** Merged old and new lists before purging:")
#            ndn = 0
#            for nd in new_dnlist:
#                print(ndn,nd)
#                ndn = ndn + 1

#  Append elements in new list to old list that aren't in old list.
#  Both old_dnlist and new_dnlist are lists of tuples.

            print(" MERGING OLD AND NEW DnList INFO FOR ",person_id)
#  If the old and new lists aren't the same, combine their unique elements
            if old_dnlist != new_dnlist:
                print(" Old and new lists differ.  Merging.")
                mod_dnlist = []
                for nlist in new_dnlist:
#  Compare the value entry in each tuple in new_dnlist to all the value entries in the tuples in the old_dnlist to see if they match.
                    if not any(element == nlist for element in old_dnlist):
#  If there is no match append the new_dnlist tuple to the old_dnlist list.
#  This should create an old_dnlist that contains all the unique entries of both old_dnlist and new_dnlist.
                        old_dnlist.append(nlist)
            else:
                print(" Old and new lists identical.  No merging.")

            print(" ***** Merged old and new lists after purging:")
            ndn = 0
            for nd in old_dnlist:
                print(ndn,nd)
                ndn = ndn + 1

#  Redo the ordinal seq numbers for the merged list.
            nl = 0
            tmplist = []
            for old in old_dnlist:
                l_old = list(old)
                l_old[5] = nl
                old = tuple(l_old)
                tmplist.append(old)
                nl = nl + 1
            old_dnlist = tmplist

#   5. Delete the new DN list from data_tbl.

            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            try:
                sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
                data = (person_id, 'DnList')
                cur.execute(sql, data)
                conn.commit()
                print(" Deleted all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
            except:
                print_psycopg2_exception(err)
                print(" Unable to delete all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

#   6. Add the merged DN list to data_tbl.

            try:
                conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            except:
                print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
            cur = conn.cursor()
            nl = 0
            try:
                for data in old_dnlist:
#                    print(" data = ",data)
                    sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                    cur.execute(sql,data)
                    conn.commit()
                    nl = nl + 1
                cur.close()
                conn.close()
                print(" Inserted merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")
            except:
                print_psycopg2_exception(err)
                print(" Failed to insert merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")

#  Update transaction_tbl table value of state_id to 'completed'.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to database ",dbase)
        cur = conn.cursor()
        sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        try:
            cur.execute(sql,data)
            conn.commit()
            cur.close()
            conn.close()
            print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Update of 'state_id' for 'trans_rec_id' = ",trans_rec_id," failed for '",type_id,"' request.")

#  Construct the InformTransactionComplete(ITC) success packet

        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

        if (sendpackets == 'yes'):
            amie_client.send_packet(itc)
            print(" Sent 'request_user_modify' packet for 'trans_rec_id' = ",trans_rec_id,".")
        else:
            print(" Test run. No 'request_user_modify' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

# ********************* PROCESS request_user_merge REQUESTS *************************************

# PACKET TYPE
    if type_id == 'request_person_merge':

#        continue

        keep_global_id = packet.KeepGlobalID
        keep_person_id = packet.KeepPersonID
        keep_portal_login = packet.KeepPortalLogin
#            delete_global_id = packet.DeleteGlobalID
        delete_person_id = packet.DeletePersonID
        delete_portal_login = packet.DeletePortalLogin

#  Search for delete_person_id in local_info.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to database ",dbase)
        cur = conn.cursor()
        try:
            sql = "SELECT * from local_info WHERE person_id = %s"
            data = (delete_person_id,)
            cur.execute(sql,data)
            (d_fn,d_ln,d_email,d_person_id,d_project_id,d_rsn,d_gn,d_sa,d_su,d_sd,d_ed,d_ps,d_acs,d_uid,d_gid,d_cl,d_aid) = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            print(" Selected info for ",delete_person_id," in 'local_info'.")
        except:
            print_psycopg2_exception(err)
            print(" Could not find info for ",delete_person_id," in 'local_info'.")

#  Search for keep_person_id in local_info.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to database ",dbase)
        cur = conn.cursor()
        try:
            sql = "SELECT * from local_info WHERE person_id = %s"
            data = (keep_person_id,)
            cur.execute(sql,data)
            (k_fn,k_ln,k_email,k_person_id,k_project_id,k_rsn,k_gn,k_sa,k_su,k_sd,k_ed,k_ps,k_acs,k_uid,k_gid,k_cl,k_aid) = cur.fetchone()
            conn.commit()
            cur.close()
            conn.close()
            print(" Selected info for ",keep_person_id," in 'local_info'.")
        except:
            print_psycopg2_exception(err)
            print(" Could not find info for ",keep_person_id," in 'local_info'.")

#  Dump merge info into 'archive_merge' table.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to database ",dbase)
        cur = conn.cursor()
        try:
            sql = "INSERT INTO archive_merge (trans_rec_id,d_first,d_last,d_email,d_pid,d_proj_id,k_first,k_last,k_email,k_pid,k_proj_id,k_grant_no,time_merge) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            data = (trans_rec_id,d_fn,d_ln,d_email,d_person_id,d_project_id,k_fn,k_ln,k_email,k_person_id,k_project_id,k_gn,tstamp())
            cur.execute(sql,data)
            conn.commit()
            cur.close()
            conn.close()
            print(" Inserted merge data into 'archive_merge'.")
        except:
            print_psycopg2_exception(err)
            print(" Could not insert  merge data into 'archive_merge'.")

#  Remove info for deleted person 'd_person_id' info from data_tbl and local_info.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to 'data_tbl' attempting to delete info for ",person_id," for 'request_person_merge'.")
        cur = conn.cursor()
#  Remove from 'data_tbl'.
        try:
            sql = "DELETE FROM data_tbl WHERE person_id = %s"
            data = (delete_person_id,)
            print(" ***** Attempting to delete ",delete_person_id," from 'data_tbl'.")
            cur.execute(sql,data)
            conn.commit()
            print(" Deleted all lines for ",delete_person_id," from 'data_tbl' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to delete all lines for ",delete_person_id," from 'data_tbl' for '",type_id,"' request.")
#  Remove from 'local_info'.
        try:
            sql = "DELETE FROM local_info WHERE person_id = %s"
            data = (delete_person_id,)
            print(" ***** Attempting to delete ",delete_person_id," from 'local_info'.")
            cur.execute(sql,data)
            conn.commit()
            print(" Deleted line for ",delete_person_id," from 'local_info' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Unable to delete line for ",delete_person_id," from 'local_info' for '",type_id,"' request.")
        cur.close()
        conn.close()

#  Update transaction_tbl table value of state_id to 'completed'.

        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        except:
            print(" Error connecting to database ",dbase)
        cur = conn.cursor()
        sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        try:
            cur.execute(sql,data)
            conn.commit()
            cur.close()
            conn.close()
            print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed' for '",type_id,"' request.")
        except:
            print_psycopg2_exception(err)
            print(" Update of 'state_id' for 'trans_rec_id' = ",trans_rec_id," failed for '",type_id,"' request.")

#  Construct and send the InformTransactionComplete(ITC) success packet

        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

        if (sendpackets == 'yes'):
            amie_client.send_packet(itc)
            print(" Sent 'request_user_modify' packet for 'trans_rec_id' = ",trans_rec_id,".")
        else:
            print(" Test run. No 'request_user_modify' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

#        try:
#            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
#        except:
#            print(" Error connecting to database ",dbase)
#        cur = conn.cursor()
#        try:
#            sql = "UPDATE local_info SET first_name = %s, last_name = %s, email = %s WHERE person_id = %s"
#            data = (k_fn,k_ln,k_email,delete_person_id)
#            cur.execute(sql,data)
#            conn.commit()
#            cur.close()
#            conn.close()
#            print(" Replacing 'delete_person_id' with 'keep_person_id' in 'local_info'.")
#        except:
#            print_psycopg2_exception(err)
#            print(" Could not replace delete_person_id' with 'keep_person_id' in 'local_info'.")

# PACKET TYPE
    if type_id == 'inform_transaction_complete':

            print(" The 'information_transaction_complete' packets are skipped.")


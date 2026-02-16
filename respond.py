#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
import sys
import json
import time
import logging
import os.path
import subprocess
import random
import signal
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from amieclient import AMIEClient
from pprint import pprint

test = 0
diag = 0
inspect_packets = 0
single_tri = 0

script_name = sys.argv[0][2:]

#  Ascertain whether this is a production of development environment.
#  Find out which directory we're in.
ACCESS_HOME = os.getcwd()
if ACCESS_HOME == '/home/baum/AMIEDEV':
    amie_config = ACCESS_HOME + '/amiedev.ini'
    email_receivers = ['baum@tamu.edu']
    lock_file_path = ACCESS_HOME + 'accessdev_respond.lock'
    print(" DEVELOPMENT MODE!!!!!")
elif ACCESS_HOME == '/home/baum/AMIE':
    amie_config = ACCESS_HOME + '/amie.ini'
    email_receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
    lock_file_path = ACCESS_HOME + '/access_respond.lock'
    print(" PRODUCTION MODE!!!!")
else:
    print(" Not running in either '/home/baum/AMIE' or '/home/baum/AMIEDEV'. Exiting.")
    os.remove(lock_file_path)
    sys.exit()

######################################################
#  DEAL WITH OUTSTANDING LOCK FILES
delete_lock_file = 'y'
delete_choices = ['y','n']
file_gone = 'y'
pid_gone = 'y'
#  Check for lock file.  If it exists, delete it if running from cron or interactively.  If it doesn't, create one and write the PID to it.
#if os.path.isfile(lock_file_path):
#    print(lock_file_path)
#    with open(lock_file_path,'r') as f:
#        content = f.read().strip()
#        if content:
#            try:
#                pid = int(content)
#            except:
#  Delete lock file it it contains invalid PID.
#                print("Lock file ",lock_file_path," contains invalid PID data: ",content,". Deleting lock file.")
#                os.remove(lock_file_path)
##### EXIT
#                sys.exit()
#        else:
#  Delete lock file if it is empty.
#            print("Lock file ",lock_file_path," is empty.  Deleting lock file.")
#            os.remove(lock_file_path)
##### EXIT
#            sys.exit()
#        pid = 2727395
#  Lock file exists with valid PID.
#        print("A lock file '",lock_file_path,"' exists with PID ",pid,".")
#        if os.path.isdir(f'/proc/{pid}'):
#            result = subprocess.run(["ps", "-Flww", "-p", str(pid), "-f"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#            output_str = result.stdout.decode('utf-8')
#            print("The details of the process are:")
#            print(output_str)
#  Automatically delete lock file if the process was started with cron.
#            print("Deleting process ",pid," and lock file ",lock_file_path,".")
#            os.kill(pid, signal.SIGTERM)
#            os.remove(lock_file_path)
#            time.sleep(2)
#            if os.path.isfile(lock_file_path):
#                print("lock file = ",lock_file_path)
#                file_gone == 'n'
#            if os.path.isdir(f'/proc/{pid}'):
#                print("PID = ",pid)
#                pid_gone == 'n'
#            if file_gone == 'y' and pid_gone == 'y':
#                print("Lock file and and process successfully deleted.")
#            else:
#                print("Lock file and process not deleted. Try again. Exiting.")
#                sys.exit()
#        else:
#            print("Could not find process with PID ",pid,". Deleting lock file.")
#            os.remove(lock_file_path)
##### EXIT
#            sys.exit()

#  OPEN NEW LOCK FILE
print("No lock file 'access_respond.lock' for respond.py.  Creating one.")
file_lock = open(lock_file_path, 'w')
pid = os.getpid()
file_lock.write(str(pid))
print("Opened lock file: ",lock_file_path," with PID ",str(pid)) 

##### EXIT
#sys.exit()

log_file = ACCESS_HOME + '/amie.log'
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
        print("             nothing - run the program")
        print("             1 - run the demo program with additional diagnostic output")
        print("             t - print a summary of approved packets and exit")
        print("             s - process only the packet with the trans_rec_id argument that follows 's'")
        print("                 the trans_rec_id is obtained via the 't' argument")
        print("             All other arguments rejected.")
        print(" ")
        os.remove(lock_file_path)
        sys.exit()
    else:
#  Check for the only presently valid command-line argument "1".
        if sys.argv[1] == '1':
#            print(" Command-line argument is ",sys.argv[1])
            diag = int(sys.argv[1])
            print(" Printing diagnostics.")
        elif sys.argv[1] == 't':
            print(" It's manual override time!!!!")
            inspect_packets = 1
        elif sys.argv[1] == 's':
            single_tri = 1
            try:
                sys.argv[2]
            except:
                print(" The 's' argument requires a following 'trans_rec_id' number.  No number supplied. Exiting.")
                os.remove(lock_file_path)
                sys.exit()
        else:
#  Kick all invalid arguments ingloriously out of the script.
            print(" ")
            print(" The command-line argument entered '",sys.argv[1],"' is meaningless.  Exiting.")
            print(" Try one of ",help_list," for available valid arguments.")
            print(" ")
            os.remove(lock_file_path)
            sys.exit()

#  Set up email notifications
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
#        print(" Notification sent to:  ",','.join(email_receivers))

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
        except Exception as e:
            t = t + 1
            print(" Data for packet_rec_id ",packet_rec_id," tag ",tag," already exists in data_tbl.")
            print(" ERROR message: ",str(e))
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

start_time = time.time()

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

#check_for_previous_crash()
#print("LOCK EXIT")

#  Set list of allocation types for different processing modalities for 'request_project_create' packets.
allocation_types = ['new','renewal','supplement','extension','transfer','advance','adjustment']

#  Get all the available packets.
packets = amie_client.list_packets().packets
npack = len(packets)
print(' Number of unfiltered packets to process: ',npack)
if (npack == 0):
    print(" No packets to process.  Exiting.")
    os.remove(lock_file_path)
    sys.exit()

#  Filter out those eternally persistent 'inform_transaction_complete' packets.
filtered_packets = []
for packet in packets:
    pdict = json.loads(packet.json())
    type_id = json_extract(pdict,'type')[0]
    if type_id != 'inform_transaction_complete':
        filtered_packets.append(packet)
npack = len(filtered_packets)
print (" Number of filtered packets to process: ",npack)

if inspect_packets == 1:
#  Diagnostic for inspecting all packets before processing.
    print(" trans_rec_id         type_id        allocation_type    person_id     project_id      grant_number       last_name    access ID      SUA      resource     status")
    for packet in filtered_packets:
        body = packet._original_data['body']
#        print(" body = ",body)
        original_data = getattr(packet,'_original_data')
#        print(" original data = ",original_data)
#        print(" #################################################### ")
#        print(" body = ",body)
        trans_rec_id = packet.trans_rec_id
#  Find approval status of packet from trans_rec_id in 'approval' table.
        sql = "SELECT approval_status FROM approval WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
#  If packet not yet in 'local_info' skip and move to next packet.
        if len(results) == 0:
            print(" No entry for trans_rec_id ",trans_rec_id," in 'approval' table.  Not yet processed by 'dump_approvals.py'.  Skipping.")
            continue
#  If packet in 'local_info', snag 'approval_status'.
        else:
            appstatus = results[0][0]
        type_id = original_data['type']
        try:
            resource = packet.ResourceList[0].split(".")[0]
        except:
            resource = 'NA'
        if resource == 'faster':
            continue
        try:
            sua = packet.ServiceUnitsAllocated
        except:
            sua = 'NA'
        try:
            site_person_id = packet.SitePersonId
            access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']
        except:
            access_id = 'NA'
        try:
            allocation_type = packet.AllocationType
        except:
            allocation_type = 'NA'
        try:
            person_id = packet.PersonID
        except:
            person_id = 'NA'
        try:
            project_id = packet.ProjectID
        except:
            project_id = 'NA'
        try:
            grant_number = packet.GrantNumber
        except:
            grant_number = 'NA'
        try:
            last_name = packet.PiLastName
        except:
            try:
                last_name = packet.UserLastName
            except:
                last_name = 'NA'
        if appstatus == 'reject':
            continue
        if appstatus != 'stasis':
            print(f'{trans_rec_id :>12} {type_id :>22} {allocation_type :>12} {person_id :>15} {str(project_id) :>17} {str(grant_number) :>13} {str(last_name) :>15} {str(access_id) :>12} {str(sua) :>12} {str(resource) :>11} {str(appstatus) :>11}')
#        print(trans_rec_id," ",type_id," ",allocation_type," ",person_id," ",project_id," ",grant_number," ",last_name)
#        sys.exit()
    end_time = time.time()
    slurm_time = end_time - start_time
    print(" ")
    print("Script time: ",slurm_time)
    os.remove(lock_file_path)
    sys.exit()
else:
    print(" No packet inspection.")
#    sys.exit()

#  Looping through packets that aren't 'inform_transaction_complete' packets.
pnum = 1
keeptrack = 0

#if single_tri == 1:
#    selected_tri = int(sys.argv[2])
#selected_tri = 116854685
#selected_tri = 116862152
#selected_tri = 116862153
#selected_tri = 116862434
#selected_tri = 116855778
#selected_tri = 116864697

nselect = 0

for packet in filtered_packets:

    if keeptrack > 200:
        os.remove(lock_file_path)
        sys.exit()
    keeptrack = keeptrack + 1

#  Jump out of loop for everything but the selected trans_rec_id (selected_tri).
    trans_rec_id = packet.trans_rec_id
    if single_tri == 1:
        selected_tri = int(sys.argv[2])
        if trans_rec_id != selected_tri:
            nselect = nselect + 1
            print(" Packet #",nselect," (",trans_rec_id,") is not the selected trans_rec_id (",selected_tri,").  Skipping.")
            continue
        else:
            print(" Packet #",nselect," (",trans_rec_id,") is the selected trans_rec_id (",selected_tri,"). Processing.")
#            sys.exit()

#  These two statements extract exactly the same things from the 'packet.'
    original_data = getattr(packet,'_original_data')
    pdict = json.loads(packet.json())
#    print(" pdict = ",pdict)
#    print(" ")
#    print(" Original data: ",original_data)
#    print(" ")
#    print(" type = ",original_data['type'])

#  Get a list of attributes and values in an object.
#    print(" vars(packet) = ",vars(packet))
#  Get a list of attributes in an object.
#    print(" dir(packet) = ",dir(packet))
#    sys.exit()

#  Read cases from incoming stream.
#  The json.loads construct decodes the JSON into Python data structures.
    pdict = json.loads(packet.json())

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
#    print(" Packet type ",type_id,"' with transaction ID = '",trans_rec_id,"' pulled from queue.")

#    continue

#    sys.exit()

# ********************* CHECK REQUESTS AGAINST APPROVAL STATUS *************************************
#  Process packet  unless:
#  1. packet is not in the 'approval' table
#  2. packet is in 'approval' table but 'unapproved'
#  3. packet is set to 'completed' in 'transaction_tbl' (this filters out the eternally lingering 'inform_transaction_complete' packets)

#  Check if packet is in 'approval' table and grab allocation_type and new_proxy from table.
    sql = "SELECT approval_status,supplemental FROM approval WHERE trans_rec_id = %s"
    data = (trans_rec_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print(" No entry in 'approval' table for trans_rec_id = ",trans_rec_id,". Skipping to next packet.")
        pnum = pnum + 1
        continue
    else:
        approval_status = results[0][0]
#  The 'alloc_proxy' extracted from the 'supplemental' column in 'local_info' is of format: allocation_type-new_proxy, so both required
#  types of information are packed into a single variable.  This extracts both:
        alloc_proxy = results[0][1]
        if alloc_proxy != None:
            try:
                (allocation_type, new_proxy) = alloc_proxy.split('-')
            except:
                allocation_type = alloc_proxy
#  new_proxies = ['renewal','supplement','advance','transfer']
#        print(" allocation_type = ",allocation_type," new_proxy = ",new_proxy)
#  Check if packet is 'approved' in 'approval' table.
        if approval_status != 'approved':
            pnum = pnum + 1
            print(" Added to pnum for unapproved packet to make ",pnum)
            print(" Approval status for Transaction #",trans_rec_id," is: ",approval_status,". Skipping to next packet.")
            continue
#  Skip the packet if it is of type 'inform_transaction_complete'.
        else:
            if type_id == 'inform_transaction_complete':
                print(" Skipping 'inform_transaction_complete' packet.")
                pnum = pnum + 1
                continue
            else:
#  If packet approved go forth and process it.
                print("Approved packet of type ",type_id,"' with transaction ID = '",trans_rec_id,"' pulled from queue.")
#  Add 'ts_reply' and 'reply_status' values to 'approval' table.
                try:
                    sql = 'UPDATE approval SET ts_reply = %s, reply_status = %s WHERE trans_rec_id = %s'
                    data = (tstamp(), '1', trans_rec_id)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                except Exception as e:
                    print(" Could not add info to 'approval' table for trans_rec_id = ",trans_rec_id,". Skipping packet.")
                    print(" ERROR message: ",str(e))
                    pnum = pnum + 1
                    print(" Added to pnum for approved packet to make: ",pnum)
                    continue

#  Extract 'data' dictionary from packet.            
    body = packet._original_data['body']

    logging.info(' Processing transaction ID ' + str(trans_rec_id) + ' for ' + type_id + ' with timestamp ' + str(packet_timestamp) + ' on ' + tstamp())

#    print(" Proceeding with ",type_id," packet type.")

#    sys.exit()

    new_proxies = ['renewal','supplement','advance','transfer']

# ********************* PROCESS request_project_create REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
# This will process all approval requests with new_proxy = 1 to handle both new and proxy new allocation types.
    if type_id == 'request_project_create':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

#        if allocation_type == 'new':
#            print("This RPC packet has allocation type '",allocation_type,"'. Will process for local account establishment.")
#        elif allocation_type in new_proxies and new_proxy == 1:
#            print("This RPC packet has allocation type '",allocation_type,"' as proxy for 'new'. Will process for local account establishment.")

#  Extract needed information from packet.
        try:
            first_name = packet.PiFirstName
            last_name = packet.PiLastName
            grant_number = packet.GrantNumber
            user_global_id = packet.PiGlobalID
            resource_list = packet.ResourceList[0]
            cluster = resource_list.split('.')[0]
#  Create standard 'person_id' and 'project_id' to check for existing entry matches.
#  Check if the packet contains 'ProjectID'.  All allocation type variations that open new projects will not have it.
#  Packets that modify existing projects will have it.
            person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + user_global_id
            project_id = packet.ProjectID
            if project_id == None:
#  Changed back to .000 because ACCESS can't handle different project_id values for the same project on different resources.
                project_id = 'p.' + grant_number.lower() + '.000'
#                project_id = 'p.' + grant_number.lower() + '.' + cluster[:3]
                print("This RPC packet creates a new project.  A new local project name has been created.")
                print("The new 'project_id' is: ",project_id)
            else:
                print("This RPC packet will modify an existing project.  The local project name is extracted from the packet.")
                print("The existing 'project_id' is: ",project_id)
            new_service_units = packet.ServiceUnitsAllocated
            try:
                new_start_date = packet.StartDate
            except:
                new_start_date = 'NA'
            try:
                new_end_date = packet.EndDate
            except:
                new_end_date = 'NA'
        except Exception as e:
            msg1 = "Failed to read required data from " + type_id + " packet with trans_rec_id = " + str(trans_rec_id) + ". Skipping packet."
            msg2 = "ERROR message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            send_email(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            continue

#  Check to see if this person_id/project_id/cluster combination is in local_info.
        try:
            sql = "SELECT * FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
#            print("This person_id/project_id/cluster combination exists in local_info.")
        except Exception as e:
            msg1 = "Failed to extract data from 'local_info' for " + person_id + " on project " + project_id + " on resource " + cluster + ".  Skipping packet."
            msg2 = "ERROR message: " + str(e)
            send_email(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            print(msg1 + "\r\n" + msg2)

#  If not in local_info, skip packet.
        if (len(results) == 0):
            msg1 = "Project " + project_id + " with PI " + person_id + " for cluster " + cluster + " not in 'local_info'."
            msg2 = "This cannot be processed by 'respond.py' if it is not in 'local_info'."
            msg3 = "Skipping this packet and leaving it in the ACCESS queue until 'dump_approvals.py' properly processes it."
            send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
            pnum = pnum + 1
            continue

#  Extract all column values from 'local_info' for this person_id/project_id/cluster combination.
        (fn,ln,email,peid,prid,pi_remote_site_login,gn,slurm_account,service_units_allocated,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override) = results[0]

#  Reactivate project if proj_stat = inactive for this proxy request_project_reactivate packet.
        if proj_stat == 'inactive':
            try:
                sql = "UPDATE local_info SET proj_stat = 'active' WHERE project_id = %s AND cluster = %s"
                data = (project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                print("This is a proxy 'request_project_reactivate' packet. Changing 'proj_stat' value to 'active'.")
            except Exception as e:
                print("Problem setting project to 'active' for project ",project_id," on ",cluster,".")
                print("ERROR: ",str(e))
#            print("BUGGING OUT!")
#            sys.exit()
        else:
            print("This is not a proxy 'request_project_reactivate' packet. Not changing 'proj_stat' value.")

        print("RPC packet allocation type is '",allocation_type,"' with '",new_service_units,"' SU, start date '",new_start_date,"' and end date, '",new_end_date,"'.")
        if allocation_type in new_proxies and new_proxy == 1:
            print("This is a proxy request for a 'new' allocation type. Will create entry in 'local_info' for local project account.")

        new_proxies = ['renewal','supplement','advance','transfer']

        print("allocation_type = ",allocation_type," new_proxy = ",new_proxy)
#### EXIT
#        sys.exit()

        rcount = 0
# ##############################################################################################
#        if allocation_type == 'new' or (allocation_type in new_proxies and new_proxy == 1):
        if allocation_type == 'new':
# ##############################################################################################
#            print("#1 - Entering new_proxy = 1 section for 'renewal','supplement','advance','transfer' allocation types.")

            print("rcount = ",rcount)
            msg1 = "Processing '" + allocation_type + "' RPC for user '" + person_id + "' on project '" + project_id + "' on resource '" + cluster + "'."
            print(msg1)
            print("This is a request for creating a new project.")
#            if allocation_type != 'new':
#                print("This is a proxy request for creating a new project.")
            rcount = rcount + 1
### EXIT
#CRAP            sys.exit()

# ##############################################################################################
        elif allocation_type in new_proxies and new_proxy == 1:
# ##############################################################################################

            print("#1 - Entering new_proxy = 1 section for 'renewal','supplement','advance','transfer' allocation types.")
            msg1 = "Processing '" + allocation_type + "' RPC for user '" + person_id + "' on project '" + project_id + "' on resource '" + cluster + "'."
            print(msg1)
            print("This is a proxy request for creating a new project.")
### EXIT
#            sys.exit()

# ##############################################################################################
        elif allocation_type == 'extension':
# ##############################################################################################
# extension  - An extension extends the end date of an allocation. An extension should only be received
#                for an existing allocation.
#POO

            print("#2 - Entering new_proxy = 0 section for 'extension' allocation type.")
            print(" allocation_type = ",allocation_type," new_proxy = ",new_proxy)
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
#            sys.exit()
            msg1 = "Processing RPC with '" + allocation_type + "' allocation type for user '" + person_id + "' on project '" + project_id + "' on resource '" + cluster + "'."
            print(msg1)
            try:
                if test == 0:
#                   sql = "SELECT proj_stat FROM local_info WHERE project_id = %s AND cluster = %s"
#                   data = (project_id,cluster,)
#                   results = []
#                   amiedb_call(sql,data,script_name,results)
#                   proj_stat = results[0][0]
                    print("new_end_date = ",new_end_date," end_date = ",end_date)
#  This updates the end date for everybody on this project_id/cluster combination.
                    sql = "UPDATE local_info set end_date = %s where project_id = %s and cluster = %s"
                    data = (new_end_date,project_id,cluster,)                    
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Updated 'end_date' from '",str(end_date),"' to '",str(new_end_date),"'.")
#                    if project_stat == 'inactive':
#                        sql = "UPDATE local_info set proj_stat = 'active' where project_id = %s and cluster = %s"
#                        data = (project_id,cluster,)
#                        results = []
#                        amiedb_call(sql,data,script_name,results)
#                        print("Changed 'proj_stat' to 'active' for this proxy 'request_project_reactivate' packet.")
            except Exception as e:
                msg1 = "ERROR: Failed to update end date for 'extension' request for " + person_id + " on project " + project_id + " on resource " + cluster + ".  Skipping packet."
                msg2 = "ERROR message: " + str(e)
                if test == 0:
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                    continue
                else:
                    print(" TESTING: Email message would be:")
                    print(msg1 + "\r\n" + msg2)
# ##############################################################################################
        elif (allocation_type == 'transfer' or allocation_type == 'supplement' or allocation_type == 'advance') and new_proxy == 0:
# ##############################################################################################
            print("#3 -  Entering new_proxy = 0 for 'transfer', 'supplement' or 'advance' allocation types.")
            print(" allocation_type = ",allocation_type," new_proxy = ",new_proxy)
#   transfer   - A transfer type occurs as one of a pair of transactions – one negative, and one positive. A 
#                negative transfer for an unknown allocation is an error. Otherwise, the amount is deducted 
#                from the allocation. A positive transfer should be treated the same as a supplement.
#   supplement - A supplement adds SUs to an existing allocation (without changing the start or end dates).
#                A supplement can also be received for a project unknown to the RP or a machine not
#                previously on the project, in which case it should be handled as a new type, using the
#                provided SUs and start/end dates.
#   advance    - An advance is an allocation given in anticipation of a new or renewal allocation. It should
#                be treated as a supplement.

            msg1 = "Processing RPC with '" + allocation_type + "' allocation type for user '" + person_id + "' on project '" + project_id + "' on resource '" + cluster + "'."
            print(msg1)
# TRANS
#  Replace the 'start_date' and 'end_date' values in 'local_info' with the 'StartDate' and 'EndDate' values in the packet.
#  The old PDF docs explain that a positive transfer for an existing project should be treated as a supplement, which adds SUs to an existing
#  allocation without changing the start or end dates.  The packets that have been received beg to disagree with this, so we will perform
#  the replacement if start and end dates are included in the packet.
            if new_start_date != 'NA':
                try:
                    sql = "UPDATE local_info SET start_date = %s,end_date = %s where project_id = %s and cluster = %s"
                    data = (new_start_date,new_end_date,project_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results) 
                except Exception as e:
                    msg1 = "Problem updating the 'start_date/end_date' values in 'local_info' for allocation type " + allocation_type + " for " + person_id + " on project " + project_id + ".  Skipping packet."
                    msg2 = "ERROR message: " + str(e)
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING: Email message would be:")
                        print(msg1 + "\r\n" + msg2)
                    continue

# ##############################################################################################
        elif allocation_type == 'renewal' and new_proxy == 0:
# ##############################################################################################
#   renewal    - A renewal is a continuing award for an existing project. In a sense, it "replaces" the
#                previous year's allocation (as of the start date of the renewal). A renewal may be sent for a
#                machine that did not previously have an allocation within the project, since a PI can renew
#                onto different machines each year. In this case, the project may be unknown to the RP. A
#                renewal for a resource not previously within the project should be handled by the RP as a
#                new type, using the provided start/end dates.
#                One can renew an allocation onto a new resource which makes it functionally new.

            print("#4 - Entering new_proxy = 0 for 'renewal' allocation type.")
            print(" allocation_type = ",allocation_type," new_proxy = ",new_proxy)
            print(" Processing RPC with '",allocation_type,"' allocation type for user '",person_id,"' on project '",project_id,"' on resource '",cluster,"'.")            
            print(" This is not a proxy for starting a new project.")
#  Extract date portion of new allocation start date.
            new_start_date = start_date.date()
#  Obtain today's date.
            today = date.today()
            print(" New allocation start date is ",str(new_start_date)," and today is ",str(today),".")
            if new_start_date <= today:
                try:
                    print(" New start date ",str(new_start_date)," is earlier than or equal to today's date ",str(today),".  Processing now.")
                    new_allocation = float(service_units_allocated)
                    if test == 0:
                        sql = "UPDATE local_info SET service_units=%s,start_date=%s,end_date=%s WHERE person_id = %s AND project_id = %s and cluster = %s"
                        data = (new_service_units,start_date,end_date,person_id,project_id,cluster,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                    else:
                        print("TESTING: Skipped updating 'local_info' for test.")
                    msg1 = "Renewing project " + project_id + " under " + person_id + " by replacing previous year's allocation."
                    msg2 = "New allocation starts on " + str(new_start_date) + " with new " + str(new_allocation) + " service units."
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                    else:
                        print(" TESTING: Email message would be:")
                        print(msg1 + "\r\n" + msg2)
                except Exception as e:
                    msg1 = "ERROR: Failed to process " + allocation_type + " request for " + person_id + " on project " + project_id + " on resource " + cluster + ".  Skipping packet."
                    msg2 = "ERROR message: " + str(e)
                    if test == 0:
                        send_email(msg1 + "\r\n" + msg2)
                        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                        print(msg1 + "\r\n" + msg2)
                        continue
                    else:
                        print(" TESTING: Email message would be:")
                        print(msg1 + "\r\n" + msg2)
            else:
#  Extract new start date and notification status from 'rpc_packets'.
                sql = "SELECT start_date,notify FROM rpc_packets WHERE person_id = %s AND project_id = %s and cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                new_start_date = results[0][0]
                notify = results[0][1]
#  Send notification about delay the first time through.
#  The 'notify' column in 'rpc_packets' is set to 'N' after the first notification.
                if notify == 'Y':
                    today = date.today()
                    msg1 = " New start date " + new_start_date + " is later than today " + today + ". Postponing processing even upon approval. Skipping packet response."
                    if test == 0:
                        send_email(msg1)
                        logging.info(script_name + ": " + msg1)
                        print(msg1)
                    else:
                        print(" TESTING:  Email message would be:")
                        print(msg1)
#  Change notification status to 'N' to forestall further tediously redundant emails.
                    if test == 0:
                        sql = "UPDATE rpc_packets SET notify = 'N' WHERE person_id = %s AND project_id = %s and cluster = %s"
                        data = (person_id,project_id,cluster,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
#  Persist in skipping return packet processing until the dates match.
                continue
#############################################################################################
#  End of allocation types.
#############################################################################################

#  All effectively 'new' allocation type packets need to have their essential info dumped into data_tbl.
#  NOTE:  Even the non-'new' RPC packets probably need to have their info processed to update whatever might be new or different.
# =============== POPULATE data_tbl TABLE =============================
        packet_rec_id = packet.packet_rec_id
#        print(" Populating 'data_tbl' from RPC packet for user '",person_id,"' on project '",project_id,"' with access ID '",access_id,"'.")

#  ADD INFORMATION TO TABLE data_tbl
#  This cycles through all the entries in the 'body' portion of the JSON packet and transmogrifies
#  them into entries for the 'data_tbl` table.

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
                    if len(value) > 110:
                        value = value[0:100]
                    if diag > 0:
                        print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                    amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
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
                                amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
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
                            if len(value) > 110:
                                value = value[0:100]
                            if diag > 0:
                                print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                            amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
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
                        amiedb_data_tbl(person_id,project_id,packet_rec_id,tag,subtag,seq,value)
#                    print(" ")
                    seq = seq + 1
#  The key value is an unknown data type.
                else:
                    print(" Unknown data type for key: ",key)

# 
# -------------------- SEND notify_project_create PACKET ------------------------

        pfos_number = packet.PfosNumber
        pi_org_code = packet.PiOrgCode
        project_title = packet.ProjectTitle
        resource_list = body['ResourceList']
#        service_units_allocated = json_extract(pdict,'ServiceUnitsAllocated')[0]
#        start_date = json_extract(pdict,'StartDate')[0]

#  CONSTRUCT A notify_project_create PACKET FOR A REPLY
        try:
            npc = packet.reply_packet()
            npc.ProjectID = project_id
            npc.PiPersonID = person_id
            npc.PiRemoteSiteLogin = pi_remote_site_login
            npc.GrantNumber = grant_number
            npc.PfosNumber = pfos_number
            npc.PiOrgCode = pi_org_code
            npc.ProjectTitle = project_title
            npc.ResourceList = resource_list
            npc.ServiceUnitsAllocated = service_units_allocated
            npc.StartDate = start_date
#            msg1 = "Successfully constructed 'notify_project_create' packet for " + person_id + " on project " + project_id + " on resource " + cluster + " with access ID " + access_id + "."
#            print(msg1)

#  Dump response 'npc' packet to archive subdirectory.
            try:
                pref = 'npc'
                trans_rec_id = packet.trans_rec_id
                dumpfile = ACCESS_HOME + '/' + pref + '/' + pref + str(trans_rec_id) + ".json"
                with open(dumpfile, 'w') as f:
                    print(vars(npc), file=f)
#                print(" The NPC packet being sent for ",person_id," on project ",project_id," on resource ",cluster," has been written to the archive as: ",dumpfile,".")
            except Exception as e:
                msg1 = "Failed to archive " + dumpfile + " for " + person_id + " on project " + project_id + " on resource " + cluster + "."
                msg2 = "ERROR message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)

        except Exception as e:
            logging.error(' Failed to construct notify_project_create packet.')
            msg1 = "ERROR: Failed to construct 'notify_project_create' packet for " + person_id + " on project " + project_id + " on resource " + cluster + " with access ID " + access_id + ". Skipping packet."
            msg2 = "ERROR Message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
# commented due to email spamming over the weekend LMP 1Jul2023
            send_email(msg1 + "\r\n" + msg2)
            continue

        logging.info(' Sending notify_project_create response on ' + str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        if (sendpackets == 'yes'):
#  Attempt to send the notify_project_create packet.
            try:
                amie_client.send_packet(npc)
                msg1 = "Successfully sent 'notify_project_create' packet for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + "."
                print(msg1)
                send_email(msg1)
                logging.info(script_name + ": " + msg1)
                npc_sent = 1
            except Exception as e:
                msg1 = "ERROR: Failed to send 'notify_project_create' packet for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ". Skipping packet."
                msg2 = "Error message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue
#  ==================
#  UPDATE local_info
#  ==================
#  If packet successfully sent, change project and account statuses in 'local_info', and state ID in 'packet_tbl'.
#  Do not skip to next packet for errors in here.  They can be changed by hand in the appropriate tables.
            if npc_sent == 1:
                try:
# CRACK
                    proj_stat = 'active'
                    acct_stat = 'active'
                    sql = "UPDATE local_info SET proj_stat=%s, acct_stat=%s WHERE person_id = %s AND project_id = %s AND cluster = %s"
                    data = (proj_stat,acct_stat,person_id,project_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    msg1 = " Successfully changed 'proj_stat' and 'acct_stat' to 'active' in 'local_info' table for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + "."
#                    print(msg1)
                except Exception as e:
                    print_psycopg2_exception(err)
                    msg1 = "ERROR: Problem changing 'proj_stat' and 'acct_stat' to 'active' in 'local_info' table for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ", although the NPC packet was successfully sent."
                    msg2 = "ERROR message: " + str(e)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    send_email(msg1 + "\r\n" + msg2)
#  ==================
#  UPDATE packet_tbl
#  ==================
                try:
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    msg1 = " Changed 'state_id' to 'completed' in 'packet_tbl' for " + person_id + " on project " + project_id + " with access ID " + access_id + " and local user ID " + person_id + " on resource " + cluster + "."
#                    print(msg1)
                except Exception as e:
                    msg1 = "ERROR: Problem changing 'state_id' to 'completed' in 'packet_tbl' for " + person_id + " on project " + project_id + " with access ID " + access_id + " and local user ID " + person_id + " on resource " + cluster + ", although the NPC packet was successfully sent."
                    msg2 = "ERROR message: " + str(e)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    send_email(msg1 + "\r\n" + msg2)
        else:
            print(" Test run. No 'notify_project_create' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS data_project_create REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'data_project_create':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        print(" Processing 'data_project_create' (DPC) as final response to RPC packet with transaction ID ",trans_rec_id,".")

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
        version = '0.6.0'        
        packet_state = packet.packet_state
        outgoing_flag = packet.outgoing_flag
#  Find cluster from trans_rec_id since that info is not in the packet.
        try:
            sql = "SELECT cluster FROM approval WHERE trans_rec_id = %s"
            data = (str(trans_rec_id),)
            results = []
            amiedb_call(sql,data,script_name,results)
            cluster = results[0][0]
        except Exception as e:
            cluster = "NA"
            print("Could not obtain cluster information from 'local_info' for trans_rec_id = ",trans_rec_id,". Setting to 'NA' and continuing.")

#  CLUSTER PROBLEM
#  Extract 'access_id' and 'cluster' from 'local_info' using 'person_id' and 'project_id' in DPC packet.
#  QUESTION:  HOW DO WE KNOW WHICH person_id, project_id, cluster COMBINATION IS CORRECT?
        sql = "SELECT access_id FROM local_info WHERE person_id = %s AND project_id = %s and cluster = %s"
        data = (person_id,project_id,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
#        print("person_id = ",person_id," project_id = ",project_id," cluster = ",cluster," results = ",results)
#        sys.exit()
        access_id = results[0][0]
#        cluster = results[0][1]
#        print(" access_id = ",access_id," cluster = ",cluster)

        try:
#  Add packet_rec_id of this packet to packet_tbl.
            sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (packet_rec_id) DO NOTHING;"""
            data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state,outgoing_flag,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
#            print(" Inserted packet_rec_id = '",packet_rec_id,"' into 'packet_tbl' for '",type_id,"' packet for '",person_id,"' on project '",project_id,"'.")
        except Exception as e:
            msg1 = "ERROR: Failed to insert packet_rec_id " + packet_rec_id + " into 'packet_tbl' for " + type_id + " packet for " + person_id + " on project " + project_id + "."
            msg2 = "ERROR message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            send_email(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)

# This DnList must be added to/merged with the DnList already in the local data_tbl for this PI.
#  Process DnList in packet if it exists.  Skip if not.
        try:
            dnlist = packet.DnList
#            print(" DnList exists as: ",dnlist)
        except:
            print(" Problem extracting DnList from 'data_project_create' packet no. ",packet_rec_id," for ",person_id,". Setting DnList to empty.")
            dnlist = ""

#  Check if dnlist exists.  If not, skip DnList processing.
        if not dnlist:
            print(" No DnList in 'data_project_create' packet no. ",packet_rec_id," for ",person_id,". Skipping DnList processing.")
#  If DnList exists, process it.
        else:
            try:
#  Extract new DNs from the data_project_create packet.
                new_dnlist = []
                for dn in dnlist:
                    tup = (person_id,project_id,packet_rec_id,'PiDnList','NULL',0,dn)
                    new_dnlist.append(tup)

#  Extract previous DNs from data_tbl.
                sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
                data = (person_id, 'PiDnList')
                results = []
                amiedb_call(sql,data,script_name,results)
                old_dnlist = results
#                try:
#                    cur.execute(sql,data)
#                    old_dnlist = cur.fetchall()

                tmplist = []
                for old in old_dnlist:
                    l_old = list(old)
                    l_old[5] = 0
                    l_old[2] = packet_rec_id
                    old = tuple(l_old)
                    tmplist.append(old)
                old_dnlist = tmplist

                if old_dnlist != new_dnlist:
#            print(" Old and new lists differ.  Merging.")
                    mod_dnlist = []
                    for nlist in new_dnlist:
                        if not any(element == nlist for element in old_dnlist):
                            old_dnlist.append(nlist)
                else:
                    hodedo = 1
#                    print(" Old and new lists identical.  No merging.")

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
                sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
                data = (person_id, 'PiDnList')
                results = []
                amiedb_call(sql,data,script_name,results)
#                print(" Deleted all PiDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
#   6. Insert merged PiDnList lines.
                for data in old_dnlist:
                    sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    amiedb_call(sql,data,script_name,results)
            except Exception as e:
                msg1 = "Error processing DnList in 'data_project_create' packet for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                msg2 = "Non-critical error message: " + str(e)
                print(msg1)
                print(msg2)
                send_email(msg1 + "\r\n" + msg2)
                print(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)

        # send the ITC
        if sendpackets == 'yes':
#  Snag first_name, last_name and email from local_info.
            try:
                sql = "SELECT first_name,last_name,email FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
                data = (person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (first_name,last_name,email) = results[0]
                print("printing troika:")
                print("first_name = ",first_name," last_name = ",last_name," email = ",email)
                firstlastemail = 1
            except Exception as e:
                firstlastemail = 0

#  Construct and send the InformTransactionComplete(ITC) success packet
            try:
                itc = packet.reply_packet()
                itc.StatusCode = 'Success'
                itc.DetailCode = '1'
                itc.Message = 'OK'
                amie_client.send_packet(itc)
                itc_sent = 'y'
            except Exception as e:
                msg1 = "Problem sending ITC success packet for " + person_id + " on project " + project_id + " on cluster " + cluster + " with ACCESS ID " + access_id + ". Skipping to next packet."
                msg2 = "ERROR: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue
### CHANGE!
#  Extract further information from 'rpc_packets' to further explain the 'allocation_type' details.
            try:
                sql = "SELECT service_units,allocation_type,cur_all,charges,balance FROM rpc_packets WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (service_units,allocation_type,cur_all,charges,balance) = results[0]
                allocation_details = 1
            except Exception as e:
                print("Problem extracting 'allocation_type' from 'rpc_packets' for transaction ",trans_rec_id,".")
                print("ERROR: ",str(e))
                allocation_error = str(e)
                allocation_details = 0

            try:
                if firstlastemail == 0:
                    msg1 = " Successfully completed 'request_project_create' transaction for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ".  Could not obtain first name, last name, and email."
                else:
# CUT AND PASTE
# updated with additional printing for emailing 25Feb14 by Lisa Perez
                    msg1 = " Successfully completed 'request_project_create' transaction for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ".\n\n" + "bash " + cluster + "_notify " + email + " " + "'" + first_name + " " + last_name + "' " + person_id + " " + project_id.split('.')[1].upper()
                if allocation_details == 0:
                    msg2 = "Problem extracting 'allocation_type' and other data from 'rpc_packets'. ERROR: " + allocation_error
                else:
                    try:
                        msg2 = "Allocation type: " + str(allocation_type) + ", SU: " + str(service_units) + ", Current allocation: " + str(cur_all) + ", Charges: " + str(charges) + ", Balance: " + str(balance)
                    except Exception as e:
                        msg2 = "Allocation type details not available.  ERROR: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
#  Send ITC packet to indicate request_project_create transaction is complete.
            except Exception as e:
                msg1 = " Problem sending local completion message for 'request_project_create' transaction for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ".  ITC packet successfully sent."
                msg2 = " ERROR message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
 
#  Update tables if return packet successfully sent.
            if itc_sent == 'y':
#  Update transaction_tbl table value of state_id to 'completed'.
                try:
                    sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
                    data = (trans_rec_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    msg1 = " Changed 'state_id' to 'completed' in 'transaction_tbl' for trans_rec_id " + str(trans_rec_id) + " for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + "."
#                    print(msg1)
                except Exception as e:
                    msg1 = " Failed to change 'state_id' to 'completed' in 'transaction_tbl' for trans_rec_id " + str(trans_rec_id) + " for " + person_id + " on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ", although ITC packet successfully sent."
                    msg2 = " ERROR message: " + str(e)
                    print(msg1 + "\r\n" + msg2)
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                try:
                    packet_rec_id = packet.packet_rec_id
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    msg1 = " Changed 'state_id' to 'completed' in 'packet_tbl' for trans_rec_id " + str(trans_rec_id) + " for " + str(person_id) + " on project " + str(project_id) + " with access ID " + str(access_id) + " on resource " + str(cluster) + "."
#                    print(msg1)
                except Exception as e:
                    msg1 = " Failed to change 'state_id' to 'completed' in 'packet_tbl' for trans_rec_id " + str(trans_rec_id) + " for " + str(person_id) + " on project " + str(project_id) + " with access ID " + str(access_id) + " on resource " + str(cluster) + ", although ITC packet successfully sent."
                    msg2 = " ERROR message: " + str(e)
                    print(msg1 + "\r\n" + msg2)
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
        else:
            print(" Test run. No 'inform_transaction_complete' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# ********************* PROCESS request_account_create REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_account_create':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

#        print(" Starting 'request_account_create' processing for local account creation.")

        proxy = 0
        grant_number = packet.GrantNumber
        project_id = packet.ProjectID
        resource_list = packet.ResourceList[0]
        cluster = resource_list.split('.')[0]

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
#  The 'request_account_create' packet may not contain a ProjectID, so recreate it here.
        project_id = 'p.' + grant_number.lower() + '.000'
        access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']

#  Check to see if the project status is active.  If not, skip and don't activate account until project is activated.
        results2 = []
        sql = "SELECT proj_stat FROM local_info WHERE project_id = %s and pi = 'Y' and cluster = %s"
        data = (project_id,cluster,)
        amiedb_call(sql,data,script_name,results2)
        proj_stat = results2[0][0]
#        print(" proj status = ",projstat)
#        sys.exit()

        print("Processing 'request_project_create' request for user ",user_person_id," on project ",project_id," on cluster ",cluster,".")
#  Only set accounts to active if the project is active.
#  ==========================================
        if proj_stat == 'active':
#  ==========================================
            print("Project is active.  Will set 'acct_stat' to 'active'.")
#  Checking for 'request_account_create' being used as a proxy for 'request_account_reactivate'.
            try:
#  Checking for person_id/project_id/cluster combination in 'local_info'.
                sql = "SELECT acct_stat FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
#                sql = "SELECT * FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
                data = (user_person_id,project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  If acct_stat is either 'pending' or 'inactive', change to 'active'.  Do nothing if it is already set to 'active'.
                if len(results) != 0:
                    acctstat = results[0][0]
#                    (first_name,last_name,email,pid,project_id,remote_site_login,gn,sa,service_units_allocated,start_date,ed,projstat,acctstat,uid,gid,cluster,access_id,pi) = results[0]
#  If account exists and the acct_stat is either 'inactive' or 'pending', set to 'active'.
                    if acctstat == 'inactive' or acctstat == 'pending':
                        if acctstat == 'inactive':
                            print("Account status is 'inactive'.  Proxy reactivation request.")
                            proxy = 1
                        elif acctstat == 'pending':
                            print("Account status is 'pending'.  Account creation request.")
                        acct_stat = 'active'
                        try:
                            if test == 0:
                                sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s and project_id = %s and cluster = %s"
                                data = (acct_stat, user_person_id, project_id, cluster,)
                                results = []
                                amiedb_call(sql,data,script_name,results)
                            else:
                                print("TESTING: Did not set 'acct_stat' to 'active' in 'local_info' for test.")
                        except Exception as e:
                            msg1 = "ERROR: Cannot change 'acct_stat' of " + user_person_id + " on project " + project_id + " on resource " + cluster + " to " + acctstat + "."
                            msg2 = "ERROR: " + str(e)
                            if test == 0:
                                send_email(msg1 + "\r\n" + msg2)
                                print(msg1 + "\r\n" + msg2)
                                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                            else:
                                print("TESTING: Not sending email for test.  Message would be:")
                                print(msg1 + "\r\n" + msg2)
                    else:
                        print(" Account status of user ",user_person_id," on project ",project_id," is 'active'.  Not a proxy request.")
                else:
                    msg1 = "No match in 'local_info' for user " + person_id + " on project " + project_id + " on cluster " + cluster + ". Skipping."
                    print(msg1)
                    continue
            except Exception as e:
                msg1 = "ERROR: Problem with 'local_info' query for " + user_person_id + " on project " + project_id + " on resource " + cluster + " for 'request_account_create' packet."
                msg2 = "ERROR message: " + str(e)
                if test == 0:
                    send_email(msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                else:
                    print("TESTING: Not emailing error message for test.  Message would be:")
                    print(msg1 + "\r\n" + msg2)
                continue
            print("Project ",project_id," is active.  Activating account of project user ",user_person_id,".")
#  ==========================================
        elif proj_stat == 'inactive' or proj_stat == 'pending':
#  ==========================================
            print("Project ",project_id," is inactive or pending.  Cannot activate account until project is active.  Skipping.")
            continue

# =============== CHECK data_tbl TO SEE IF USER INFO IS ALREADY THERE ===============

        try:
            sql = "SELECT * FROM data_tbl WHERE person_id = %s and project_id = %s"
            data = (user_person_id,project_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            number_of_results = len(results)
#            print(" User ",user_person_id," with project_id = ",project_id," is already in the 'data_tbl' table.  Skipping table population. ")
        except Exception as e:
            print(" Problem with searching for user ",user_person_id," with project_id = ",project_id," in 'data_tbl' table.")
            print(" ERROR message: ",str(e))

# =============== POPULATE data_tbl TABLE =============================

        packet_rec_id = packet.packet_rec_id

#  Only add entries to data_tbl if this user has no entries in that table.
        if (number_of_results == 0):

#            print("Populating 'data_tbl' with contents of 'request_account_create' packet for ",user_person_id," on project ",project_id,".")
#            print(" ")

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
                    if len(value) > 110:
                        value = value[0:100]
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
                            if len(value) > 110:
                                value = value[0:100]
                            if diag > 0:
                                print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                            amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
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
                        amiedb_data_tbl(user_person_id,project_id,packet_rec_id,tag,subtag,seq,value)
#                    print(" ")
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
        try:
#            print(' Constructing the NAC packet.')
            nac = packet.reply_packet()
            nac.ProjectID = project_id
            nac.UserPersonID = user_person_id
            nac.UserRemoteSiteLogin = user_remote_site_login
            nac.ResourceList = packet.ResourceList
#            print(" ProjectID = ",project_id)
#            print(" UserPersonID = ",user_person_id)
#            print(" UserRemoteSiteLogin = ",user_remote_site_login)
#            print(" ResourceList = ",packet.ResourceList)
        except Exception as e:
            print("Failed to construct the NAC packet for ",user_person_id," on project ",project_id,". Skipping to next packet.")
            continue
#HODAD
#  It is somewhat ambiguous in the docs as to the necessity of the following in the reply.
#  That is, the docs tell us that a table lists tags that could be included, and that the ones in bold must be included.
#  In the table the subtags for these are in bold but not the tags.  Go figure.
#        resource_login = [{'Resource': 'test-resource1.tamu.xsede', 'Login': user_remote_site_login}]
#        nac.ResourceLogin = resource_login
#        nac.AcademicDegree = packet.AcademicDegree
#        print(" ResourceLogin = ",resource_login)
#        print(" AcademicDegree = ",packet.AcademicDegree)

#  Dump response 'nac' packet to archive subdirectory.
            try:
                pref = 'nac'
                trans_rec_id = packet.trans_rec_id
                dumpfile = ACCESS_HOME + '/' + pref + '/' + pref + str(trans_rec_id) + ".json"
                with open(dumpfile, 'w') as f:
                    print(vars(nac), file=f)
                print(" The NPC packet being sent for ",user_person_id," on project ",project_id," on resource ",cluster," has been written to the archive as: ",dumpfile,".")
#                print(" Wrote file: ",dumpfile," to archive.")
            except Exception as e:
                print(" Failed to write file ",dumpfile," to archive.")
                print(" ERROR message: ",str(e))
        except Exception as e:
            msg1 = "ERROR: Failed to construct 'notify_account_create' packet for " + user_person_id + " on project " + project_id + " on resource " + cluster + " with access ID " + access_id + ". Skipping packet."
            msg2 = "ERROR Message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            send_email(msg1 + "\r\n" + msg2)
            continue

        if (sendpackets == 'yes'):
# Attempt to send the notify_account_create (NAC) packet.
            try:
                amie_client.send_packet(nac)
                if proxy == 0:
                    msg1 = "Sent 'notify_account_create' packet account creation for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                else:
                    msg1 = "Sent 'notify_account_create' packet for proxy reactivation request for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                print(msg1)
                nac_sent = 'y'
#  Print error message if sending NAC packet fails. DOODY
            except Exception as e:
                msg1 = "Error sending 'notify_account_create' packet for " + user_first_name + " " + user_last_name + " with access ID " + access_id + " and local user ID " + user_person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                msg2 = "Error message: " + str(e)
                send_email(msg1 + "\r\n" + msg2)
                print(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                print(" Skipping to next packet.")
                if npack == pnum-1:
                    print(" No more packets.  Exiting.")
                continue
        else:
            print(" Test run. No 'notify_account_create' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

        if nac_sent == 'y':
#  Change 'state_id' to 'completed' in 'packet_tbl' table.
            try:
                sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                data = (str(packet_rec_id),)
                results = []
                amiedb_call(sql,data,script_name,results)
#                print(" Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
            except Exception as e:
                print(" Failed to change 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
                print(" ERROR message: ",str(e))

        pnum = pnum + 1

# ********************* PROCESS data_account_create REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'data_account_create':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

#  The XDCB replies to a 'notify_account_create' packet with a 'data_account_create' packet.
#  This contains all the DNs

        trans_rec_id = packet.trans_rec_id
        packet_rec_id = packet.packet_rec_id
        packet_id = packet.packet_id
        person_id = packet.PersonID
        project_id = packet.ProjectID
        type_id = json_extract(pdict,'type')[0]
        version = '0.6.0'
        packet_state = packet.packet_state
        outgoing_flag = packet.outgoing_flag
#  Find cluster from trans_rec_id since that info is not in the packet.
        try:
            sql = "SELECT cluster FROM approval WHERE trans_rec_id = %s"
            data = (str(trans_rec_id),)
            results = []
            amiedb_call(sql,data,script_name,results)
            cluster = results[0][0]
        except Exception as e:
            cluster = "NA"
            print("Could not obtain cluster information from 'local_info' for trans_rec_id = ",trans_rec_id,". Setting to 'NA' and continuing.")
#  Yank 'access_id' out of 'local_info'.
        try:
            sql = "SELECT access_id FROM local_info WHERE person_id = %s AND project_id = %s and cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            access_id = results[0][0]
        except:
            access_id = 'unknown'

        print(" Processing 'data_account_create' packet no. ",trans_rec_id," for ",person_id," on project ",project_id)

        try:
#  Add packet_rec_id of this packet to packet_tbl.
            sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (packet_rec_id) DO NOTHING;"""
            data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state,outgoing_flag,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
#            print(" Inserted packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
        except Exception as e:
            print(" Failed to insert packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
            print(" ERROR message: ",str(e))

#  Process DnList in packet if it exists.  Skip if not.

        try:
            dnlist = packet.DnList
#            print(" DnList exists as: ",dnlist)
        except:
            print(" Problem extracting DnList from 'data_account_create' packet no. ",packet_rec_id," for ",person_id,". Setting DnList to empty.")
            dnlist = ""

#  Check if dnlist exists.  If not, skip DnList processing.
        if not dnlist:
            print(" No DnList in 'data_account_create' packet no. ",packet_rec_id," for ",person_id,". Skipping DnList processing.")
#  If DnList exists, process it.
        else:
            try:
#  Extract new DNs from the data_account_create packet.
                dnlist = packet.DnList
                length_dnlist = len(dnlist)
                if length_dnlist == 0:
                    print(" No DnList in 'data_account_create' packet for ',person_id,'. Skipping DnList processing.")
                if length_dnlist != 0:
#                    print(" dnlist = ",dnlist)
                    new_dnlist = []
                    for dn in dnlist:
                        tup = (person_id,project_id,packet_rec_id,'UserDnList','NULL',0,dn)
                        new_dnlist.append(tup)

#  Extract previous DNs from data_tbl.
                    try:
                        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
                    except Exception as e:
                        print(" Error connecting to table 'data_tbl' when processing '",type_id,"' packet.")
                        print(" ERROR message: ",str(e))
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
#                    print(" old_dnlist size = ",osize)
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
                        hooray = 1
#                        print(" Old and new lists identical.  No merging.")

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
#                        print(" Deleted all UserDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
                    except:
                        print_psycopg2_exception(err)
                        print(" Unable to delete all UserDnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

                    try:
                        for data in old_dnlist:
                            sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                            cur.execute(sql,data)
                            conn.commit()
                            nl = nl + 1
#                        print(" Inserted all merged UserDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")
                    except:
                        print_psycopg2_exception(err)
                        print(" Unable to insert all merged UserDnList lines for pid = ",person_id," into table 'data_tbl' for '",type_id,"' request.")
                    cur.close()
                    conn.close()
            except Exception as e:
                msg1 = "Problem processing 'data_account_create' packet DNList for " + person_id + " on project " + project_id
                msg2 = "ERROR: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)

#  ADD THE LIST OF DNs TO A LOCAL TABLE

        # the data_account_create(DPC) packet has two functions:
        # 1. to let the site know that the account for the given ProjectID has been setup in the XDCDB
        # 2. to provide any new DNs for the user that were added after the RPC was sent
        # NOTE: a DPC does *not* have the resource. You have to get the resource from the RPC for the trans_rec_id

# proxy
# send the ITC
        if (sendpackets == 'yes'):
            try:
# INFORM_TRANS
#  Construct the InformTransactionComplete(ITC) success packet
                print("Constructing ITC packet.")
                itc = packet.reply_packet()
                itc.StatusCode = 'Success'
                itc.DetailCode = '1'
                itc.Message = 'OK'
                amie_client.send_packet(itc)

                msg1 = " Successfully completed 'request_account_create' transaction for " + person_id + " on project " + project_id + " with access ID " + access_id + ". ITC packet successfully sent."
                print(msg1)
                itc_sent = 'y'
            except Exception as e:
                msg1 = " Problem completing 'request_account_create' transaction for " + person_id + " on project " + project_id + " with access ID " + access_id + ".  Sending ITC packet failed.  Skipping to next packet."
                msg2 = " ERROR message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue
#  Send EMAIL notification that request_project_create transaction is complete.
            if (itc_sent == 'y'):
#  Update transaction_tbl table value of state_id to 'completed'.
                try:
                    sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
                    data = (trans_rec_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print(" Updated 'state_id' for 'data_account_create' packet for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
                except Exception as e:
                    print(" Failed to update 'state_id' for 'data_account_create' packet for 'trans_rec_id' = ",trans_rec_id," to completed.")
                    print(" ERROR message: ",str(e))
#  Change 'state_id' to 'completed' in 'packet_tbl' table.
                try:
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print(" Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")
                except Exception as e:
                    print(" Failed to change 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id = ",packet_rec_id,".")     
                    print(" ERROR message: ",str(e))
#  Extract first and last name from local_info using person_id.
                try:
                    sql = "SELECT first_name,last_name,email,access_id FROM local_info WHERE person_id = %s and cluster = %s"
#                    sql = "SELECT first_name,last_name,email,access_id,cluster FROM local_info WHERE person_id = %s"
                    data = (person_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    (first_name,last_name,email,access_id) = results[0]
#                    (first_name,last_name,email,access_id,cluster) = results[0]
                except Exception as e:
                    print(" First and last names not found in 'local_info' via 'person_id' search.")
                    print(" ERROR message: ",str(e))
                    first_name = 'unknown'
                    last_name = 'unknown'
                    email = 'unknown'
                    access_id = 'unknown'
#  Send email to notify sending of 'inform_transaction_complete' packet.
                try:
                    sql = "SELECT supplemental FROM approval WHERE trans_rec_id = %s"
                    data = (trans_rec_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    supplemental = results[0][0]
                    # modified msg1 to include commands needed to send email.  Modified by Lisa Perez 25Feb12
                    if supplemental == 'reactivate':
                        msg1 = "Finished reactivating account with 'request_account_create' transaction for " + person_id + "(" + first_name + " " + last_name + ") on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ".  All processes successfully completed.\n\n" + "bash " + cluster + "_notify " + email + " " + "'" + first_name + " " + last_name + "' " + person_id + " " + project_id.split('.')[1].upper()
                    else:
                        msg1 = "Finished creating account with 'request_account_create' transaction for " + person_id + "(" + first_name + " " + last_name + ") on project " + project_id + " with access ID " + access_id + " on resource " + cluster + ".  All processes successfully completed.\n\n" + "bash " + cluster + "_notify " + email + " " + "'" + first_name + " " + last_name + "' " + person_id + " " + project_id.split('.')[1].upper()
                    send_email(msg1)
                except Exception as e:
                    print(" Problem with sending email confirmation for ",type_id," completion for ",person_id," on project ",project_id," on resource ",cluster,".")
                    print(" ERROR message: ",str(e))
                    logging.info(" Problem with sending email confirmation for ",type_id," completion for ",person_id," on project ",project_id," on resource ",cluster,".")
                    logging.info(" ERROR message: ",str(e))
        else:
            print(" Test run. No 'inform_transaction_complete' packet sent for 'data_account_create' for 'trans_rec_id' = ",trans_rec_id,".")

        pnum = pnum + 1

# FOOBAR
# Packet with transaction ID = ' 116869461 ', type = ' data_account_create ' and packet_timestamp = ' 2023-05-23T17:35:48.273Z ' pulled from queue.
# allocation_type =  none  new_proxy =  0
# Approval status is ' approved '. Processing.
# Proceeding with  data_account_create  packet type.
# Processing 'data_account_create' packet no.  116869461  for  u.fd98591  on project  p.sta220004.000
# Inserted packet_rec_id =  233644597  into 'packet_tbl' for ' data_account_create ' packet.
# DnList exists as:  ['/C=US/O=National Center for Supercomputing Applications/CN=Francis Dang', '/C=US/O=Pittsburgh Supercomputing Center/CN=Francis Dang']
# Deleted all UserDnList lines for pid =  u.fd98591  from table 'data_tbl' for ' data_account_create ' request.
# Inserted all merged UserDnList lines for pid =  u.fd98591  into table 'data_tbl' for ' data_account_create ' request.
# Sent 'inform_transaction_complete' packet for 'data_account_create' for 'trans_rec_id' =  116869461 .
# Updated 'state_id' for 'data_account_create' packet for 'trans_rec_id' =  116869461  to 'completed'.
# Changed 'state_id' to 'completed' in 'packet_tbl' table for packet_rec_id =  233644597 .
# Problem with sending email confirmation for  data_account_create  completion for  u.fd98591  on project  p.sta220004.000 .
# ERROR message:  must be str, not NoneType

# ********************* PROCESS request_project_inactivate REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_project_inactivate':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        print(" Processing packet type:  'request_project_inactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        cluster_list = packet.ResourceList[0]
        resource_list = body['ResourceList']
        cluster = cluster_list.split('.')[0]
        project_id = packet.ProjectID
        print(' %%%%%%%%%%%%%%%%%%%%%%% project_id to inactivate: ',project_id)

#  Construct a 'notify_project_inactivate' reply packet.
        try:
            npi = packet.reply_packet()
            npi.ProjectID = project_id
            npi.ResourceList = resource_list
            msg1 = "Successfully constructed 'notify_project_inactivate' packet for " + project_id + " on resource " + cluster + ". Movin' on."
            print(msg1)
#  Dump response 'npi' packet to archive subdirectory.
            try:
                pref = 'npi'
                trans_rec_id = packet.trans_rec_id
                dumpfile = ACCESS_HOME + '/' + pref + '/' + pref + str(trans_rec_id) + ".json"
                with open(dumpfile, 'w') as f:
                    print(vars(npi), file=f)
            except Exception as e:
                msg1 = "Failed to archive 'notify_project_inactivate' packet " + dumpfile + " for project " + project_id + " on resource " + cluster + "."
                msg2 = "ERROR message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
        except Exception as e:
            msg1 = "ERROR: Failed to construct 'notify_project_inactivate' packet for " + project_id + " on resource " + cluster + ". Skipping packet."
            msg2 = "ERROR Message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            continue

        if (sendpackets == 'yes'):
            npi_sent = 0
            try:
#  First, try to send reply packet for NPI request.
                amie_client.send_packet(npi)
                print(" Successfully sent 'notify_project_inactivate' packet for 'trans_rec_id' = ",trans_rec_id,".")
                proj_stat = 'inactive'
                acct_stat = 'inactive'
                npi_sent = 1
            except Exception as e:
                msg1 = "ERROR: Failed to send 'notify_project_inactivate' packet for project " + project_id + " on resource " + cluster + ". Skipping packet."
                msg2 = "Error message: " + str(e)
                print(msg1 + "\r\n" + msg2)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue

            if npi_sent == 1:
#  Change 'proj_stat' and 'acct_stat' to 'inactive' in 'local_info' for all users of account.
                try:
#                    print("We got to the local_info update place.")
#                    sys.exit()
                    sql = "UPDATE local_info SET proj_stat = %s, acct_stat = %s WHERE project_id = %s AND cluster = %s"
                    data = (proj_stat,acct_stat,project_id,cluster,)
#                    sql = "UPDATE local_info SET proj_stat = %s WHERE project_id = %s AND cluster = %s"
#                    data = (proj_stat,project_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'proj_stat/acct_stat' to 'inactive' in 'local_info' for project_id = ",project_id," on resource ",cluster,".")
#                    print(" Changed 'proj_stat' to 'inactive' in 'local_info' for project_id = ",project_id," on resource ",cluster,".")
                except Exception as e:
                    msg1 = "ERROR: Failed to update 'proj_stat/acct_stat' to " + proj_stat + " for project " + project_id + " on resource " + cluster + "."
#                    msg1 = "ERROR: Failed to update 'proj_stat' to " + proj_stat + " for project " + project_id + " on resource " + cluster + "."
                    msg2 = "Cannot skip successfully sent return packet.  The 'local_info' table 'proj_stat' values must be manually adjusted."
                    msg3 = "Error message: " + str(e)
                    print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                    send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                print("CHECKING OUT!!!")
#                sys.exit()

#  Send email notification of project becoming inactive.  If we got this far without skipping out, we can send a triumphant message of success.
                sql = "SELECT first_name,last_name,email,person_id,access_id FROM local_info WHERE project_id = %s AND pi = 'Y' AND cluster = %s"
                data = (project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (first_name,last_name,email,person_id,access_id) = results[0]
                msg1 = "Sent 'notify_project_inactivate' packet for project " + project_id + " on resource " + cluster + "."
                msg2 = "Project PI is " + first_name + " " + last_name + " " + email + " with user ID " + person_id + " and ACCESS ID " + access_id + "."
                msg3 = "Changed 'proj_stat' and 'acct_stat' to '" + acct_stat + "' in 'local_info'."
                print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2 + "\r\n" + msg3)

# ********************* PROCESS request_project_reactivate REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_project_reactivate':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        trans_rec_id = packet.trans_rec_id
        print(" Processing packet type:  'request_project_reactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource = packet.ResourceList[0]
        project_id = packet.ProjectID
        person_id = packet.PersonID

# SP: reactivate the project and the PI account on the project (but no other accounts)

        npr = packet.reply_packet()
        if (sendpackets == 'yes'):
#  First, send reactivation NPR packet.
            try:
                amie_client.send_packet(npr)
                print(" Sent 'request_project_reactivate' packet for 'trans_rec_id' = ",trans_rec_id,".")
                proj_stat = 'active'
                acct_stat = 'active'
#  Next, change project status to 'active' in 'local_info' for all users on project.
                try:
                    sql = "UPDATE local_info SET proj_stat = %s WHERE project_id = %s"
                    data = (proj_stat,project_id)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#  Set PI account status to 'active'.
                    print(" Changed 'proj_stat' to 'active' in 'local_info' table for all project ",project_id," users.")
                except Exception as e:
                    print(" Problem changing 'proj_stat' to 'active' in 'local_info' table for all project ",project_id," users.")
                    print(" ERROR: ",str(e))
                    continue
#  Change account status of PI to 'active', but not for non-PI users on project.
                try:
                    sql = "UPDATE local_info SET acct_stat = %s WHERE project_id = %s and person_id = %s"
                    data = (proj_stat,project_id,person_id)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'acct_stat' to 'active' in 'local_info' table for PI ",person_id," on project ",project_id,".")
                except Exception as e:
                    print(" Problem changing 'acct_stat' to 'active' in 'local_info' table for PI ",person_id," on project ",project_id,".")
                    print(" ERROR: ",str(e))
                    continue
            except Exception as e:
                print(" Problem with sending 'notify_project_reactivate' for 'trans_rec_id' = ",trans_rec_id,". Skipping packet.")
                print(" ERROR message: ",str(e))
                continue
        else:
            print(" Test run. No 'request_project_reactivate' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

# ********************* PROCESS request_account_inactivate REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_account_inactivate':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

# SP: send a 'notify_account_inactivate' packet - DONE
# SP: inactivate a single account - DONE

#  TYPICAL RAI PACKET:
#pdict =  {'DATA_TYPE': 'Packet', 'type': 'request_account_inactivate', 'body': {'ProjectID': 'p.ibt228285.000', 'PersonID': 'u.ae57432', 'ResourceList': ['test-resource1.tamu.xsede'], 'Comment': 'amie_test', 'AllocatedResource': 'test-resource1.tamu.xsede'}, 'header': {'packet_rec_id': 222153030, 'packet_id': 1, 'transaction_id': 93, 'trans_rec_id': 111135872, 'expected_reply_list': [{'type': 'notify_account_inactivate', 'timeout': 30240}], 'local_site_name': 'TGCDB', 'remote_site_name': 'TAMU', 'originating_site_name': 'TGCDB', 'outgoing_flag': 1, 'transaction_state': 'in-progress', 'packet_state': 'in-progress', 'packet_timestamp': '2022-03-25T21:36:05.173Z'}}

#        print(" Processing packet type:  'request_account_inactivate' for 'trans_rec_id' = ",trans_rec_id,".")
        project_id = packet.ProjectID
        person_id = packet.PersonID
        resource_list = packet.ResourceList[0]
        cluster = resource_list.split('.')[0]
        packet_rec_id = packet.packet_rec_id
#  Get 'first_name', 'last_name' and 'access_id' from 'local_info' using 'person_id', 'project_id', 'cluster' triple.
        sql = "SELECT first_name,last_name,access_id FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
        data = (person_id,project_id,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if len(results) == 0:
            print("No results from 'local_info' for ",person_id," ",project_id," ",cluster)
            continue
#        print(results)
#        print(person_id,project_id,cluster)
#        sys.exit()
        (first_name,last_name,access_id) = results[0]
#  Set 'acct_stat' to inactive in 'local_info'.
        acct_stat = 'inactive'
        try:
#  Update 'acct_stat' to 'inactive' for specific person_id/project_id/cluster combination.
            sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s AND project_id = %s AND cluster = %s"
            data = (acct_stat,person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            stat_change = 'yes'
        except Exception as e:
            msg1 = "Problem changing 'acct_stat' to 'inactive' for user " + person_id + " on project " + project_id + " on resource " + cluster + "."
            msg2 = "ERROR message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            send_email(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            continue
#  Send packet ONLY if 'acct_stat' is successfully changed to 'inactive'.
        if (sendpackets == 'yes'):
            if stat_change == 'yes':
                nai = packet.reply_packet()
                try:
                    amie_client.send_packet(nai)
#POO                    print("Sent 'notify_account_inactivate' packet for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")
                    sent_nai = 'yes'
                except Exception as e:
                    msg1 = "ERROR: Problem sending 'notify_account_inactivate' for 'trans_rec_id' = " + str(trans_rec_id) + ". Skipping packet."
                    msg2 = "ERROR: User " + first_name + " " + last_name + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + "."
                    msg3 = "ERROR: " + str(e)
                    print(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
#                    send_email(msg1 + "\r\n" + msg2 + "\r\n" + msg3)
                    continue
#  Update tables and send email ONLY if both packet sending and 'acct_stat' changing are successful.
        if (sent_nai == 'yes' and stat_change == 'yes'):
            try:
                sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
#                print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
            except Exception as e:
                print(" Failed to update 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed'.")
                print(" ERROR message: ",str(e))
#  Update packet_tbl table value of state_id to 'completed'.
            try:
                sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                data = (str(packet_rec_id),)
                results = []
                amiedb_call(sql,data,script_name,results)
#                print(" Updated 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")
            except Exception as e:
                print(" Failed to update 'state_id' for 'packet_rec_id' = ",packet_rec_id," to 'completed'.")
                print(" ERROR message: ",str(e))
#  Find first and last names from person_id.
#            try:
#                sql = "SELECT first_name,last_name,access_id,cluster FROM local_info WHERE person_id LIKE %s"
#                data = (person_id,)
#                results = []
#                amiedb_call(sql,data,script_name,results)
#                (first_name,last_name,access_id,cluster) = results[0]
#            except Exception as e:
#                print(" First and last names not found in 'local_info' via 'person_id' search.")
#                print(" ERROR message: ",str(e))
#                first_name = 'unknown'
#                last_name = 'unknown'
#  Send email notification of account becoming inactive.
            try:
                msg1 = "User " + first_name + " " + last_name + " with ID " + person_id + " on Project ID " + project_id + "with access ID " + access_id + " on resource " + cluster + " has been set to inactive."
                send_email(msg1)
                print(msg1)
#                print(" Sent email confirmation for user ",first_name,last_name," with ID ",person_id," on project ",project_id," being set to inactive.")
            except Exception as e:
                print(" Unable to send email confirmation for user ",first_name,last_name," with ID ",person_id," on project ",project_id," being set to inactive.")
                print(" ERROR message: ",str(e))

# ********************* PROCESS request_account_reactivate REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_account_reactivate':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        print(" Processing packet type:  'request_account_reactivate' for 'trans_rec_id' = ",trans_rec_id,".")

        resource_list = packet.ResourceList[0]
        cluster = resource_list.split('.')[0]
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
                    sql = "UPDATE local_info SET acct_stat = %s WHERE person_id = %s AND project_id = %s AND cluster = %s"
                    data = (acct_stat,person_id,project_id,cluster,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'acct_stat' to 'active' in 'local_info' table for project ID #",project_id,".")
                except Exception as e:
                    print(" Problem with changing 'acct_stat' to 'active' in 'local_info' table for project ID #",project_id,".")
                    print(" ERROR message: ",str(e))
            except Exception as e:
                print(" Problem with sending 'notify_account_reactivate' for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")
                print(" ERROR message: ",str(e))

        else:
            print(" Test run. No 'notify_account_reactivate' packet sent for 'trans_rec_id' = ",trans_rec_id," for project ID = ",project_id,".")

# ********************* PROCESS request_user_modify REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_user_modify':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

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
#        print("Skipping 'request_user_modify' temporarily.")
#        continue
        person_id = packet.PersonID
        action_type = packet.ActionType
        first_name = packet.FirstName
        last_name = packet.LastName
        packet_rec_id = packet.packet_rec_id

#        print(" packet_rec_id = ",packet_rec_id)
#        sys.exit()

#  Do what needs to be done for both 'delete' (completely) and 'replace' (partially).

#  Process DnList in packet if it exists.  Skip if not.
        try:
            dnlist = packet.DnList
            print(" DnList exists as: ",dnlist)
        except:
            try:
                packet_rec_id = packet.packet_rec_id
                print(" Problem extracting DnList from 'request_user_modify' packet no. ",packet_rec_id," for ",person_id,". Setting DnList to empty.")
            except:
                print(" Problem extracting DnList from 'request_user_modify' for ",person_id,". Setting DnList to empty.")
            dnlist = ""

        if not dnlist:
            print(" No DnList in 'request_user_modify' packet no. ",packet_rec_id," for ",person_id,". Skipping DnList processing.")
#  If DnList exists, process it.
        else:
#  Extract the old DnList from 'data_tbl' for the user info to be deleted/replaced.
            sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
#  The DnList may be for a Pi (pi.*) or a User (u.*).
            if person_id.split('.')[0] == 'u':
                data = (person_id,'UserDnList')
            else:
                data = (person_id,'PiDnList')
            results = []
            amiedb_call(sql,data,script_name,results)
            old_dnlist = results
            old_dnlist_length = len(old_dnlist)

#  Store old DnList in variable 'old_dnlist'.
#                old_dnlist = cur.fetchall()
#                print(" Extracted old DnList info from 'data_tbl' for ",person_id," for '",type_id,"' request.")

#  Extract the ProjectID from old_dnlist since it is not contained within the 'request_user_modify' packet.
            try:
                if old_dnlist_length == 0:
                    (x1,project_id,x2,x3,x4,x5,x6) = old_dnlist[0]
            except:
                print(" Problem with DnList machinations in 'request_user_modify`.")

#  For delete, the only optional tag will be DnList which will specify DNs that must be deleted from the grid mapfile.

#  For action_type = 'delete' for 'request_user_modify':
#   1. Extract as delete_dnlist from the incoming packet the list of DNs to be deleted from 'data_tbl'.
#   2. Place the old_dnlist - delete_dnlist tuple list into mod_dnlist.
#   3. Remove the old_dnlist entries from data_tbl.
#   4. Add the mod_dnlist entries to data_tbl.

################################################################################################
#  ACTION_TYPE = DELETE
################################################################################################
        if action_type == 'delete':
            print(" PROCESSING delete ACTION for request_user_modify.")

            if not dnlist:
                print(" Skipping 'delete' action for DnList.")
            else:
                try:
                    mod_dnlist = []
#  Extract from the packet the list of DNs to be deleted.
                    delete_dnlist = packet.DnList
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
                    sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
                    if person_id.split('.')[0] == 'u':
                        data = (person_id,'UserDnList')
                        print(" Deleting UserDnList entries in data_tbl.")
                    else:
                        data = (person_id,'PiDnList')
                        print(" Deleting PiDnList entries in data_tbl.")
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Deleted all *DnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
#                        print(" Unable to delete all *DnList lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

#  Add the mod_dnlist entries to 'data_tbl'.

                    print(" ADDING mod_dnlist TO data_tbl FOR ",person_id)
                    nl = 0
                    for data in mod_dnlist:
                        print(" data = ",data)
                        sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        nl = nl + 1
                    print(" Inserted merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")
#                        print(" Failed to insert merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")

                except Exception as e:
                    msg1 = "Problem processing DnList for 'request_user_modify' delete action for user ID " + person_id + " on project " + project_id + "."
                    msg2 = "Error message: ",str(e)
                    send_email(msg1 + "\r\n" + msg2)

#  For action_type = 'replace' for 'request_user_modify':
#   1. Delete all the previous data_tbl entries for person_id.
#   2. Add all the new data_tbl entries for person_id.
#   3. Extract the new DN list from data_tbl.
#   4. Merge the new and old DN lists.
#   5. Delete the new DN list from data_tbl.
#   6. Add the merged DN list to data_tbl.

################################################################################################
#  ACTION_TYPE = REPLACE
################################################################################################
#        if action_type == 'replace':
        else:
            print("Processing 'request_user_modify' request with action type 'replace'.")

            packet_rec_id = packet.packet_rec_id

#   1. Delete all the previous data_tbl entries for person_id.
            print("1. Deleting previous 'data_tbl' entries for user ",person_id,".")
#  If the 'person_id' is in 'data_tbl', move on to deletions.  If not, skip packet.
            try:
                sql = "SELECT * from data_tbl WHERE person_id = %s"
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
            except Exception as e:
                msg1 = "Problem finding info for " + person_id + " in 'data_tbl' for " + type_id + " packet.  Skipping packet."
                msg2 = "ERROR: " + str(e)
                send_email(msg1 + "\r\n" + msg2)
                print(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                continue
#  If 'person_id' is in the table, remove all the information already present.  If not, skip packet.
            if len(results) != 0:
                try:
                    sql = "DELETE FROM data_tbl WHERE person_id = %s"
                    data = (person_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Deleted all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
                except Exception as e:
                    msg1 = "Problem deleting lines for user " + person_id + " from 'data_tbl' for " + type_id + " packet.  Skipping packet."
                    msg2 = "ERROR: " + str(e)
                    send_email(msg1 + "\r\n" + msg2)
                    print(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                    continue
            else:
                msg1 = "No entries for user " + person_id + " in 'data_tbl'.  Skipping packet."
                send_email(msg1)
                print(msg1)
                logging.info(script_name + ": " + msg1)
                continue

#   2. Add all the new data_tbl entries for person_id.
            print("2. Adding new 'data_tbl' entries for user ",person_id,".")
            for key in body:
                if (isinstance(body[key],str)):
                    tag = key
                    subtag = 'NULL'
                    seq = 0
                    value = body[key]
#                    print(" Calling values: ",user_person_id,project_id,packet_rec_id,key,subtag,seq,value)
                    project_id = 'NA'
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
            if not dnlist:
                print(" Skipping 'replace' action processing of DnList.")
            else:
                try:
                    print(" EXTRACTING new_dnlist FROM data_tbl ",person_id)
                    sql = "SELECT * FROM data_tbl WHERE person_id = %s AND tag = %s"
                    data = (person_id, 'DnList')
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    new_dnlist = results
#  Store new DnList in variable 'new_dnlist'.
#                        new_dnlist = cur.fetchall()

#   3b.  Change DnList to UserDnlist/PiDnList for consistency.
#        (Must change tuple to list, change item assignment, and change list back to tuple because tuples are immutable.)

                    tmplist = []
                    for new in new_dnlist:
                        print(" before new = ",new)
                        l_new = list(new)
                        print(" list new = ",l_new)
                        if person_id.split('.')[0] == 'u':
                            print(" Changing to UserDnList.")
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

#  Append elements in new list to old list that aren't in old list.
#  Both old_dnlist and new_dnlist are lists of tuples.

#   4. Merge the new and old DN lists.
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

                    sql = "DELETE FROM data_tbl WHERE person_id = %s AND tag = %s"
                    data = (person_id, 'DnList')
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Deleted all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")
#                        print(" Unable to delete all lines for pid = ",person_id," from table 'data_tbl' for '",type_id,"' request.")

#   6. Add the merged DN list to data_tbl.

                    nl = 0
                    for data in old_dnlist:
#                            print(" data = ",data)
                        sql = "INSERT INTO data_tbl (person_id,project_id,packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s, %s, %s);"
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        nl = nl + 1
                    print(" Inserted merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")
#                        print(" Failed to insert merged DnList into 'data_tbl' for ",person_id," for '",type_id,"' request.")

                except Exception as e:
                        msg1 = "Error processing DnList for 'request_user_modify' replace for " + first_name + " " + last_name + " and local user ID " + person_id + " and 'trans_rec_id' = " + str(trans_rec_id) + "."
                        msg2 = "Non-critical error message: " + str(e)
                        print(msg1)
                        print(msg2)
                        send_email(msg1 + "\r\n" + msg2)

#  Update 'local_info' if changed values include 'first_name', 'last_name', 'email' or 'access_id'.
#
#  Create list of tags in 'data_changes_tbl' for given 'trans_rec_id'.
        sql = "SELECT tag FROM data_changes_tbl WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        tags = []
        cnt = 0
        for tag in results:
            tags.append(results[cnt][0])
            cnt = cnt + 1
#  Dictionary of tags relevant to 'local_info'.
        print("Ascertaining if 'local_info' changes required.")
        tag_dict = {'FirstName':'first_name','LastName':'last_name','Email':'email'}
        local_tags = ['FirstName','LastName','Email']
#  Loop over changed tags in packet.
        for tag in tags:
#            print("tag = ",tag)
#  Check if tag is one requiring a change in 'local_info'.
            info_changes = 0
            if tag in local_tags:
                try:
                    info_changes = info_changes + 1
                    print("Processing tag: ",tag)
#  Find old and new values for tag in 'data_changes_tbl'.
                    sql = "SELECT person_id,old_value,new_value FROM data_changes_tbl WHERE trans_rec_id = %s AND tag = %s"
                    data = (trans_rec_id,tag,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    (person_id,old_value,new_value) = results[0]
#  Find present value of tag in 'local_info'.
                    try:
                        sql = "SELECT " + tag_dict[tag] + " FROM local_info WHERE person_id = %s"
                        data = (person_id,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        old_tag_value = results[0][0]
                    except Exception as e:
                        print("Could not find ",tag_dict[tag]," value for ",person_id," in 'local_info'. Skipping")
                        continue
                    if old_tag_value == new_value:
                        print("The ",tag_dict[tag]," value for ",person_id," has already been changed from ",old_value," to ",new_value,".")
                    elif old_tag_value == old_value:
#  Update value in 'local_info' if everything kosher.
                        sql = "UPDATE local_info SET " + tag_dict[tag] + " = '" + new_value + "' WHERE person_id = %s AND " + tag_dict[tag] + " = %s"
                        data = (person_id,old_value,)
                        results = []
                        amiedb_call(sql,data,script_name,results)
                        print("The ",tag_dict[tag]," value for ",person_id," in 'local_info' has been changed from ",old_value," to ",new_value,".")
                    else:
                        print("Column ",tag_dict[tag]," value in 'local_info' matches neither old or new value in packet.")
                except Exception as e:
                    msg1 = "Problem changing 'local_info' " + tag + " value for 'request_user_modify' packet for " + person_id + "."
                    msg2 = "Error: " + string(e)
                    send_email(msg1 + "\r\n" + msg2)

            print("Total of ",info_changes," changes to 'local_info'.")
#            sys.exit(
#        sys.exit()

#  Update transaction_tbl table value of state_id to 'completed'.

        try:
            sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed' for '",type_id,"' request.")
        except Exception as e:
            print(" Failed to update 'state_id' for 'trans_rec_id' = ",trans_rec_id," for '",type_id,"' request.")
            print(" ERROR: ",str(e))

#  Construct the InformTransactionComplete(ITC) success packet

        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(itc)
                print(" Sent 'inform_transaction_complete' packet in reply to 'request_user_modify' for 'trans_rec_id' = ",trans_rec_id,".")
                msg = "Successfully completed the request to modify user ID " + person_id + "."
                send_email(msg)
            except Exception as e:
                print(" Failed to send 'inform_transaction_complete' packet in reply to 'request_user_modify' for 'trans_rec_id' = ",trans_rec_id,".")
                msg1 = "The attempt to send an ITC packet in reply to 'request_user_modify' for  user ID " + person_id + " has failed."
                msg2 = "Error message: ",str(e)
                send_email(msg1 + "\r\n" + msg2)
        else:
            print(" Test run. No 'request_user_modify' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

#        print("EXITING!!")
#        sys.exit()

# ********************* PROCESS request_user_merge REQUESTS *************************************
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'request_person_merge':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

#        continue

        k_global_id = packet.KeepGlobalID
        k_person_id = packet.KeepPersonID
        person_id = k_person_id
        k_portal_login = packet.KeepPortalLogin
        d_global_id = packet.DeleteGlobalID
        d_person_id = packet.DeletePersonID
        d_portal_login = packet.DeletePortalLogin

#  Search for delete_person_id in local_info.

        try:
            sql = "SELECT first_name,last_name,email,project_id,remote_site_login,uid,gid,access_id from local_info WHERE person_id = %s"
            data = (d_person_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            (d_first,d_last,d_email,d_project_id,d_login,d_uid,d_gid,d_access_id) = results[0]
            conn.commit()
            cur.close()
            conn.close()
            print("Obtained info for person_id/global_id/access_id values to be deleted in 'local_info'.")
        except Exception as e:
            print(" Could not find entry for merged info to be deleted for local person ID ",d_person_id," in 'local_info'.  Skipping.")
            print(" ERROR: ",str(e))
            continue

#  Search for keep_person_id in local_info.

        try:
            sql = "SELECT first_name,last_name,email,project_id,remote_site_login,uid,gid,access_id from local_info WHERE person_id = %s"
            data = (k_person_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            (k_first,k_last,k_email,k_project_id,k_login,k_uid,k_gid,k_access_id) = results[0]
            print(" Selected info for person_id/global_id/access_id values to be retained in 'local_info'.")
        except Exception as e:
            print(" Could not find entry for merged info to be retained for local person ID ",k_person_id," in 'local_info'. Skipping.")
            print(" ERROR: ",str(e))
            continue

#  Dump merge info into 'archive_merge' table.

        try:
            sql = "INSERT INTO merge_archive (trans_rec_id,d_first,d_last,d_email,d_person_id,d_project_id,d_uid,d_gid,d_access_id,k_first,k_last,k_email,k_person_id,k_project_id,k_uid,k_gid,k_access_id,time_merge) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            data = (trans_rec_id,d_first,d_last,d_email,d_person_id,d_project_id,d_uid,d_gid,d_access_id,k_first,k_last,k_email,k_person_id,k_project_id,k_uid,k_gid,k_access_id,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" Inserted 'request_person_merge' data into 'archive_merge'.")
        except Exception as e:
            print(" Could not insert 'request_person_merge' data into 'archive_merge'.  Skipping.")
            print(" ERROR: ",str(e))
            continue

        print("TESTING PHASE.  EXITING.")
        os.remove(lock_file_path)
        sys.exit()

#  Remove info for deleted person 'd_person_id' info from data_tbl and local_info.

#  Remove from 'data_tbl'.
        try:
            sql = "DELETE FROM data_tbl WHERE person_id = %s"
            data = (d_person_id,)
            results = []
#            amiedb_call(sql,data,script_name,results)
            print("Deleted all lines for ",d_person_id," from 'data_tbl' for '",type_id,"' request.")
        except Exception as e:
            print("Problem deleting lines for ",d_person_id," from 'data_tbl' for '",type_id,"' request.")
            print("ERROR: ",str(e))
            continue
#  Remove from 'local_info'.
        try:
            sql = "DELETE FROM local_info WHERE person_id = %s AND access_id = %s"
            data = (d_person_id,k_portal_login,)
            print("***** Attempting to delete ",d_person_id," with ACCESS ID ",access_id," from 'local_info'.")
            results = []
#            amiedb_call(sql,data,script_name,results)
            print(" Deleted line for ",d_person_id," from 'local_info' for '",type_id,"' request.")
        except Exception as e:
            print("Problem deleting line(s) for ",d_person_id," from 'local_info' for '",type_id,"' request.")
            print("ERROR: ",str(e))
            continue

#  Update transaction_tbl table value of state_id to 'completed'.

        sql = "UPDATE transaction_tbl SET state_id = 'completed' WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        try:
            amiedb_call(sql,data,script_name,results)
            print(" Updated 'state_id' for 'trans_rec_id' = ",trans_rec_id," to 'completed' for '",type_id,"' request.")
        except Exception as e:
            print(" Update of 'state_id' for 'trans_rec_id' = ",trans_rec_id," failed for '",type_id,"' request.")
            print(" ERROR: ",str(e))

#  Construct and send the InformTransactionComplete(ITC) success packet

        itc = packet.reply_packet()
        itc.StatusCode = 'Success'
        itc.DetailCode = '1'
        itc.Message = 'OK'

        if (sendpackets == 'yes'):
            try:
                amie_client.send_packet(itc)
                print(" Sent 'request_user_modify' packet for 'trans_rec_id' = ",trans_rec_id,".")
            except Exception as e:
                print(" Failed to 'request_user_modify' packet for 'trans_rec_id' = ",trans_rec_id,".")
                print(" ERROR: ",str(e))
        else:
            print(" Test run. No 'request_user_modify' packet sent for 'trans_rec_id' = ",trans_rec_id,".")

# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    if type_id == 'inform_transaction_complete':
# &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        print(" The 'information_transaction_complete' packets are skipped.")
        os.remove(lock_file_path)

#  Delete lock file.
#print("Removing lock file.")
#try:
os.remove(lock_file_path)
#except Exception as e:
#    print("No lock file to remove.")
#print("Removed lock file.")

end_time = time.time()
slurm_time = end_time - start_time
print(" ")
print("Script time: ",slurm_time)
#sys.exit()

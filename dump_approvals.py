#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
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

faster_id = '14'

script_name = sys.argv[0][2:]
#print(" script_name = ",script_name)
#sys.exit()
#logging.info(' Running ' + script_name)

dir_dev = '/home/baum/AMIEDEV'
dir_pro = '/home/baum/AMIE'
cwd = os.getcwd()
if cwd == dir_pro:
    amie_config = '/home/baum/AMIE/amie.ini'
    log_file = '/home/baum/AMIE/amie.log'
    print(" PRODUCTION MODE!!!!")
elif cwd == dir_dev:
    amie_config = '/home/baum/AMIEDEV/amiedev.ini'
    log_file = '/home/baum/AMIEDEV/amiedev.log'
    print(" DEVELOPMENT MODE!!!!!")
else:
    print(" Not running in either ",dir_dev," or ",dir_pro,".  Exiting.")
    sys.exit()

#  Configuration and log file processing.
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

#  Command-line argument processing
diag = 0
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
#            print(" Printing diagnostics.")
        else:
#  Kick all invalid arguments ingloriously out of the script.
            print(" ")
            print(" The command-line argument entered '",sys.argv[1],"' is meaningless.  Exiting.")
            print(" Try one of ",help_list," for available valid arguments.")
            print(" ")
            sys.exit()

#  Email notifications
def send_email(body):
    sender = 'admin@faster-mgt2.hprc.tamu.edu'
    if cwd == dir_dev:
        receivers = ['baum@tamu.edu']
    elif cwd == dir_pro:
        receivers = ['baum@tamu.edu','tmarkhuang@tamu.edu','perez@tamu.edu','kjacks@hprc.tamu.edu']
    msg = MIMEMultipart()
    msg['Subject'] = 'ACCESS Packet Alert'
    msg['From'] = sender
    msg['To'] = ",".join(receivers)
    msg.attach(MIMEText(body, 'plain'))
    with smtplib.SMTP('smtp-relay.tamu.edu') as server:
        server.sendmail(sender, receivers, msg.as_string())
        print("   Notification of packet reception sent to:  ",','.join(receivers))

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
trans_pi = ['request_project_create','request_project_inactivate','request_project_reactivate']
trans_user = ['request_account_create','request_account_inactivate','request_account_reactivate','request_user_modify']

#print(' ')
#print(' Running ',sys.argv[0],' to process incoming packets at ',tstamp())
#print(' ')

#  Filtering out those eternally persistent 'inform_transaction_complete' packets.
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

pnum = 1
for packet in filtered_packets:

    trans_rec_id = packet.trans_rec_id
    packet_rec_id = packet.packet_rec_id
    pdict = json.loads(packet.json())
    type_id = json_extract(pdict,'type')[0]
    logging.info(script_name + ": processing packet no. " + str(pnum) + " of type '" + type_id + "'.")
    print(" ")
    print(" Processing packet no. ",pnum," of type '",type_id," with trans_rec_id = ",trans_rec_id," and packet_rec_id = ",packet_rec_id,".")

#  Dump packet to text file as 'xxx/xxx-trans_rec_id.json' for archival purposes (if not already dumped)
#  where 'xxx' is the first letters of the transaction type, e.g. request_project_create = rpc.
    packet_type = type_id
    pref = packet_type.split("_")[0][0] + packet_type.split("_")[1][0] + packet_type.split("_")[2][0]
    trans_rec_id = packet.trans_rec_id
    jsonfile = pref + "/" + pref + str(trans_rec_id) + ".json"
    if not os.path.isfile(jsonfile):
        with open(jsonfile, 'w') as convert_file:
            convert_file.write(json.dumps(pdict))

#    print("pdict = ",pdict)
#    sys.exit()

#  If transaction type if one of seven that require approval, process the packet.
    if type_id in allpack:

# ================== PROCESSING FOR local_info TABLE ========================

#######################################################################
#  PACKET:  REQUEST_PROJECT_CREATE
#######################################################################
        if type_id == 'request_project_create':
            if diag > 0:
                print(" DUMP_APPROVALS: ********************** Processing 'local_info' insertion for ",type_id)
            first_name = packet.PiFirstName
            last_name = packet.PiLastName
            pi_global_id = packet.PiGlobalID
#  Extract the PersonID for the SitePersonId dictionary with the X-PORTAL key value.
            site_person_id = packet.SitePersonId
            access_id = next(item for item in site_person_id if item["Site"] == "X-PORTAL")['PersonID']
            email = packet.PiEmail
            resource_list = packet.ResourceList[0]
            cluster = resource_list.split('.')[0]
            grant_number = json_extract(pdict,'GrantNumber')[0]
            print("   Request from PI ",first_name," ",last_name," with email ",email," global ID ",pi_global_id," and grant no. ",grant_number)
            service_units_allocated = json_extract(pdict,'ServiceUnitsAllocated')[0]
#            print(" sua = ",service_units_allocated)
            start_date = json_extract(pdict,'StartDate')[0]
            end_date = json_extract(pdict,'EndDate')[0]
            person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + pi_global_id
#  2023-01-05 - Stopped using 'pi' for PIs.
#            person_id = 'pi.' + first_name[0].lower() + last_name[0].lower() + pi_global_id
            remote_site_login = person_id
            project_id = 'p.' + grant_number.lower() + '.000'
            proj_stat = 'pending'
            acct_stat = 'pending'
# --------------------------------------------------------------------------------------------
#  Must check all previous random strings to avoid duplicate.
#  Select all 'slurm_acct' entries for PIs, strip out random number parts, and compare to new random number.
            newrand = slurm_rand()
            sql = "SELECT slurm_acct FROM local_info WHERE person_id LIKE %s"
            data = ('pi%',)
            results = []
            amiedb_call(sql,data,script_name,results)
#            prev_rand_num = []
# Deal with initial condition of no previous 'slurm_acct' numbers.
            if len(results) != 0:
#                for rands in results[0]:
#                    prev_rand_num.append(int(rands[-4:]))
                prn = []
                for slno in results[0]:
                    prn.append(int(slno[-4:]))
                no_match = 1
                while no_match == 1:
                    if newrand in prn:
                        no_match = 1
                        newrand = slurm_rand()
#                        print("New random number matches old one. Try again: ",newrand)
                    else:
#                        print("New random number doesn't match old one.")
                        no_match = 0
            slurm_acct = faster_id + str(int(pi_global_id) + 500000) + newrand
# ---------------------------------------------------------------------------------------------

#  Find out if this project_id already exists in 'local_info'.
            sql = "SELECT * FROM local_info WHERE project_id = %s"
            data = (project_id,)
            results = []
            amiedb_call(sql,data,script_name,results)

#  For new project, find combined max of UID and GID columns.
#  The next GID will be MAX+1, and the PI UID will be MAX+2.
            if len(results) == 0:
#  Find maximum UID in local_info.
                sql = "SELECT MAX(uid) FROM local_info;"
                data = ()
                results = []
                amiedb_call(sql,data,script_name,results)
                uid = results[0][0]
#  Find maximum GID in local_info.
                sql = "SELECT MAX(gid) FROM local_info;"
                data = ()
                results = []
                amiedb_call(sql,data,script_name,results)
                gid = results[0][0]
#  Deal with DB initial condition.
                if uid == None:
                    uid = 50000
                if gid == None:
                    gid = 50000
#  Find maximum of GID and UID.
                maxid = max(uid,gid)
#  Set next UID and GID balues.
#                print(" Previous max ID = ",maxid," Next GID = ",maxid+1," Next UID = ",maxid+2)
                gid = maxid + 1
                uid = gid + 1
                pi = 'Y'
#                print("#####")
#                print(" ALL = ",first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units_allocated,start_date,end_date,proj_stat,acct_stat,uid,gid)
#                print("#####")
                try:
                    sql = "INSERT INTO local_info (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    data = (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units_allocated,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    logging.info(script_name + ": Person ID - " + person_id + ", Project ID - " + project_id + ", Transaction ID = " + str(trans_rec_id))
#  Email notification of request_project_create packet.
                    msg = "A " + type_id + " packet for PI " + first_name + " " + last_name + " (" + email + ") with Access ID " + access_id + ", Global ID " + pi_global_id + " and grant " + grant_number + " is awaiting approval."
                    send_email(msg)
                    logging.info(script_name + ": " + msg)
                except Exception as e:
                    msg1 = "Error adding " + type_id + " packet for PI " + first_name + " " + last_name + " (" + email + ") with Access ID " + access_id + ", Global ID " + user_global_id + " and grant " + grant_number + " to approval table."
                    msg2 = "Error message: " + str(e)
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1)
                    logging.info(script_name + ": " + msg2)
                    print(" Skipping to next packet.")
                    continue
            else:
                print("   The 'local_info' table already contains person_id = ",person_id," with project_id = ",project_id,". Skipping insertion.")
                if diag > 0:
                    print(" DUMP_APPROVALS: The 'local_info' table already contains ",person_id," with project_id ",project_id,". Skipping insertion.")
##########################################################################
#  PACKET:  REQUEST_ACCOUNT_CREATE
##########################################################################
        elif type_id == 'request_account_create':
            if diag > 0:
                print(" DUMP_APPROVALS: ********************** Processing 'local_info' insertion for ",type_id)
#            print(" DUMP_APPROVALS: Starting 'local_info' table transaction in 'dump_approvals.py'")
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
            print("   Request from ",first_name," ",last_name," with email ",email," global ID ",user_global_id," and grant no. ",grant_number)
            person_id = 'u.' + first_name[0].lower() + last_name[0].lower() + user_global_id
            remote_site_login = person_id
            project_id = 'p.' + grant_number.lower() + '.000'
            acct_stat = 'pending'
#            acct_stat = 'inactive'
#  If the person_id/project_id combination is already in local_info, skip it.
#  If just the person_id is already there, create a new local_info entry with a new project_id.
            sql = "SELECT * FROM local_info WHERE person_id = %s and project_id = %s"
            data = (person_id, project_id)
            results = []
            amiedb_call(sql,data,script_name,results)
            already_in_local_info = len(results)
            print(" ACCOUNT: already_in_local_info = ",already_in_local_info)
#  If person_id/project_id combination not in local_info, add it.
            if already_in_local_info == 0:
                sql = "SELECT * FROM local_info WHERE person_id = %s"
                data = (person_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                additional_person_id_entry = len(results)
#  Find the GID of the project via the grant_number and assign it to this person_id.
                sql = "SELECT gid from local_info WHERE grant_number = %s"
                data = (grant_number,)
                results = []
                amiedb_call(sql,data,script_name,results)
                gid = results[0][0]
#  If this person_id isn't in local_info, create a new UID entry.
                if additional_person_id_entry == 0:
                    print(" ACCOUNT:  Creating new UID for new person_id entry.")
                    sql = "SELECT MAX(uid) FROM local_info;"
                    data = ()
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    uid = results[0][0]
                    maxid = max(uid,gid)
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
#  Find 'slurm_acct', 'service_units', 'start_date', 'end_date' and 'proj_stat' from 'local_info' given the 'project_id'.
                sql = "SELECT slurm_acct,service_units,start_date,end_date,proj_stat FROM LOCAL_INFO WHERE project_id = %s LIMIT 1;"
#                sql = "SELECT slurm_acct FROM LOCAL_INFO WHERE project_id = %s LIMIT 1;"
                data = (project_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (slurm_acct,service_units,start_date,end_date,proj_stat) = results[0]
#                slurm_acct = results[0][0]
                pi = 'N'

                try:
                    sql = "INSERT INTO local_info (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
                    data = (first_name,last_name,email,person_id,project_id,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#  Email notification of request_account_create packet.
                    msg = "A " + type_id + " packet for user " + first_name + " " + last_name + " (" + email + ") with Access ID " + access_id + ", Global ID " + user_global_id + " and grant " + grant_number + " is awaiting approval."
                    send_email(msg)
                except Exception as e:
                    msg1 = "Error adding " + type_id + " packet for user " + first_name + " " + last_name + " (" + email + ") with Access ID " + access_id + ", Global ID " + user_global_id + " and grant " + grant_number + " to approval table."
                    msg2 = "Error message: " + str(e)
                    send_email(msg1 + "\r\n" + msg2)
                    logging.info(script_name + ": " + msg1)
                    logging.info(script_name + ": " + msg2)
                    print(" Skipping to next packet.")
                    continue
#  If person_id/project_id combination in local_info, skip.
            else:
                print("   The 'local_info' table already contains person_id = ",person_id," with project_id = ",project_id,". Skipping insertion.")
                if diag > 0:
                    print(" DUMP_APPROVALS: The 'local_info' table already contains ",person_id," with project_id ",project_id,". Skipping insertion.")

#######################################################
#  PACKET:  REQUEST_USER_MODIFY
#######################################################
        elif type_id == 'request_user_modify':
            person_id = packet.PersonID
            project_id = 'NA'
            try:
                sql = "SELECT * FROM transaction_tbl WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
#  If not in transaction table, then send email notification.
                if len(results) == 0:
                    print(" No previous transaction_tbl entry.  Sending email notification.")
                    body = "A request to modify user ID " + person_id + " is awaiting approval."
                    send_email(body)
            except:
                print(" Problem processing table look-up for 'request_user_modify' transaction ID ",trans_rec_id,".")
########################################################
#  PACKET:  REQUEST_PERSON_MERGE
########################################################
        elif type_id == 'request_person_merge':
            person_id = packet.KeepPersonID
            project_id = 'NA'
            try:
                sql = "SELECT * FROM transaction_tbl WHERE trans_rec_id = %s"
                data = (trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                if len(results) == 0:
                    print(" No previous transaction_tbl entry.  Sending email notification.")
                    body = "A request to merge user ID " + person_id + " is awaiting approval."
                    send_email(body)
            except:
                print(" Problem processing table look-up for 'request_person_merge' transaction ID ",trans_rec_id,".")
#########################################################
#  PACKET:  REQUEST_PROJECT_INACTIVATE
#########################################################
        elif type_id == 'request_project_inactivate':
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id)
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" results = ",results)
            if len(results) == 0:
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
                project_id = packet.ProjectID
                print(" packet = ",vars(packet))
                person_id = 'NA'
                print(" Request to inactivate project ID ",project_id)
                body = "Request to inactivate project ID " + project_id + " with PI " + first_name + " " + last_name + " is awaiting approval."
                send_email(body)
            else:
                print("   This packet has already been dumped for approval.")
                continue
############################################################
#  PACKET:  REQUEST_PROJECT_REACTIVATE
############################################################
        elif type_id == 'request_project_reactivate':
            project_id = packet.ProjectID
            person_id = 'NA'
            print(" Request to reactivate project ID ",project_id)
            body = "A request to reactivate project ID " + project_id + " is awaiting approval."
            send_email(body)
############################################################
#  PACKET:  REQUEST_ACCOUNT_INACTIVATE
############################################################
        elif type_id == 'request_account_inactivate':
            print(" Processing ",type_id," packet.")
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            person_id = packet.PersonID
            project_id = packet.ProjectID
            results = []
            amiedb_call(sql,data,script_name,results)
            if len(results) == 0:
                try:
                    sql = "SELECT first_name,last_name,person_id,grant_number,uid,access_id FROM local_info WHERE person_id LIKE %s"
                    data = (person_id,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    (first_name,last_name,pid,grant_number,uid,access_id) = results[0]
                except:
                    print(first_name," ",last_name," with access ID ",access_id," not found in 'local_info' via 'person_id' search for 'request_account_inactivate' packet.")
                    continue
                person_id = packet.PersonID
                project_id = packet.ProjectID
                body = "Request to inactivate user " + first_name + " " + last_name + " with accessID " + access_id + ", person ID " + person_id + " and grant " + grant_number + " is awaiting approval."
                print(body)
                send_email(body)
            else:
                print(" This ",type_id," packet has already been dumped for approval.")
                continue
#  PACKET:  DATA_PROJECT_CREATE
        elif type_id == 'data_project_create':
            print(" Approval not needed for 'data_project_create'.  Run 'respond.py' again.")
#  PACKET:  DATA_ACCOUNT_CREATE
        elif type_id == 'data_account_create':
            print(" Approval not needed for 'data_account_create'.  Run 'respond.py' again.")
#  PACKET:  INFORM_TRANSACTION_COMPLETE
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
                    sql = "UPDATE packet_tbl SET state_id = 'completed' WHERE packet_rec_id = %s"
                    data = (str(packet_rec_id),)
                    results = []
                    amiedb_call(sql,data,script_name,results)
                    print(" Changed 'state_id' to 'completed' in 'packet_tbl' for packet_rec_id = ",packet_rec_id," and trans_rec_id = ",trans_rec_id)
            except:
                print(" The state_id value extracted from packet_tbl for packet_rec_id = ",packet_rec_id," is ",results," and unusable.")
#  PACKET:  UNKNOWN
        else:
            person_id = packet.PersonID
            project_id = packet.ProjectID
            print(" Unknown packet type: ",type_id)

# =====================================================================================
#  DUMP INFO INTO TABLES transaction_tbl, packet_tbl, expected_reply_tbl and approval.
# =====================================================================================

# ================== PROCESSING FOR transaction_tbl TABLE INSERTION ========================

        if diag > 0:
            print(" DUMP_APPROVALS: ********************** Processing 'transaction_tbl' insertion")
        originating_site_name = packet.originating_site_name
        transaction_id = packet.transaction_id
        local_site_name = packet.local_site_name
        remote_site_name = packet.remote_site_name
        state_id = packet.transaction_state

#  Find out if this trans_rec_id already exists in 'transaction_tbl'.
        sql = "SELECT * FROM transaction_tbl WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if diag > 0:
            print(" DUMP_APPROVALS: trans_rec_id = ",trans_rec_id," transaction_tbl: results = ",results," len(results) = ",len(results))

#  Add this trans_rec_id to 'transaction_tbl'.
        if len(results) == 0:
            sql = """INSERT INTO transaction_tbl (trans_rec_id,originating_site_name,transaction_id,local_site_name,remote_site_name,state_id,ts) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
            data = (trans_rec_id, originating_site_name, transaction_id, local_site_name, remote_site_name, state_id, tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
            print("   This ",type_id," with trans_rec_id ",trans_rec_id," is now added to 'transaction_tbl'.")
            if diag > 0:
                print(" DUMP_APPROVALS: transaction_tbl: results = ",results)
        else:
            print("   This ",type_id," with trans_rec_id ",trans_rec_id," has already been added to 'transaction_tbl'.")
            if diag > 0:
                print(" DUMP_APPROVALS: The 'transaction_tbl' table already contains 'trans_rec_id' = ",trans_rec_id,". Skipping insertion.")
        
# ================== PROCESSING FOR packet_tbl TABLE INSERTION ========================

        if diag > 0:
            print(" DUMP_APPROVALS: ********************** Processing 'packet_tbl' insertion")
        packet_rec_id = packet.packet_rec_id
        packet_id = packet.packet_id
        type_id = json_extract(pdict,'type')[0]
        version = '0.6.0'
        packet_state_id = packet.packet_state
        outgoing_flag = packet.outgoing_flag

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
            sql = """INSERT INTO packet_tbl (packet_rec_id,trans_rec_id,packet_id,type_id,version,state_id,outgoing_flag,ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""
            data = (packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state_id,outgoing_flag,tstamp())
            results = []
            amiedb_call(sql,data,script_name,results)
            print("   This ",type_id," with packet_rec_id ",packet_rec_id," is now added to 'packet_tbl'.")
            if diag > 0:
                print(" DUMP_APPROVALS: data = ",packet_rec_id,trans_rec_id,packet_id,type_id,version,packet_state_id,outgoing_flag,tstamp())
                print(" DUMP_APPROVALS: Inserted packet_rec_id = ",packet_rec_id," into 'packet_tbl' for '",type_id,"' packet.")
        else:
            print("   This ",type_id," with packet_rec_id ",packet_rec_id," has already been added to 'packet_tbl'.")
            if diag > 0:
                print(" DUMP_APPROVALS: The 'packet_tbl' table already contains 'packet_rec_id' = ",packet_rec_id,". Skipping insertion.")

# ================== PROCESSING FOR expected_reply_tbl TABLE INSERTION ========================
#  The contents of the 'local_info' and 'approval' tables most likely render 'expected_reply_tbl' moot, but here it is anyway.

        if diag > 0:
            print(" DUMP_APPROVALS: ********************** Processing 'expected_reply_tbl' insertion")

#  Find out if this 'packet_rec_id' already exists in 'expected_reply_tbl'.
        sql = "SELECT * FROM packet_tbl WHERE packet_rec_id = %s"
        data = (str(packet_rec_id),)
        if diag > 0:
            print(" DUMP_APPROVALS: sql = ",sql," data = ",data)
        results = []
        amiedb_call(sql,data,script_name,results)
        if diag > 0:
            print(" DUMP_APPROVALS: packet_rec_id = ",packet_rec_id," packet_tbl: results = ",results," len(results) = ",len(results))

#  Add this 'packet_rec_id` to 'expected_reply_tbl'.
        if len(results) == 0:
            timeout = 60
            sql = """INSERT INTO expected_reply_tbl (packet_rec_id,type_id,timeout) VALUES (%s, %s, %s);"""
            data = (packet_rec_id,type_id,timeout)
            results = []
            amiedb_call(sql,data,script_name,results)
            if diag > 0:
                print(" DUMP_APPROVALS: Inserted packet_rec_id = ",packet_rec_id," into 'expected_reply_tbl' for '",type_id,"' packet.")
        else:
            if diag > 0:
                print(" DUMP_APPROVALS: The 'expected_reply_tbl' table already contains 'packet_rec_id' = ",packet_rec_id,". Skipping insertion.")

# ================== PROCESSING FOR approval TABLE ========================

#  !!!!! NOTE  !!!!! - filter out non-approval packets here rather than above.

        if type_id in approvals:
            if diag > 0:
                print(" DUMP_APPROVALS: ********************** Processing 'approval' insertion")

#  All initial requests are set to a  default of 'unapproved'.
            approval_status = 'unapproved'

#  Find out if this 'trans_rec_id' already exists in 'approval' table.
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            if diag > 0:
                print(" DUMP_APPROVALS: sql = ",sql," data = ",data)
            results = []
            amiedb_call(sql,data,script_name,results)
#            print(" trans_rec_id = ",trans_rec_id," RESULTS = ",results)

            if len(results) == 0:
                sql = "INSERT INTO approval (trans_rec_id,type_id,person_id,project_id,approval_status,ts_received) VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (trans_rec_id) DO NOTHING;"
                data = (trans_rec_id,type_id,person_id,project_id,approval_status,tstamp())
                results = []
                amiedb_call(sql,data,script_name,results)
                logging.info(script_name + ": Done processing 'local_info', 'approval', 'transaction_tbl', 'packet_tbl' and 'expected_reply_tbl' tables'")
#                logging.info(sys.argv[0][2:])
                if diag > 0:
                    print(" DUMP_APPROVALS: Inserted info into 'approval' table for '",type_id,"' packet for user ",person_id,".")
            else:
                if diag > 0:
                    print(" DUMP_APPROVALS: The 'approval' table already contains 'trans_rec_id' = ",packet_rec_id,". Skipping insertion.")
                logging.info(script_name + ": Skipped processing DB tables for already existent entry.")
#                logging.info(sys.argv[0][2:])

    pnum = pnum + 1




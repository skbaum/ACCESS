#!/usr/bin/python3

#  Script for creating and sending locally originated 'notify_project_usage' packets.
#  There are three packet types: 'send', 'get_failed_records' and 'status'.
#
#  'send' packet type
#
#  Sends compute usage record packets from info stored in 'usage_compute_aces' table.
#
#  1. Extract the stored value of 'start_id' from 'usage_compute_count' to know where to start in 'usage_compute_aces'.
#  2. Extract the maximum value of 'id' from 'usage_compute_aces' to know where to stop.
#     a. If 'cur.py send' is followed by a number - e.g. 'cur.py send 10' - only send 10 packets.
#  3. Extract the 'usage_compute_aces' info for a given 'id' value.
#  4. Check if entry 'id' has already been processed by checking the 'status' column for 'Y' or 'N'.
#  5. Create Python data structure for required packet from 'usage_compute_aces'.
#  6. Convert Python data structure into JSON packet for usage type 'Compute'.
#  7. Create cURL string to send to ACCESS.
#  8. Check if charge amount is effectively zero, and if so:
#     a. Change 'status' value to 'Y' and time_sent to '2525-01-01 00:00:00' to flag such entries.
#  9. Send usage packet to ACCESS.
# 10. Change 'status' column to 'Y' and update 'time_sent' column in 'usage_compute_aces'.
# 11. Update the 'id_start' value in 'usage_compute_count'.
#
#  'get_failed_records' packet type
#
#  Request report on compute usage packets that failed to be processed by ACCESS.
#
#  NOT FINISHED
#
#  'status' packet type
#
#  Requests a report on usage records sent in a supplied time range.
#
#  1. Create cURL string using to send 'status' packet using start and end dates supplied as command-line arguments.
#  2. Send status packet.
#
# Field key for usage_compute_aces for Lisa Perez 9/23/25
# id - internal number in usage_compute_aces table that Steve B uses to keep track of the number of records and more
# serial_no - job_db_inx (slurm)
# person_id - local username
# project_id - local name for the Project in ACCESS
# uid - uid of the user on the system
# local_record_id - slurm account number
# resource - the name of the system
# submit_time - time the job was submitted (Time Zone to be determined)
# start_time - time the job started (Time Zone to be determined)
# end_time - time the job ended (Time Zone to be determined)
# charge - SU charge directly from Keith's table
# node_count - reported node count (Keith)
# cpu_core_count - reported cpu core count (Keith)
# job_name - job id (id_job column in Keith)
# max_memory - memory in MB???
# queue - The name of the queue
# parent_record_id - optional
# local_reference - optional
# time_stored - when it has been added to the table (time zone tbd)
# time_sent - when the packet is sent (time zone tdb)
# access_id - ACCESS ID of the user
# status - N before the usage packet has been sent to ACCESS - Y usage packet has been sent to ACCESS

from configparser import ConfigParser
from datetime import datetime,date,timezone
from collections import namedtuple
#from zoneinfo import ZoneInfo

from pprint import pprint
import inspect
import sys
import logging
import os
import json
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
import mysql.connector

#script_name = sys.argv[0][2:]
script_name = 'cur.py'

ACCESS_HOME = '/home/baum/AMIE'
log_file = ACCESS_HOME + '/access_usage.log'
#  Configuration and log file processing.
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

#  Configuration and log file processing.
log_file = '/home/baum/AMIEDEV/amiedev.log'
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

dir_dev = '/home/baum/AMIEDEV'
dir_pro = '/home/baum/AMIE'
cwd = os.getcwd()

if cwd == dir_pro:
    amie_config = "/home/baum/AMIE/amie.ini"
    print(" PRODUCTION MODE!!!!")
elif cwd == dir_dev:
    amie_config = "/home/baum/AMIEDEV/amiedev.ini"
    print(" DEVELOPMENT MODE!!!!!")
else:
    print(" Not running in either ",dir_dev," or ",dir_pro,".  Exiting.")
    sys.exit()

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
        print("             None - Program exits.  Arguments required.")
        print("             send - send all outstanding compute usage request packets")
        print(" ")
        sys.exit()
    else:
        no_to_process = 'NA'

        available_args = ['send','check']
        req = ""
#  Check for the usage arguments.
        if sys.argv[1] == 'send':
            req = str(sys.argv[1])
            if len(sys.argv) == 3:
                nbunch = int(sys.argv[2])
                try:
                    int(nbunch)
                except:
                    print("The 'N' value given after 'send' is '",nbunch,"' and not an integer. Exiting.")
                    sys.exit()
            else:
                print("The 'send' argument must be followed by an integer specifying the bunching parameter.")
                sys.exit()
        else:
#  Kick all invalid arguments ingloriously out of the script.
            print(" ")
            print(" The command-line argument '",sys.argv[1],"' is useless.  Try again with 'send', 'get_failed_records' or 'status'.  Exiting.")
            print(" Try one of ",help_list," for available valid arguments.")
            print(" ")
            sys.exit()
else:
    print("At least one command line argument is required.  Enter './cur.py -h' for choices.")
    sys.exit()

#sys.exit()

def unix_to_utc(unix_timestamp):
  """
  Converts a Unix timestamp to a UTC datetime object.

  Args:
    unix_timestamp: An integer or float representing the Unix timestamp.

  Returns:
    A timezone-aware datetime object in UTC.
  """
  return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)

def amiedb_call(sql,data,script_name,results):
    if diag > 0:
        print(" AMIEDB_CALL: sql = ",sql)
        print(" AMIEDB_CALL: data = ",data)
    try:
        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
    except:
        print(" AMIEDB_CALL: Error connecting to database in ",script_name)
        print(" AMIEDB_CALL: Problematic SQL: ",sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql,data)
        conn.commit()
        try:
            matches = cursor.fetchall()
            len_matches = len(matches)
            if diag > 0:
                print(" AMIEDB_CALL: No. of matches: ",len_matches," match(es) = ",matches)
            for match in matches:
                results.append(match)
        except:
            results = []
            if diag > 0:
                print(" AMIEDB_CALL: No cursor.fetchall() results to process.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("*************** DB transaction error ***************: ",exc_info=True)
#  This executes after either 'try' or 'except'.
    finally:
        if conn:
            cursor.close()
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

test = 0

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
site_name = site_con['site_name']
api_key = site_con['api_key']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Find id_start and id_max from entries in table with status = 'N'.
sql = "select id from usage_compute_aces where status = 'N' order by id limit 1"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
if len(results) == 0:
    print("No status = 'N' entries to process.  Exiting.")
    sys.exit()
id_start = results[0][0]
sql = "select * from usage_compute_aces where status = 'N' order by id desc limit 1"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
id_max = results[0][0]
#print("The starting 'id' value: ",id_start," and maximum 'id' value: ",id_max," for 'status' = 'N' entries.")

sql = "select count(*) from usage_compute_aces where status = 'N'"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
number_to_send = results[0][0]
#print("The number of unprocessed reports to send is: ",number_to_send)

sql = "select id from usage_compute_aces where status = 'N' order by id"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
ids_to_process = []
id_track = 0
for id_no in results:
    ids_to_process.append(results[id_track][0])
    id_track = id_track + 1

print("There are ",len(ids_to_process)," reports to send:")
#print("ids = ",ids)
#sys.exit()
#print(ids_to_process[15:25])
############ REVERSE #####################
#  Reverse the list order.
#ids_to_process = list(reversed(ids_to_process))
#print(ids_to_process[15:25])

# TEST
#ids_to_process = ids_to_process[:10000]
#print("ids = ",len(ids_to_process))
#sys.exit()

#ids_to_process = [998]

if req == 'send':
    print("In 'send' loop.")

    if no_to_process != 'NA':
        id_max = int(no_to_process) - 1 + id_start
        ids_to_process = ids_to_process[:int(no_to_process)]
        print(ids_to_process)
        print("Processing only ",no_to_process," out of ",number_to_send," unprocessed entries to send usage reports.")
#    else:
#        print("Processing all ",number_to_send," outstanding unprocessed entries to send usage reports.")
#    id_max_next = id_max + 1

    logtstamp = tstamp().split(" ")[0] + "T" + tstamp().split(" ")[1].replace(":","-")
#    send_log = "/home/baum/AMIE/daily_cur_logs/" + sys.argv[0].split("/")[1] + "-" + logtstamp + ".log-test"
    send_log = "/home/baum/AMIE/daily_cur_logs/" + sys.argv[0].split("/")[1] + "-" + logtstamp + ".log"
    print("send_log = ",send_log)

    if id_start > id_max:
        print("Nothing to process.  Exiting.")
        sys.exit()

    total_available = number_to_send
    msg1 = "Running 'cur.py' to send " + str(len(ids_to_process)) + " of " + str(total_available) + " outstanding usage reports from 'id' " + str(id_start) + " to " + str(ids_to_process[-1]) + " on " + str(tstamp()) + ".\n"
    print(msg1)

    f = open(send_log, "a")
    print(msg1, file=f)

    Records = []
    packet = {}
    count = 0
    packets = 1
#  Loop over unprocessed entries in 'usage_compute_aces' table.
#  Process elements in 'ids_to_process' until it is empty.
    while len(ids_to_process) != 0:

#  Extract first N elements from ids_to_process to bundle into a single usage report packet.
        indices_subset = ids_to_process[:nbunch]
#  Remove the processed elements from ids_to_process.
        del ids_to_process[:nbunch]

#        print("indices_subset: ",indices_subset)
#        print("ids_to_process[:20]: ",ids_to_process[:20])
#        sys.exit()

        Records = []
#  Create text list of id value subset being processed.
        serial_no_list = ''
        for entry_index in indices_subset:
#            print("Processing 'id' value: ",entry_index)
            serial_no_list = serial_no_list + str(entry_index) + ", "
        serial_list = "Sending packet #" + str(packets) + " with " + str(nbunch) + " reports with 'usage_compute' ids " + serial_no_list + "."
        print(serial_list)
        print(serial_list, file=f)
        packets = packets + 1
#        sys.exit()

        for entry_index in indices_subset:
#            print("entry_index = ",entry_index)

#  Note: The 'submit_time' etc. values are not automatically converted to the local timezone during extraction because their
#        type definition is 'timestamp without time zone'.

#  Extract 'usage_compute_aces' info for a given 'id' value.
            try:
                sql = "SELECT * FROM usage_compute_aces WHERE id = %s"
                data = (entry_index,)
                results = []
                amiedb_call(sql,data,script_name,results)
                len_results = len(results)
                print(results)
#                sys.exit()
                if len_results != 0:
                    (id,serial_no,person_id,project_id,uid,local_record_id,resource,submit_time,start_time,end_time,charge,node_count,cpu_core_count,job_name,max_memory,queue,parent_record_id,local_reference,time_stored,time_sent,access_id,status) = results[0]
#                    print(id,serial_no,status)
                else:
                    print("No entry in 'usage_compute_aces' for 'id' = ",entry_index,". Skipping to next 'id' number.")
                    continue
            except Exception as e:
                msg1 = "Problem extracting 'usage_compute_aces' info for 'id' " + entry_index + ". Skipping to next entry."
                msg2 = "ERROR message: " + str(e)
                send_email(msg1 + "\r\n" + msg2)
                logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
                print(msg1 + "\r\n" + msg2)
                continue

            if status == 'Y':
                print("Entry ",entry_index," already processed.  Skipping.")
                continue
            usage_type = 'Compute'

            utc_time_submit = unix_to_utc(int(submit_time))
            print("utc_time_submit = ",utc_time_submit," submit_time = ",submit_time)
            utc_time_start = unix_to_utc(int(start_time))
            utc_time_end = unix_to_utc(int(end_time))

#            sys.exit()

            Attributes = {}
            Attributes.update({'NodeCount':node_count})
            Attributes.update({'CpuCoreCount':cpu_core_count})
            Attributes.update({'Queue':queue})
            Attributes.update({'Memory':max_memory})

            Record = {}
            Record.update({'Username':person_id})
            Record.update({'LocalProjectID':project_id})
            Record.update({'LocalRecordID':str(serial_no)})
            Record.update({'Resource':resource + '.tamu.access.org'})
            utc_time_submit = str(utc_time_submit).split(' ')[0] + "T" + str(utc_time_submit).split(' ')[1]
            Record.update({'SubmitTime':utc_time_submit})
            utc_time_start = str(utc_time_start).split(' ')[0] + "T" + str(utc_time_start).split(' ')[1]
            Record.update({'StartTime':utc_time_start})
            utc_time_end = str(utc_time_end).split(' ')[0] + "T" + str(utc_time_end).split(' ')[1]
            Record.update({'EndTime':utc_time_end})
            Record.update({'Charge':charge})
            Record.update({'Attributes':Attributes})

#            print("utc_time_submit = ",utc_time_submit)

#            sys.exit()

            print(Record, file=f)
#  Add this Record dictionary to the Records list.
            Records.append(Record)

        count = count + 1
        print("Updated count.")

        packet.update({'UsageType':'Compute'})
        packet.update({'Records':Records})

#  Convert Python data structures to JSON.
        json_packet = "'" + json.dumps(packet) + "'"

#  Create string for curl.

        curl_string = 'curl -X POST -H "XA-Site: ' + site_name + '" -H "XA-API-Key: ' + api_key + '" -H "Content-Type: application/json"'
        curl_string = curl_string + " -d " + json_packet + " 'https://usage.access-ci.org/api/v1/usage'"
        id_log = "/home/baum/AMIE/" + str(entry_index) + ".log"
        curl_string = curl_string + " > " + id_log
        print("curl_string = ",curl_string)

##### EXIT
#        sys.exit()

##### EXIT FOR CHECKING CURL STRING
#        print("Checking curl string and exiting.  No packets sent.")
#        sys.exit()

        try:
            if test == 0:
#                print("NOT SENDING NOW!")
                os.system(curl_string)
#                continue
                print("Sent packet with ",nbunch," reports with serial nos. ",serial_no_list,".")
            else:
                print("TEST: Not sending packet with serial nos. ",serial_no_list,".")
        except Exception as e:
            msg1 = "Problem sending packet for usage_compute_aces with serial_nos. " + serial_no_list + "."
            msg2 = "ERROR message: " + str(e)
            send_email(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            print(msg1 + "\r\n" + msg2) 
            continue

#  Print list of serial numbers in this packet in the log file.
#        print(serial_list)

#  Print return from cURL SEND message to daily log file.
        curl_return = "cat " + str(entry_index) + ".log >> " + send_log
        os.system(curl_return)

#  If packet successfully sent, update the 'time_sent' and 'status' columns in 'usage_compute_aces'.
        try:
            if test == 0:
#  Loop over all the entries in 'indices_subset'.
                for entry_index in indices_subset:
#                    print("Table update for ",entry_index)
                    sql = "UPDATE usage_compute_aces SET time_sent = %s,status = %s WHERE id = %s"
#                    print("tstamp() = ",tstamp()," entry_index = ",entry_index)
                    data = (tstamp(),'Y',entry_index,)
                    results = []
                    amiedb_call(sql,data,script_name,results)
#                    print("Inserted 'time_sent' and 'status' into 'usage_compute_aces' for id ",str(entry_index),".")
#                else:
#                    print("TEST: Did not insert 'time_sent' and 'status' into 'usage_compute_aces' for id ",str(entry_index),".")
        except Exception as e:
            print("Failed to insert 'time_sent' and 'status' into 'usage_compute_aces' for id ",str(entry_index),".")
            print("ERROR: ",str(e))

        print(" ")
        os.system('rm ' + id_log)

    f.close()

    f = open(send_log, "r")
    lines = f.readlines()

    er = 0
    for line in lines:
        if "Message" in line:
            if "0 records failed" not in line:
                print("Record sending failure in ",send_log,"!")
                er = 1
            else:
                continue
    if er == 0:
        print("No record sending failures in ",send_log,"!")
    f.close()

##### EXIT
#    sys.exit()

elif req == 'get_failed_records':
    print("Attempting to send get failed records request.")
    curl_string = 'curl -X GET -H "XA-Site: ' + site_name + '" -H "XA-API-Key: ' + api_key + '" '
    curl_string = curl_string + "'https://usage.access-ci.org/api/v1/usage/get_failed_records'"
    print("curl_string = ",curl_string)
    os.system(curl_string)

elif req == 'status':
    print("Attempting to send status request.")
    start_date = start_obj
    end_date = end_obj
    print("Start date = ",start_date," End date = ",end_date)
    curl_string = 'curl -X GET -H "XA-Site: ' + site_name + '" -H "XA-API-Key: ' + api_key + '" '
    curl_string = curl_string + "'https://usage.access-ci.org/api/v1/usage/status?FromTime="
    curl_string = curl_string + str(start_date) + "&ToTime=" + str(end_date) + "'"
    print("curl_string = ",curl_string)
    os.system(curl_string)

# curl -X GET -H "XA-Site: TAMU" -H "XA-API-Key: AJTOfQEg6oBTSRi2B6ydsI4jjGVFmx31hPnuauGP6LSF8Zg9ybbPjShx31WuUN8C" 'https://usage.access-ci.org/api/v1/usage/status?FromTime=2023-07-25&ToTime=2023-08-01'

#print("Usage message '",req,"' sent.")

#        cur = ComputeUsageRecord.as_dict(ComputeUsageAttributes=(node_count,cpu_core_count,job_name,max_memory,queue),parent_record_id=parent_record_id,charge=charge,submit_time=str(submit_time),start_time=str(start_time),end_time=str(end_time),local_project_id=project_id,local_record_id=local_record_id,local_reference=local_reference,resource=resource,username=person_id)
#        cur = ComputeUsageRecord.as_dict(node_count=node_count,cpu_core_count=cpu_core_count,job_name=job_name,memory=max_memory,queue=queue,parent_record_id=parent_record_id,charge=charge,submit_time=str(submit_time),start_time=str(start_time),end_time=str(end_time),local_project_id=project_id,local_record_id=local_record_id,local_reference=local_reference,resource=resource,username=person_id)
#        cur = ComputeUsageRecord(node_count=node_count,cpu_core_count=cpu_core_count,job_name=job_name,memory=max_memory,queue=queue,parent_record_id=parent_record_id,charge=charge,submit_time=str(submit_time),start_time=str(start_time),end_time=str(end_time),local_project_id=project_id,local_record_id=local_record_id,local_reference=local_reference)


#!/usr/bin/python3

#  slurm_aces.py - Extracts aces ACCESS usage information from 'aces_job_processing' and 'aces_job_table' for entries with valid Slurm IDs.
#
#  The general strategy is:
#
#  1. Extract unique ACCESS Slurm account (slurm_acct) numbers for aces accounts in 'local_info' table.
#  2. Extract last 'serial_no' - corresponding to 'job_table_record_id' in 'aces_job_processing' table - from 'usage_compute' table.
#  3. Extract all the 'job_table_record_id' values from the 'aces_job_processing' table where 'state' = 1.
#  4. Perform an inner join of 'aces_job_processing' and 'aces_job_table' on the first column serial numbers to extract
#     the info from 'aces_job_table' needed to construct an ACCESS usage report packet.
#  5. Find 'job_table_record_id' values for first and last values extracted from inner join in previous step.
#  6. Loop over extracted info serial numbers from the last 'serial_no' in 'usage_compute' + 1 to the last 'job_table_record_id' extracted via the inner join.
#    a. Check if the Slurm ID from 'aces_job_table' - i.e. the 'account' column' - matches an entry in the list of ACCESS Slurm IDs extracted from 'local_info'.
#    b. Check if the serial number is already in the 'usage_compute' table.
#    c. Check if the service units value is non-zero.
#    d. Create a new entry in the 'usage_compute' table if all three checks are passed.

import sys
import os
import logging
import mysql.connector
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from configparser import RawConfigParser
from datetime import datetime,timezone
import pytz

#  Configuration and log file processing.
log_file = '/home/baum/AMIEDEV/amiedev.log'
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

script_name = sys.argv[0][2:]

dir_dev = '/home/baum/AMIEDEV'
dir_pro = '/home/baum/AMIE'
cwd = os.getcwd()
if cwd == dir_pro:
    slurm_config = "/home/baum/AMIE/slurm.ini"
    amie_config = "/home/baum/AMIE/amie.ini"
    print(" PRODUCTION MODE!!!!")
elif cwd == dir_dev:
    slurm_config = "/home/baum/AMIEDEV/slurm.ini"
    amie_config = "/home/baum/AMIEDEV/amiedev.ini"
    print(" DEVELOPMENT MODE!!!!!")
else:
    print(" Not running in either ",dir_dev," or ",dir_pro,".  Exiting.")
    sys.exit()

select_range = 0
diagnostics = 0

#  Command-line argument processing
change_last_index = 0
diag = 0
getdat = 0
help_list = ['-h','--h','-H','--H','-help','--help','-Help','--Help']
#  Check for the presence of a command-line argument.
if len(sys.argv) > 1:
#  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(" Program: ",script_name," - demonstrate the use of a function for PostgreSQL access via Python/psycopg2")
        print(" Arguments: ",help_list," - this message")
        print("             None - run the program")
        print("             c N - process only a maximum of N pending usage entries")
        print("             d - print diagnostics and exit")
        print("             1 - run the demo program with additional diagnostic output")
        print("             s - start at the ordinal number that follows 's'") 
        print("             t - run test and exit")
        print(" ")
        sys.exit()
    else:
#  Check for the only presently valid command-line argument "1".
        if sys.argv[1] == '1':
#            print(" Command-line argument is ",sys.argv[1])
            diag = int(sys.argv[1])
#            print(" Printing diagnostics.")
        elif sys.argv[1] == 'g':
            getdat  = 1
        else:
#  Kick all invalid arguments ingloriously out of the script.
            print(" ")
            print(" The command-line argument entered '",sys.argv[1],"' is meaningless.  Exiting.")
            print(" Try one of ",help_list," for available valid arguments.")
            print(" ")
            sys.exit()

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

cluster = 'aces'
test = 0

#  Obtain login info for AMIE PostgreSQL db.
config_site = RawConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Obtain login info for SLURM MySQL db.
config_site.read(slurm_config)
m_site_con = config_site['SLURM']
m_pw = m_site_con['pw']
m_dbase = m_site_con['dbase']
m_dbuser = m_site_con['dbuser']
m_localhost = m_site_con['local_address']
#mysql -h mgt1 -u ams -p slurm_acct_db
#print(" m_dbuser = ",m_dbuser," m_dbase = ",m_dbase," m_localhost = ",m_localhost," m_pw = ",m_pw)

#  Create lists of valid slurm account numbers and valid PI IDs.
sql = "SELECT DISTINCT slurm_acct,project_id,person_id FROM local_info WHERE pi = 'Y' AND cluster = %s AND proj_stat = 'active' AND acct_stat = 'active' ORDER BY slurm_acct"
data = (cluster,)
results = []
amiedb_call(sql,data,script_name,results)
print("results = ",results)
valid_slurm_account_numbers = []
valid_pi_ids = []
for accts in results:
#  Filter out NAIRR accounts.
#    if not str(accts[1]).startswith("p.nairr"):
    valid_slurm_account_numbers.append(int(accts[0]))
    valid_pi_ids.append(accts[2])
#####  MODIFICATION FOR BACKFILLING NAIRR USAGE.
##### SET SINGLE SLURM ACCOUNT NUMBER HERE.
#    valid_slurm_account_numbers = ['157199739541']
#    valid_pi_ids = ['u.db219973']
#####
print("Summary of valid slurm account number information:")
print(" ")
print("There are ",len(valid_slurm_account_numbers)," ACCESS slurm account numbers in 'local_info':")
print(valid_slurm_account_numbers)
print(" ")

##### EXIT
#sys.exit()
print("The ",len(valid_pi_ids)," corresponding PI person_id values:")
print(valid_pi_ids)

#  Create list of dictionaries containing all users for each project_id/slurm_acct.
users_per_slurm_acct = []
for acct in valid_slurm_account_numbers:
#    print(acct)
#    print(" ")
#    sql = "SELECT person_id FROM local_info WHERE slurm_acct = %s AND acct_stat = 'active' AND proj_stat = 'active'"
#####  MODIFICATION FOR BACKFILLING NAIRR USAGE.
#    sql = "SELECT person_id FROM local_info WHERE slurm_acct = %s"
    sql = "SELECT person_id FROM local_info WHERE slurm_acct = %s AND acct_stat = 'active' AND proj_stat = 'active'"
    data = (str(acct),)
    results = []
    amiedb_call(sql,data,script_name,results)
#    print(results)
#    print(" ")
    acct_dict = {}
    acct_list = []
    for peid in results:
        acct_list.append(peid[0])
#    print(acct_list)
    acct_dict.update({acct:acct_list})
#    print("acct_dict = ",acct_dict)
    users_per_slurm_acct.append(acct_dict)
#    print("users_per_slurm_acct = ",users_per_slurm_acct)

print("no. of accounts = ",len(users_per_slurm_acct))
print(" ")
print("users_per_slurm_acct = ",users_per_slurm_acct)
#    sys.exit()

##### EXIT FOR MYPROJ
#sys.exit()

# Print no. of entries in usage_compute_aces for each Slurm account number.
with open('usage.txt', 'w') as f:
    for san in valid_slurm_account_numbers:
        sql = "select count(*) from usage_compute_aces where local_record_id = %s"
        data = (str(san),)
        results = []
        amiedb_call(sql,data,script_name,results)
        try:
            total_entries = results[0][0]
        except:
            total_entries = 0
        sql = "select person_id,project_id,access_id from local_info where slurm_acct = %s and pi = 'Y'"
        data = (str(san),)
        results = []
        amiedb_call(sql,data,script_name,results)
        (person_id,project_id,access_id) = results[0]
        print(person_id,project_id,access_id,san,str(total_entries), file=f)

os.system("cat usage.txt | sort -nk5 | tac > usage_report_totals.txt")

#cat out.txt | sort -nk5 | tac > outsort.txt


#sys.exit()
#  CREATE UID LIST
#  Create a list of all uid values associated with the valid slurm account numbers.
#  Extract all uid values for a given slurm_acct for users with active accounts.
valid_uids = []
valid_uids_duplicates = []
#  Iterate over all valid slurm account numbers.
for slurm_no in valid_slurm_account_numbers:
#####  MODIFICATION FOR BACKFILLING NAIRR USAGE.
#    sql = "select uid,person_id,project_id from local_info where slurm_acct = %s"
    sql = "select uid,person_id,project_id from local_info where slurm_acct = %s and acct_stat = 'active'"
    data = (str(slurm_no),)
    results = []
    amiedb_call(sql,data,script_name,results)
#    print("slurm no. = ",slurm_no," results = ",results)
    for nos in results:
#        print("Adding uid: ",nos[0])
        if nos[0] in valid_uids:
#            print("UID no. ",nos[0]," already exists in the list.")
            valid_uids_duplicates.append(int(nos[0]))
        valid_uids.append(int(nos[0]))
#  Sort lists.
valid_uids.sort()
valid_uids_duplicates.sort()
#  Remove duplicates from lists.
valid_uids = list(dict.fromkeys(valid_uids))
valid_uids_duplicates = list(dict.fromkeys(valid_uids_duplicates))
#  Find length of lists.
no_valid_uids = len(valid_uids)
no_valid_uids_duplicates = len(valid_uids_duplicates)
print(" ")
print("Summary of valid uid information: ")
print(" ")
print("There are ",no_valid_uids," uid values associated with the valid slurm account numbers:")
print(valid_uids)
print(" ")
print("There are ",no_valid_uids_duplicates," non-unique values in the list:")
print(valid_uids_duplicates)
print(" ")

pi_pids = []
pi_uids = []
for vid in valid_uids:
#    sql = "select person_id from local_info where uid = %s and pi = 'Y' and cluster = 'aces' and proj_stat = 'active'"
#####  MODIFICATION FOR BACKFILLING NAIRR USAGE.
#    sql = "select distinct person_id,slurm_acct,uid from local_info where uid = %s and pi = 'Y' and cluster = 'aces'"
    sql = "select distinct person_id,slurm_acct,uid from local_info where uid = %s and pi = 'Y' and cluster = 'aces' and proj_stat = 'active'"
    data = (vid,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) != 0:
        pi_uids.append(vid)
        pi_pids.append(results[0][0])

pi_order = sorted(pi_pids)
print("Number of PI uid values: ",len(pi_uids))
print("Number of PI person_id values = ",len(pi_order))
print("Valid PI uid values: ",pi_uids)
print("Valid PI person_ids: ",pi_order)

#  EXTRACT LIST OF VALID JOB ID NUMBERS FROM aces_job_processing TABLE
try:
    conn = mysql.connector.connect(user=m_dbuser,password=m_pw,host=m_localhost,database=m_dbase)
except:
    print(" MySQL connection to 'slurm_acct_db' failed.")
cursor = conn.cursor()

#  Create list of valid 'job_table_record_id' values in 'aces_job_processing'.
valid_job_table_record_id_values = []
query = "select job_table_record_id from aces_job_processing where state = '1' order by 'job_table_record_id'"
data = ()
cursor.execute(query,data)
for values in cursor:
    valid_job_table_record_id_values.append(values[0])
print("List from 'aces_job_processing'':")
print("no. of valid entries: ",len(valid_job_table_record_id_values))
print("first no.: ",valid_job_table_record_id_values[0])
print("last no.: ",valid_job_table_record_id_values[-1])

cursor.close()
conn.close()

no_of_entries_processed = 0
slurm_acct_not_integer = 0
already_in_usage_compute_aces = 0
invalid_slurm_acct_no = 0
service_units_are_zero = 0
null_entries_processed = 0
null_entries_skipped = 0

if diagnostics == 1:
    sys.exit()

logtstamp = tstamp().split(" ")[0] + "T" + tstamp().split(" ")[1].replace(":","-")
null_log = "/home/baum/AMIE/null_logs/null_jobs.log"
#null_log = "null_logs/" + "null_jobs-" + logtstamp + ".log"
msg1 = "Log of aces jobs that can't be processed to send a usage packet.\n"
msg2 = " job index  slurm acct.   uid    SU\n"

#  Create a list of all ordered 'serial_no' values in 'usage_compute_aces'.
sql = "SELECT serial_no FROM usage_compute_aces ORDER BY serial_no"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
ordered_serial_no_list = []
for nos in results:
    ordered_serial_no_list.append(int(nos[0]))
print("List from 'usage_compute_aces':")
print("First serial_no: ",ordered_serial_no_list[0])
print("Last serial_no: ",ordered_serial_no_list[-1])
print("Length of serial_no: ",len(ordered_serial_no_list))

##### EXIT
#sys.exit()

if getdat == 1:
    sys.exit()

#  Remove values in 'ordered_serial_no_list' from 'valid_job_table_record_id_values'.
valid_indexes = list(set(valid_job_table_record_id_values) - set(ordered_serial_no_list))
valid_indexes.sort()
unprocessed_entries = len(valid_indexes)

if change_last_index == 1:
    print("CHANGING NUMBER OF VALID ENTRIES TO ",no_of_entries_to_process)
    no_of_valid_indexes = no_of_entries_to_process
    last_index = valid_indexes[no_of_valid_indexes-1]
    valid_indexes = valid_indexes[0:no_of_entries_to_process]
    print("Processing ",no_of_valid_indexes," 'valid_indexes' entries from ",valid_indexes[0]," to ",last_index,".")
else:
    last_index = valid_indexes[-1]

print("Purged list:")
purged_length = len(valid_indexes)
print("Number of valid indexes to process = ", purged_length)
purge_min = valid_indexes[0]
print("Minimum serial number = ",purge_min)
purge_max = valid_indexes[-1]
print("Maximum serial number = ",purge_max)

#  Select the largest 'id' value in 'usage_compute_aces'.
sql = "SELECT id FROM usage_compute_aces ORDER BY id DESC LIMIT 1"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
last_id = results[0][0]

invalid_slurm_acct_list = []

usage_id = last_id + 1
print("First 'id' to be added is ",usage_id)
##### EXIT
#sys.exit()
for valid_index in valid_indexes:

    print("valid_index = ",valid_index)
    parent_record_id = 'optional'
    local_reference = 'optional'

#  Keep track of number of entries processed if there is an override.
    if change_last_index == 1:
        if no_of_entries_processed > no_of_entries_to_process-1:
            sys.exit()

#  Set up the connection for the MySQL DB.
    conn = mysql.connector.connect(user=m_dbuser,password=m_pw,host=m_localhost,database=m_dbase)
    cursor = conn.cursor()

#  PERFORM AN INNER JOIN BETWEEN aces_job_table AND aces_job_processing TO CONNECT CHARGES WITH SLURM ACCOUNT NUMBERS
#  Perform the inner join extraction.
    try:
        valid_index = int(valid_index)
        print("valid_index = ",valid_index)
        query = "SELECT job_db_inx,account,time_submit,time_start,time_end,update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req,job_name,id_user,`partition` FROM aces_job_table INNER JOIN aces_job_processing ON aces_job_table.job_db_inx = aces_job_processing.job_table_record_id where job_db_inx = %s"
        data = (valid_index,)
        cursor.execute(query,data)
        matches = cursor.fetchall()
        print("matches[0] = ",matches[0])
#        print("Finished query execution.")
#  Extract required values from this line.
        (job_db_inx,account,time_submit,time_start,time_end,update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req,job_name,uid,partition) = matches[0]
#        print(" Values = ",job_db_inx,account,time_submit,time_start,time_end,update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req,job_name,uid,partition)
    except Exception as e:
        print("Problem with inner join for index = ",job_db_inx)
        print("ERROR: ",str(e))

#  CHECK IF SLURM ACCOUNT NUMBER IS AN INTEGER
    try:
        slurm_acct = int(account)
        null = 0
    except:
        null = 1

#  Convert UNIX time integers to UTC values using 'unix_to_utc'.
#
#  This converts UNIX times of the form 6846743950 to the form 2025-09-23 18:30:57+00:00, but they
#  are stored in 'usage_compute_aces' in the form 2025-09-23 18:30:57 because the data type of
#  submit_time etc. is 'timestamp without time zone'.  This prevents PostgreSQL from automatically
#  converting the datetime into the local time zone equivalent.
#
#    utc_time_submit = unix_to_utc(int(time_submit))
#    print("utc_time_submit = ",utc_time_submit," time_submit = ",time_submit)
#    utc_time_start = unix_to_utc(int(time_start))
#    utc_time_end = unix_to_utc(int(time_end))

##### EXIT
#    sys.exit()

# -----------------------------------------------
    if null == 0:
# -----------------------------------------------
#  Extract needed info from 'local_info' for valid slurm account numbers.
        if slurm_acct in valid_slurm_account_numbers:
            print("slurm_acct = ",slurm_acct," is a valid slurm account number for an ACCESS user.")
            try:
                query2 = "SELECT person_id,project_id,access_id FROM local_info WHERE slurm_acct = %s and cluster = %s and uid = %s"
                data2 = (str(slurm_acct),cluster,uid,)
                results = []
                amiedb_call(query2,data2,script_name,results)
                if len(results) == 0:
                    print("No 'local_info' entry for slurm_acct = ",str(slurm_acct),", cluster = ",cluster," and uid = ",uid,". Skipping to next entry.")
                    continue
                else:
                    (person_id,project_id,access_id) = results[0]
            except Exception as e:
                print("Problem obtaining 'local_info' information with valid slurm ACCESS account number.")
                print("No 'person_id', 'project_id' and 'access_id' match for slurm_acct = ",slurm_acct,", cluster = ",cluster," uid = ",uid)
                print("ERROR: ",str(e))
#  Skip to next index number for invalid slurm account numbers.
# -----------------------------------------------
        else:
# -----------------------------------------------
            print("Invalid slurm account number ",slurm_acct)
            invalid_slurm_acct_no = invalid_slurm_acct_no + 1
            if slurm_acct not in [invalid_slurm_acct_list]:
                invalid_slurm_acct_list.append(slurm_acct)
            continue
#  If account number isn't an integer, switch over to using the 'id_user' info.
    else:
        if uid in valid_uids and uid not in valid_uids_duplicates:
            print("################################################################")
            print("################################################################")
            print("################################################################")
            try:
                query2 = "SELECT person_id,project_id,access_id,slurm_acct FROM local_info WHERE cluster = %s and uid = %s"
                data2 = (cluster,uid,)
                results = []
                amiedb_call(query2,data2,script_name,results)
#  If 'cluster/uid' combination does not exist, skip.
                if len(results) == 0:
                    print("No 'local_info' entry for cluster = ",cluster," and uid = ",uid,". Skipping.")
                    continue
#  If combination exists, extract 'person_id', 'project_id', 'access_id' and - last but not least - 'slurm_acct'.
                else:
                    (person_id,project_id,access_id,slurm_acct) = results[0]
                    local_reference = 'none'
#                    print("person_id = ",person_id," project_id = ",project_id," access_id = ",access_id," slurm_acct = ",slurm_acct)
                    null_entries_processed = null_entries_processed + 1
#  If query bombs, tell us why.
            except Exception as e:
                print("Problem obtaining 'local_info' information with 'uid'. Skipping.")
                print("No 'person_id', 'project_id' and 'access_id' match for cluster = ",cluster," and uid = ",uid)
                print("ERROR: ",str(e))
                continue

#  Send and log a message about jobs that cannot be processed because the 'uid' is not unique for the given 'cluster'.
        else:
            print("Cannot extract usage information for job ID ",valid_index," for non-unique 'uid' ",uid,". Skipping.")
            try:
                sacct = str(account)
            except:
                sacct = 'unknown'
            msg1 = "   " + str(job_db_inx) + "      " + str(sacct) + "      " + str(uid) + "      " + str(service_units) + "\n"
            with open(null_log, "a") as f:
                f.write(msg1)
            null_entries_skipped = null_entries_skipped + 1
            continue
# -----------------------------------------------
#  If construct done.
# -----------------------------------------------

    status = 'N'
#  Check if 'aces_job_processing' serial number is already in 'usage_compute_aces' table.  If so, skip to next line.
    query4 = "SELECT * FROM usage_compute_aces WHERE serial_no = %s"
    data4 = (job_db_inx,)
    results = []
    amiedb_call(query4,data4,script_name,results)
#    print("CHECK")
#    print("results = ",results)
    if len(results) > 0:
        print("The 'aces_job_processing' serial no. ",job_db_inx," is already in 'usage_compute_aces'.  Skipping to next line.")
        print("---------------------------------------------------------------------")
        already_in_usage_compute_aces = already_in_usage_compute_aces + 1
        continue
#  Skip to next line of service_units value is effectively or actually zero.
    print("service_units = ",service_units)
    if service_units < 0.000001:
        print("Skipping to next line due to 'service_units' value (",service_units,") being effectively or actually zero.")
        print("---------------------------------------------------------------------")
        service_units_are_zero = service_units_are_zero + 1
        continue

#  Insert an entry into 'usage_compute_aces'.
    if test == 0:
        query3 = """INSERT INTO usage_compute_aces (id,serial_no,person_id,project_id,uid,local_record_id,resource,submit_time,start_time,end_time,charge,node_count,cpu_core_count,job_name,max_memory,queue,parent_record_id,local_reference,time_stored,access_id,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        print("$$$$$$$ VALUES TO INSERT INTO usage_compute_aces $$$$$")
        print(usage_id,valid_index,person_id,project_id,uid,slurm_acct,cluster,time_submit,time_start,time_end,service_units,int(nodes_alloc),int(num_cpus),id_job,mem_req,partition,parent_record_id,local_reference,tstamp(),access_id,status)
        data3 = (usage_id,valid_index,person_id,project_id,uid,slurm_acct,cluster,str(time_submit),str(time_start),str(time_end),service_units,int(nodes_alloc),int(num_cpus),id_job,mem_req,partition,parent_record_id,local_reference,tstamp(),access_id,status)

        print("Attempting insertion.")
        try:
            results = []
            amiedb_call(query3,data3,script_name,results)
            print("Successful insertion of usage report no. ",valid_index," into 'usage_compute_aces' for ",person_id," on project ",project_id)
            usage_id = usage_id + 1
        except Exception as e:
            msg1 = "Problem inserting serial no. " + valid_index + " into 'usage_compute_aces' for user " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + ". Skipping to next serial number."
            msg2 = "Error message: " + str(e)
            print(msg1 + "\r\n" + msg2)
            logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
            send_email(msg1 + "\r\n" + msg2)
            continue
#    cursor.close()
#        conn.close()
    else:
        print("TEST:  Not inserting serial no. ",valid_index," entry into 'usage_compute_aces'.")

    print("---------------------------------------------------------------------")

    no_of_entries_processed = no_of_entries_processed + 1
##### EXIT for testing a single line.
#    sys.exit()

print("Slurm account numbers not an integer: ",slurm_acct_not_integer)
print("Entries already in 'usage_compute_aces': ",already_in_usage_compute_aces)
print("Invalid slurm account numbers: ",invalid_slurm_acct_no)
print("Invalid numbers: ",invalid_slurm_acct_list)
print("Service units are zero: ",service_units_are_zero)
print("Number of entries processed: ",no_of_entries_processed)
print("Number of null entries processed: ",null_entries_processed)
print("Number of null entries skipped: ",null_entries_skipped)
print(" ")
print("No. of packets processed: ",purged_length)
print("Min. Slurm ID: ",purge_min)
print("Max. Slurm ID: ",purge_max)
print("Unprocessed entries: ",unprocessed_entries)

cursor.close()
conn.close()

sys.exit()




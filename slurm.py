#!/usr/bin/python3

#  Test program for connecting to Slurm MySQL database on faster.
#  Extra stuff.

import sys
import os
import logging
import mysql.connector
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from configparser import RawConfigParser
from datetime import datetime

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

#  Find largest value of 'serial_no' in 'usage_compute' table to know where to begin in 'job_processing' table.
#
sql = "SELECT serial_no FROM usage_compute WHERE serial_no = (SELECT MAX(serial_no) FROM usage_compute)"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
try:
    starting_index = results[0][0]
except:
    starting_index = 1
print(" Starting serial no. in 'usage_compute' table = ",starting_index)

#  Find the largest job_table_record_id in the 'job_processing' table in the 'slurm_acct_db' DB to know where to stop.
#
#  NOTE:  This is for the synthetic development table 'job_processing' created in the AMIE PostgreSQL DB.
if cwd == dir_dev:
    sql = "SELECT MAX(job_table_record_id) FROM job_processing"
    data = ()
    results = []
    amiedb_call(sql,data,script_name,results)
    last_index = results[0][0]
    print(" Last index in 'job_processing' table = ",last_index)
#    sys.exit()

elif cwd == dir_pro:
#  NOTE:  This is for the actual table in the MySQL slurm_acct_db DB.
    try:
        conn = mysql.connector.connect(user=m_dbuser,password=m_pw,host=m_localhost,database=m_dbase)
#        print(" MySQL connection succeeded.")
    except:
        print(" MySQL connection to 'slurm_acct_db' failed.")
    cursor = conn.cursor()

    try:
        query = "SELECT MAX(job_table_record_id) from job_processing"
#       data()
        cursor.execute(query)
        for (job_table_record_id) in cursor:
            last_index = job_table_record_id[0]
            print(" last_index = ",last_index)
        cursor.close()
        conn.close()
        print(" Obtained last index from 'job_processing' table.")
    except:
        print(" Failed to obtain last index from 'job_processing' table.")

print(" Cycling through 'job_processing' table from ",starting_index," to ",last_index)

for no_proc in range(starting_index,last_index):

    print(" Processing line (no_proc) #",no_proc)
##########
#  GRAB LINES FROM THE JOINED faster_job_table/job_processing TABLE
##########
    try:
        conn = mysql.connector.connect(user=m_dbuser,password=m_pw,host=m_localhost,database=m_dbase)
        cursor = conn.cursor()
        print(" MySQL connection for joined table query succeeded.")
    except:
        print(" MySQL connection failed for joined table query.")

#  INNER JOIN BETWEEN faster_job_table AND job_processing - limit of 2 for testing
#  MySQL & PostgreSQL

### NOTE (22/12/15):  This only working for the first two lines of the joined table since 'job_processing' only has two lines.

    try:
        query = "SELECT job_db_inx,account,from_unixtime(time_submit),from_unixtime(time_start),from_unixtime(time_end),update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req FROM faster_job_table INNER JOIN job_processing ON faster_job_table.job_db_inx = job_processing.job_table_record_id where job_db_inx = %s"
        data = (no_proc,)
        cursor.execute(query,data)

#        results = []
#        matches = cursor.fetchall()
#        len_matches = len(matches)
#        print(" no_proc = ",no_proc," len_matches = ",len_matches)
#        for match in matches:
#            results.append(match)
#        print(" results[0] = ",results[0])

#  Loop over unprocessed lines in 'job_processing' table.
        for (job_db_inx,account,time_submit,time_start,time_end,update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req) in cursor:
            print(" Values = ",job_db_inx,account,time_submit,time_start,time_end,update_time,service_units,nodes_alloc,num_cpus,id_job,mem_req)

        print(" Extracted values from joined tables.")

    except:
        print(" Failed to extract values from joined tables.")

    cursor.close()
    conn.close()


##########
#  Grab 'person_id' and 'project_id' from 'local_info' based on 'slurm_acct' being the same as 'account' in 'faster_job_table'
##########
#  Dummy 'account' number since 'faster_job_table' doesn't yet (7/22) contain them.
    account = '145718212538'

    query2 = "SELECT person_id,project_id,uid,cluster FROM local_info WHERE slurm_acct = %s"    
    data2 = (account,)
    results = []
    amiedb_call(query2,data2,script_name,results)
    print(" results = ",results)

    person_id,project_id,uid,cluster = results[0]
#    print(" uid = ",uid)
#    uid = int(uid)
#    print(" uid = ",uid)
#    print(person_id,project_id,uid,cluster)

#    cursor2.close()
#    conn2.close()

    try:
        conn3 = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        cursor3 = conn3.cursor()
#        print(" Opened conn3.")
    except:
        print(" Cannot open conn3.")

##########
#  Insert all these values into 'usage_compute'
##########
    resource = cluster
    account = 'UNKNOWN'
    job_name = 'UNKNOWN'
    queue = 'UNKNOWN'
    parent_record_id = 'optional'
    local_reference = 'optional'

#  The information required for a 'compute_usage_record' packet is found in the two tables
#  'faster_job_table' and 'job_processing' in the 'slurm_acct_db' database and in the table
#  'local_info' in the 'amiedb' database.  This information is combined and placed into the
#  'usage_compute' table in the 'amiedb' database, from where it will be extracted and sent
#  in packets.

#  EQUIVALENT VARIABLES BETWEEN usage_compute and faster_job_table TABLES
#
#
#                                                                                                  compute_usage_record
#      usage_compute        faster_job_table            job_processing           local_info              packet
#
#     serial_no (bigint)      job_db_inx (bigint)    job_table_record_id int
#     person_id (char)                                                        person_id (char)        Username
#     project_id (char)                                                       project_id (char)       LocalProjectID
#     uid (int)                                                               uid (int)
#     local_record_id (char)  id_job (int)                                                            LocalRecordID
#     resource (char)                                                         cluster (char)          Resource
#     charge (double)                                service_units (float)                            Charge
#     cpu_core_count (int)                           num_cpus (int)                                   CpuCoreCount
#     submit_time (ts)        time_submit (bigint)                                                    SubmitTime
#     start_time (ts)         time_start (bigint)                                                     StartTime
#     end_time (ts)           time_end (bigint)                                                       EndTime
#     node_count              nodes_alloc (int)                                                       NodeCount
#     job_name (char)         job_name (tinytext)                                                     JobName
#     max_memory (bigint)     mem_req (bigint)                                                        Memory
#     queue char                                                                                      Queue
#     parent_record_id        OPTIONAL                                                                [ParentRecordID]
#     local_reference         OPTIONAL                                                                [LocalReference]
#     time_stored
#     time_sent

#    print(" time_submit = ",time_submit)
#    time_submit = time_submit.strftime("%Y-%m-%d %H:%M:%S")
#    time_start = time_start.strftime("%Y-%m-%d %H:%M:%S")
#    time_end = time_end.strftime("%Y-%m-%d %H:%M:%S")

    try:
        query3 = """INSERT INTO usage_compute (serial_no,person_id,project_id,uid,local_record_id,resource,submit_time,start_time,end_time,charge,node_count,cpu_core_count,job_name,max_memory,queue,parent_record_id,local_reference,time_stored) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        data3 = (no_proc,person_id,project_id,uid,account,cluster,time_submit,time_start,time_end,service_units,int(nodes_alloc),int(num_cpus),id_job,mem_req,queue,parent_record_id,local_reference,tstamp())
#    data3 = (job_db_inx,person_id,project_id,uid,account,cluster,time_submit,time_start,time_end,service_units,int(nodes_alloc),int(num_cpus),id_job,mem_req,queue,parent_record_id,local_reference,tstamp())
        results = []
        amiedb_call(query3,data3,script_name,results)

        cursor3.close()
        conn3.close()
        print(" Insertion of usage report no. ",no_proc," into 'usage_compute' succeeded for ",person_id," on project ",project_id)

    except:
        print(" Insertion of usage report no. ",no_proc," into 'usage_compute' failed for ",person_id," on project ",project_id)

sys.exit()


cursor = conn.cursor()

#  slurm_acct = 145555724476
#  slurm_acct = faster_id + str(int(pi_global_id) + 500000) + newrand

query = ("SELECT id_job,account,from_unixtime(time_submit),from_unixtime(time_start),from_unixtime(time_end),nodes_alloc,cpus_req,job_name,mem_req FROM faster_job_table LIMIT 2")
data = ()

cursor.execute(query,data)

#  slurm_acct in 'local_info' is varchar
test_slurm = '145555724476'

for (id_job,account,time_submit,time_start,time_end,nodes_alloc,cpus_req,job_name,mem_req) in cursor:

    account = test_slurm
    pi_global_id = int(account[2:8]) - 500000

    print(id_job,account,pi_global_id,time_submit,time_start,time_end,nodes_alloc,cpus_req,job_name,mem_req)

conn.close()

#  GRAB CORRESPONDING INFO FROM THE job_processing TABLE

query = ("SELECT id_job,account,from_unixtime(time_submit),from_unixtime(time_start),from_unixtime(time_end),nodes_alloc,cpus_req,job_name,mem_req FROM faster_job_table LIMIT 5")
data = ()


conn.close()

sys.exit()



from configparser import ConfigParser
from datetime import datetime
import sys
import logging
import os
import json
import random
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from amieclient import AMIEClient
from amieclient import UsageClient
from amieclient.usage import ComputeUsageRecord
from amieclient.usage import AdjustmentUsageRecord
from amieclient.usage import UsageStatus
from amieclient.usage import UsageResponse
from amieclient.usage import UsageStatusResource

script_name = sys.argv[0][2:]

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

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Create the AMIE client.

usage_client = UsageClient(site_name=site_con['site_name'],
                        usage_url='https://usage.xsede.org/api/v1_test',
                        api_key=site_con['api_key'])

#amie_client = AMIEClient(site_name=site_con['site_name'],
#                         amie_url=site_con['amie_url'],
#                         api_key=site_con['api_key'])

#usage_type = 'job'
usage_type = 'adjustment'
#  Handle job information packets via ComputeUsageRecord packets.
if usage_type == 'job':

#  https://github.com/XSEDE/amieclient/blob/b96c3bc759b240dbaece5df8d0aa6b7130887dfb/amieclient/usage/record.py
#  This is handled by the ComputeUsageRecord class in record.py.
#
#        return cls(
#            username=input_dict['Username'], - the locally created user ID
#            local_project_id=input_dict['LocalProjectID'], - the locally created project ID
#            local_record_id=input_dict['LocalRecordID'], - 
#            resource=input_dict['Resource'],
#            submit_time=input_dict['SubmitTime'],
#            start_time=input_dict['StartTime'],
#            end_time=input_dict['EndTime'],
#            charge=input_dict['Charge'],
#            node_count=input_dict['Attributes'].get('NodeCount'),
#            cpu_core_count=input_dict['Attributes'].get('CpuCoreCount'),
#            job_name=input_dict['Attributes'].get('JobName'),
#            memory=input_dict['Attributes'].get('Memory'),
#            queue=input_dict['Attributes'].get('Queue'),
#            parent_record_id=input_dict.get('ParentRecordID'),
#            local_reference=input_dict.get('LocalReferen
#        )

    username = 'pi.dm55572'  # person_id in local_info
    local_project_id = 'p.sta226239.000'   # project_id in local_info
    charge = 0.1
    local_record_id = 3   #  trans_rec_id in local_transaction
    resource = 'faster.tamu.xsede.org'
    submit_time = '2022-06-09T01:00:00-05:00'
    start_time = '2022-06-09T02:00:00-05:00'
    end_time = '2022-06-09T03:00:00-05:00'
    node_count = 8

#    cur = ComputeUsageRecord(charge=charge,end_time=end_time,local_project_id=local_project_id,local_record_id=local_record_id,resource=resource,start_time=start_time,submit_time=submit_time,username=username,node_count=node_count).pretty_print()
    cur = ComputeUsageRecord(charge=charge,end_time=end_time,local_project_id=local_project_id,local_record_id=local_record_id,resource=resource,start_time=start_time,submit_time=submit_time,username=username,node_count=node_count)

#  Required.
    queue = 'bigoldqueue'
    cpu_core_count = 24
    job_name = 'rastus'
    memory = 12345000
#  Optional.
    parent_record_id = 'tamu7345653'
    local_reference = 'GIGEM-faster'

#print(dir(cur))

#    req = 'status'
#    req = 'send'
    if req == 'send':

#  A 'person_id' in the 'local_info' table.
        cur.Username = username
#  The 'project_id' corresponding to 'person_id' in 'local_info' table.
        cur.LocalProjectID = local_project_id
#  A site job ID, typically the Slurm job ID.
        cur.LocalRecordID = local_record_id
#  The resource the job ran on, e.g. 'faster.tamu.xsede.org'.
        cur.Resource = resource
#  The time the job was submitted.
        cur.SubmitTime = submit_time
#  The time the job started after it was submitted.
        cur.StartTime = start_time
#  The time the job ended.
        cur.EndTime = end_time
#  The charge for the job.
        cur.Charge = charge
#  The number of nodes the job ran on.
        cur.NodeCount = node_count
#  The number of cores used by the job.
        cur.CpuCoreCount = cpu_core_count
#  The job name.  This is not clarified in the source code comments.
        cur.JobName = job_name
#  The maximum memory used in a node.
        cur.Memory = memory
#  The name of the scheduler queue to which the job was submitted.
        cur.Queue = queue
#  An optional job ID of the parent job if this record is a sub job, typically a Slurm job ID.
        cur.ParentRecordID = parent_record_id
#  An optional key to help identify this record locally.
        cur.LocalReference = local_reference

#  To send a usage request:
        print(" Trying 'send' here.")
        usage_client.send(cur)
        print(" Sent usage request.")

    elif req == 'status':

#  To find the status of a usage request.
#  The dictionary values must be Python datetime strings.
#  If you start with ISO format strings, they must first be converted to Python datetime strings.
        print(" Trying 'status' here.")
        iso_start_date = '2022-06-08T00:00:00'
        start_date = datetime.strptime(iso_start_date,"%Y-%m-%dT%H:%M:%S")
        iso_end_date = '2023-06-08T00:00:00'
        end_date = datetime.strptime(iso_end_date,"%Y-%m-%dT%H:%M:%S")
        print(" start date = ",start_date," end_date = ",end_date)
        print(usage_client.status(from_time=start_date,to_time=end_date))
        print(" Sent status request.")

    elif req == 'failed':
#  To get failed records.
        print(" Trying 'get_failed_records' here.")
        usage_client.get_failed_records()
        print(" Sent get_failed_records request.")
    elif req == 'clear':
#  To clear failed records:
        print(" Trying 'clear_failed_records'.")
        usage_client.clear_failed_records()
        print(" Sent 'clear_failed_records' request.")

    try:
        usage_client.send(cur)
        print(" Successfully sent 'ComputeUsageRecord' packet.")

#  Insert record of transaction into AMIE DB 'usage' table.
#        sql = "INSERT INTO usage_compute (person_id,project_id,local_site_id,resource,submit_time,start_time,end_time,charge,node_count,cpu_core_count,job_name,max_memory,job_queue)"
#        data = (person_id,project_id,local_site_id,resource,submit_time,start_time,end_time,charge,node_count,cpu_core_count,job_name,max_memory,job_queue)
#        results = []
#        amiedb_call(sql,data,script_name,results)

# PostgreSQL data types:
# person_id        | character varying           |           |          | 
# project_id       | character varying           |           |          | 
# local_record_id  | character varying           |           |          | 
# resource         | character varying           |           |          | 
# submit_time      | timestamp without time zone |           |          | 
# start_time       | timestamp without time zone |           |          | 
# end_time         | timestamp without time zone |           |          | 
# charge           | double precision            |           |          | 
# node_count       | integer                     |           |          | 
# cpu_core_count   | integer                     |           |          | 
# job_name         | character varying           |           |          | 
# max_memory       | bigint                      |           |          | 
# queue            | character varying           |           |          | 
# parent_record_id | character varying           |           |          | 
# local_reference  | character varying           |           |          |

    except:
        print(" Error in attempt to send 'ComputeUsageRecord' packet.")

#  Handle credit/debit packets via AdjustmentUsageRecord packets.
elif usage_type == 'adjustment':

#  https://github.com/XSEDE/amieclient/blob/b96c3bc759b240dbaece5df8d0aa6b7130887dfb/amieclient/usage/record.py
#  This is handled by the AdjustmentUsageRecord class in record.py.
#        return cls(
#            adjustment_type=input_dict['AdjustmentType'],
#            charge=input_dict['Charge'],
#            start_time=input_dict['StartTime'],
#            local_project_id=input_dict['LocalProjectID'],
#            local_record_id=input_dict['LocalRecordID'],
#            resource=input_dict['Resource'],
#            username=input_dict['Username'],
#            comment=input_dict.get('Comment'),
#            local_reference=input_dict.get('LocalReference'),
#        )

    adjustment_type = 'credit'
    charge = 0.2
    start_time = '2022-06-09T02:00:00-05:00'
    local_project_id = 'p.sta226239.000'
    local_record_id = 3
    resource = 'faster.tamu.xsede.org'
    username = 'pi.dm55572'

    aur = AdjustmentUsageRecord(adjustment_type=adjustment_type,charge=charge,start_time=start_time,local_project_id=local_project_id,local_record_id=local_record_id,resource=resource,username=username)
#    aur = AdjustmentUsageRecord(adjustment_type=adjustment_type,charge=charge,start_time=start_time,local_project_id=local_project_id,local_record_id=local_record_id,resource=resource,username=username).pretty_print()

#  Type of adjustment: 'credit', 'refund', 'storage-credit', 'debit', 'reservation' or 'storage-debit'.
    aur.AdjustmentType = adjustment_type
#  The amount of allocation units that should be deducted from the project allocation for this job.
    aur.Charge = charge
#  Time for which this adjustment should be applied, e.g. the job refund time should be the same as the job submit time.
    aur.StartTime = start_time
#  The ProjectID created when the project was created.
    aur.LocalProjectID = local_project_id
#  The AMIE site record ID to make the record locally identifiable and unique for the resource.
    aur.LocalRecordID = local_record_id
#  The resource on which the job ran.
    aur.Resource = resource
#  The local username of the user who ran the job.
    aur.Username = username
#  An optional comment.
#    aur.Comment = comment
#  An optional key for identifying this record locally.
#    aur.LocalReference = local_reference

    try:
        usage_client.send(aur)
        print(" Successfully sent 'AdjustmentUsageRecord' packet.")
    except:
        print(" Error in attempt to send 'AdjustmentUsageRecord' packet.")



#!/usr/bin/python3

#  Script for creating and sending locally originated 'notify_project_usage' packets.

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
import mysql.connector

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


    sql = "SELECT * FROM usage_compute WHERE serial_no = %s"
    data = (serial_no,)
    results = []
    amiedb_call(query2,data2,script_name,results)

    print(" results = ",results)
    sys.exit()


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



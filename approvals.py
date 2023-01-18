#!/usr/bin/python3

from configparser import ConfigParser
from datetime import datetime
from amieclient import AMIEClient
import sys
import logging
import os
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
from amieclient import AMIEClient

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

#  Define a suitably short time stamp macro.
def tstamp():
    tst = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return tst

# Read the AMIE info and create a client object.
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
print(' Processing ',npack,' packets with ',sys.argv[0],' at ',tstamp())
logging.info(sys.argv[0][2:] + ': processing ' + str(npack) + ' pre-processed packet(s)')

no_person_id = ['request_project_inactivate','request_project_reactivate']

# ================= READ APPROVAL STATUS FROM approval TABLE =============================


sql = "SELECT * FROM approval WHERE approval_status = 'unapproved';"
data = ()
results = []
amiedb_call(sql,data,script_name,results)

noapp = len(results)
if (noapp == 0):
    print(" There are no requests presently requiring approval.")

####### NOTE ##########

#  Add in capability to change automatically generated user/login name.

#  Cycle through unapproved requests.
napp = 1
for row in results:
    print(" %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    (tri, type_id, person_id, project_id, approval_status, ts_received, ts_approved, ts_reply, reply_status) = row
    print(" Packet no. = ",napp," person_id = ",person_id," project_id = ",project_id)
#  ================= EXTRACT ADDITIONAL INFO FROM local_info BASED ON person_id ====================

    if type_id in no_person_id:
        print(" Selecting for 'request_project_inactivate' request with project_id = ",project_id)
        sql = "SELECT * FROM local_info WHERE project_id = %s"
        data = (project_id,)
    else:
        print(" Selecting for requests other than 'request_project_[in|re]activate' with person_id = ",person_id)
        sql = "SELECT * FROM local_info WHERE person_id = %s"
        data = (person_id,)

    results2 = []
    amiedb_call(sql,data,script_name,results2)
    try:
        print(" results2[0] = ",results2[0])
    except:
        print(" No entry for user ID ",person_id," with project ID ",project_id," in 'local_info'.")
        continue
    (fn,ln,email,pid,project_id,remote_site_login,gn,sa,service_units_allocated,start_date,ed,ps,accts,uid,gid,cluster,access_id,pi) = results2[0]
#    print(" Processed results[0] for person_id.")
#    sys.exit()

    trans = tri
    print(' ')
    print(" ******************************************************************************************")
    print(" Request no. ",napp," of type '",type_id,"' received on ",ts_received," is unapproved.")
    print(" ******************************************************************************************")
    print(" Request is for ",fn," ",ln," at ",email," with user ID = ",pid,", project ID = ",project_id,", UID = ",uid," and GID = ",gid,".")
    print(" ")
    print(" -------------------------------------------------------------------------------------")
    app_status = input(" Enter 'approved' to approve this request or 'no' to deny it (default 'approved'): ")
    print(" -------------------------------------------------------------------------------------")
    if len(app_status) == 0:
        app_status = 'approved'
    if (app_status == 'approved'):
        print(" You have successfully entered '",app_status,"' to approve this request.")
        print(" ")
        print(" -------------------------------------------------------------------------------------")
        name_change = input(" Do you want to change the automatically generated user name (default 'n')? (y/n): ")
        print(" -------------------------------------------------------------------------------------")
        if len(name_change) == 0:
            name_change = 'n'
        if name_change == 'y':
            new_name = input(" Enter the new user name: ")
#  Change person ID in local_info.
            sql = "UPDATE local_info SET person_id = %s WHERE person_id = %s"
            data = (new_name,pid)
            results = []
            amiedb_call(sql,data,script_name,results)
#  Change person ID in approval.
            sql = "UPDATE approval SET person_id = %s WHERE person_id = %s"
            data = (new_name,pid)
            results = []
            amiedb_call(sql,data,script_name,results)
            print(" User name changed to ",new_name)
            print(" ")
        elif name_change == 'n':
            print(" No name change.")
            print(" ")
        else:
            print(" Something.")
#        sys.exit()

    elif (app_status == 'no'):
        print(" You have entered '",app_status,"' to maintain the request as 'unapproved'.")
    else:
        print(" You have entered the string '",app_status,"' which will not approve the request.  Rerun the program to try again.")
    if (app_status == 'approved'):
        sql = 'UPDATE approval SET approval_status = %s, ts_approved = %s WHERE trans_rec_id = %s'
        ts_approved = tstamp()
        data = (app_status, ts_approved, trans)
        results = []
        amiedb_call(sql,data,script_name,results)
        print(" Request type ",type_id," received on ",ts_received," approved on ",ts_approved,".")
        print(" ")
#  https://kb.objectrocket.com/postgresql/python-error-handling-with-the-psycopg2-postgresql-adapter-645

    napp = napp + 1


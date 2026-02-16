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
import fcntl

#  Configuration and log file processing.
#log_file = '/home/baum/AMIEDEV/amiedev.log'
#if not os.path.isfile(log_file):
#    print(" Log file ",log_file," doesn't exist.  Creating it.")
#    os.system("touch " + log_file)
#  Establish log file location.
#logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

script_name = 'approvals'
#script_name = sys.argv[0][2:]

lock_file_name = 'access.lock'
#lock_file_name = 'test.lock'

#  Establish the home directory of the scripts, logfiles, etc.
ACCESS_HOME = os.getcwd() + '/'
if ACCESS_HOME == '/home/baum/AMIEDEV/':
    amie_config = ACCESS_HOME + 'amiedev.ini'
    log_file = ACCESS_HOME + 'amiedev.log'
#    log_file = ACCESS_HOME + 'accessdev.log'
    lock_file_path = ACCESS_HOME + 'accessdev.lock'
    print(" DEVELOPMENT MODE!!!!!")
elif ACCESS_HOME == '/home/baum/AMIE/':
    amie_config = ACCESS_HOME + 'amie.ini'
    log_file = ACCESS_HOME + 'amie.log'
    lock_file_path = ACCESS_HOME + lock_file_name
#    lock_file_path = ACCESS_HOME + 'access.lock'
#    log_file = ACCESS_HOME + 'access.log'
    print(" PRODUCTION MODE!!!!")
else:
    print(" ACCESS_HOME = ",ACCESS_HOME)
    print(" Not running in either '/home/baum/AMIE' or '/home/baum/AMIEDEV'. Exiting.")
    sys.exit()

#  Check for lock file.  If it exists, exit.  If it doesn't, create one.
if os.path.isfile(lock_file_path):
    print("There is a lock file for 'approvals.py'.  Exiting.")
    sys.exit()
else:
    print(" No lock file.  Creating one.")
    file_lock = open(lock_file_path, 'a')


#  Establish logging capabilities.
if not os.path.isfile(log_file):
    print(" Log file ",log_file," doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(filename=log_file, format='%(asctime)s - %(message)s', level=logging.INFO)

#dir_dev = '/home/baum/AMIEDEV'
#dir_pro = '/home/baum/AMIE'
#cwd = os.getcwd()
#if cwd == dir_pro:
#    amie_config = "/home/baum/AMIE/amie.ini"
#    print(" PRODUCTION MODE!!!!")
#elif cwd == dir_dev:
#    amie_config = "/home/baum/AMIEDEV/amiedev.ini"
#    print(" DEVELOPMENT MODE!!!!!")
#else:
#    print(" Not running in either ",dir_dev," or ",dir_pro,".  Exiting.")
#    sys.exit()

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
    except Exception as e:
        print(" ERROR: amiedb_call problem connecting to database in ",script_name)
        print(" ERROR: ",str(e))
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
        except Exception as e:
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
no_project_id = ['request_user_modify']

# ================= READ APPROVAL STATUS FROM approval TABLE =============================

try:
    sql = "SELECT * FROM approval WHERE approval_status = 'unapproved';"
    data = ()
    results = []
    amiedb_call(sql,data,script_name,results)
except Exception as e:
    msg1 = "Problem extracting unapproved entries from 'approval' table. Exiting."
    msg2 = "ERROR Message: " + str(e)
    logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
    print(msg1 + "\r\n" + msg2)
    send_email(msg1 + "\r\n" + msg2)
    sys.exit()

noapp = len(results)
if (noapp == 0):
    print(" There are no requests presently requiring approval. Exiting.")
    os.remove(lock_file_path)
    sys.exit()

#  Check for unprocessed packets in queue. 
#for row in results:
#    print(row)
#sys.exit()
####### NOTE ##########

#  Add in capability to change automatically generated user/login name.

#  Cycle through unapproved requests.
napp = 1
for row in results:
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("Packet #",napp)
    print(" ")
    (trans_rec_id, type_id, person_id, project_id, approval_status, ts_received, ts_approved, ts_reply, reply_status, alloc_proxy, cluster) = row

    try:
        pid_suffix = project_id.split(".")[2]
        if pid_suffix != '000':
            print("***** PACKET ISSUE: The 'project_id' ",project_id," suffix is ",pid_suffix," and NOT 000.")
            project_id = project_id.split(".")[0] + "." + project_id.split(".")[1] + ".000"
            print("***** Changing 'project_id' to ",project_id," and proceeding normally.")
            print(" ")
#            continue
    except Exception as e:
        print("***** This is a '",type_id,"' packet that does not contain a project ID.")
#        print("***** PACKET ISSUE: Can't check 'project_id' suffix for reason to be determined.  Proceeding.")
#        print(" ")
#        continue

# Ensure that all of 'person_id, 'project_id' and 'cluster' are available for extracting info from 'local_info'.
    try:
# The 'request_project_inactivate' and 'request_project_reactivate' packets do not contain info about the PI.
# The 'local_info' extraction is done using the 'project_id' and 'cluster'.
        if type_id in no_person_id:
            sql = "SELECT * FROM local_info WHERE project_id = %s AND cluster = %s"
            data = (project_id,cluster,)
# The 'request_user_modify' packets do not contain information about the project(s) of the user.
# The 'local_info' extract is done using the 'person_id' and 'cluster'.
        elif type_id in no_project_id:
            sql = "SELECT * FROM local_info WHERE person_id = %s"
            data = (person_id,)
# Extract local_info for person_id/project_id/cluster combination.
        else:
            sql = "SELECT * FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
            data = (person_id, project_id,cluster,)
        results2 = []
        amiedb_call(sql,data,script_name,results2)
#        (first_name,last_name,email,person_id2,project_id2,remote_site_login,grant_number,slurm_acct,service_units,start_date,end_date,proj_stat,acct_stat,uid,gid,cluster,access_id,pi,override) = results2[0]
#        print(" results2[0] = ",results2[0])
    except Exception as e:
        msg1 = "Problem extracting from 'local_info' for " + type_id + " packet with trans_rec_id = " + str(trans_rec_id) + ". Skipping packet."
        msg2 = "ERROR message: " + str(e)
        print(msg1 + "\r\n" + msg2)
        send_email(msg1 + "\r\n" + msg2)
        logging.info(script_name + ": " + msg1 + "\r\n" + msg2)
        continue

#
    try:
# no_person_id = ['request_project_inactivate','request_project_reactivate']
        if type_id in no_person_id:
            print("Unapproved '",type_id,"' request for project '",project_id,"' on resource '",cluster,"'.")
# no_project_id = ['request_user_modify']
        elif type_id in no_project_id:
            print("Unapproved '",type_id,"' request for user '",person_id,"'.")
# All packet types not in no_person_id or no_project_id.
        else:
# Extract 'first_name', 'last_name', 'email' and 'access_id' from 'local_info' using 'person_id', 'project_id' and 'cluster'.
            sql = "SELECT first_name,last_name,email,access_id FROM local_info WHERE person_id = %s and project_id = %s and cluster = %s"
            data = (person_id,project_id,cluster,)
            results = []
            amiedb_call(sql,data,script_name,results)
            (first_name,last_name,email,access_id) = results[0]
            print("Unapproved '",type_id,"' request for ",first_name," ",last_name,"",email,".")
            print("The local user ID is '",person_id,"' on project '",project_id,"' with ACCESS ID '",access_id,"' on resource '",cluster,"'.")
# Extract PI info for this user.
            sql = "SELECT first_name,last_name,email,person_id,access_id FROM local_info WHERE project_id = %s and pi = 'Y'"
            data = (project_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            (pi_first_name,pi_last_name,pi_email,pi_person_id,pi_access_id) = results[0]
            print("The PI is ",pi_first_name," ",pi_last_name," at ",pi_email," with user ID ",pi_person_id," and ACCESS ID ",pi_access_id,".")
    except:
        print("No 'local_info' entry for ",type_id," packet for person_id ",person_id," on project_id ",project_id," on cluster ",cluster,".")
        print("Skipping to next packet.")
        napp = napp + 1
        continue

#    print("Finished account info look-up in 'local_info'.  Exiting.")
#    sys.exit()
#    print(" results2 = ",results2)
#  Extract required information for person_id/project_id from local_info.
    try:
        (first_name,last_name,email,pers_id,proj_id,remote_site_login,grant_number,sa,service_units,start_date,end_date,ps,accts,uid,gid,cluster,access_id,pi,override) = results2[0]
    except Exception as e:
        print("Problem extracting information for ",person_id," from 'local_info'.  Skipping.")
        print("ERROR: ",str(e))

    if len(results2) == 0:
        print("There is no entry for ",person_id," in 'local_info'.")
        napp = napp + 1
        continue
#    sys/.exit()
#    print(' ')
#    print(" ******************************************************************************************")
#    print(" Request no. ",napp," of type '",type_id,"' received on ",ts_received," is unapproved.")
#    print(" ******************************************************************************************")
#----------------------------------------------------------------------------
    if type_id in no_person_id:
#----------------------------------------------------------------------------
#        print(" Request is for activation/deactivation of project ID = ",project_id,".")
        msg1 = "A " + type_id + " request for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + "."
        print(msg1)
        if override != 'N':
            msg2 = "WARNING: This project has the override value '",override,"'."
            print(msg1 + "\r\n" + msg2)
        else:
            print(msg1)
#----------------------------------------------------------------------------
    elif type_id in no_project_id:
#----------------------------------------------------------------------------
        msg1 = "A user modification request for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + "."
        print(msg1)
#  Extract 'trans_rec_id' from 'approval' table for use with 'data_changes_tbl' table.
        appstat = 'unapproved'
        sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND approval_status = %s AND type_id = %s"
        data = (person_id,appstat,type_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        trans_rec_id = int(results[0][0])
        print("MODIFY: trans_rec_id = ",trans_rec_id)
#        sys.exit()
#  Look for entries matching 'trans_rec_id' in 'data_changes_tbl'.
        sql = "SELECT tag,old_value,new_value FROM data_changes_tbl WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        no_of_mods = len(results)
#  Print number of changes and then print the changes line by line.
        print(" There are ",no_of_mods," modifications requested for user ",person_id,".")
        for line in results:
            (tag,old_value,new_value) = line
            print(" Value for ",tag," is changing from ",old_value," to ",new_value)
        if override != 'N':
            print("WARNING: This project has the override value '",override,"'.")
#----------------------------------------------------------------------------
    elif type_id == 'request_project_create':
#----------------------------------------------------------------------------
        (allocation_type, new_proxy) = str(alloc_proxy).split('-')
        msg1 = "A " + type_id + " request for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + "."
#        print(" A '" + type_id + "' Request for '",first_name,"' '",last_name,"' at '",email,"' with user ID = '",pid,"', project ID = '",project_id,"' and ACCESS ID = '",access_id,"'.")
#  Query 'rpc_packets' table to find service units, start date, end date and proxy status of unapproved table entry.
#        print(" trans_rec_id = ",trans_rec_id)
        sql = "SELECT service_units,start_date,end_date,proxy FROM rpc_packets WHERE trans_rec_id = %s"
        data = (trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
#        print(" results = ",results)
        new_service_units = results[0][0]
        new_start_date = results[0][1]
        new_end_date = results[0][2]
        proxy = results[0][3]
        if proxy == 1:
            if allocation_type == 'new':
                print("This '",type_id,"' packet is of allocation type '",allocation_type,"'.")
            else:
                print("This '",type_id,"' packet is of allocation type '",allocation_type,"' that is a proxy for a new account request.")
        else:
            print(" This '",type_id,"' is of allocation type '",allocation_type,"'.")
        print(" Request is for '",new_service_units,"' SU starting on '",new_start_date,"' and ending on '",new_end_date,"'.")
#        print(" Present values are '",service_units,"' SU starting on '",start_date,"' and ending on '",end_date,"'.") 
#        print(" allocation_type = ",allocation_type," proxy = ",proxy)
#        sys.exit()
        new_proxies = ['renewal','supplement','advance','transfer']
        if allocation_type == 'new' or allocation_type in new_proxies and proxy == 1:
            print("Approval will initiate transaction completion to confirm local project creation.")
        elif allocation_type == 'extension':
            print(" This will extend the end date of an allocation for an existing project.")
            print(" The present end date is ",end_date," and the requested new end date is ",new_end_date,".")
            if ps == 'inactive':
                print(" The project is 'inactive', so this is also a proxy for 'request_project_reactivate'.")
        elif allocation_type == 'transfer' or allocation_type == 'supplement' or allocation_type == 'advance' and new_proxy == 0:
######################### FIX THIS ####################################
#  We need to keep obtain or keep track of the remaining old allocation amount somewhere.
            print(" Approval will add the new allocation amount '",str(new_service_units),"' to the remaining old allocation amount.")
        elif allocation_type == 'renewal' and new_proxy == 0:
            print(" Approval will replace the present allocation amount with the new amount on the new start day.")
            print(" This will not occur until the new start day arrives if it is not yet here.")
        if override != 'N':
            print("WARNING: This project has the override value '",override,"'.")
#----------------------------------------------------------------------------
    elif type_id == 'request_account_create':
#----------------------------------------------------------------------------
#  Get the status of the PI account.
        sql = "SELECT proj_stat FROM local_info WHERE project_id = %s AND cluster = %s AND pi = 'Y'"
        data = (project_id,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        pi_proj_stat = results[0][0]
#  Find out if this is a proxy request to reactivate an account, i.e. it is if 'acct_stat' = 'inactive' rather than 'pending'.
        sql = "SELECT proj_stat,acct_stat FROM local_info WHERE person_id = %s AND project_id = %s AND cluster = %s"
        data = (person_id,project_id,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        proj_stat = results[0][0]
        acct_stat = results[0][1]
#        print("person_id = ",person_id," project_id = ",project_id," cluster = ",cluster)
#        print("proj_stat = ",proj_stat," acct_stat = ",acct_stat)
#        print("pi_proj_stat = ",pi_proj_stat)
#  Only activate a user account if the project is active.
        if pi_proj_stat == 'active':
### Case: user account inactive, i.e. a proxy for account reactivation
            if acct_stat == 'inactive':
                msg1 = "Approval will reactivate an existing account. This is a proxy for a 'request_account_reactivate' request."
#                msg1 = "Approval will reactivate an an existing account for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + ".  This is a proxy for 'request_account_reactivate'."
### Case: user account activation pending
#            elif acct_stat == 'pending' or acct_stat == 'active':
            elif acct_stat == 'pending':
# Find PI information for this account request.
                sql = "SELECT first_name,last_name,email,access_id FROM local_info WHERE project_id = %s AND cluster = %s AND pi = 'Y'"
                data = (project_id,cluster,)
                results = []
                amiedb_call(sql,data,script_name,results)
                (pi_first_name,pi_last_name,pi_email,pi_access_id) = results[0]
                print("PI is ",pi_first_name," ",pi_last_name," ",pi_email," with ACCESS ID ",pi_access_id,".")
                msg1 = "User's account status is 'pending' and project status is 'active'.  Approval will activate the account."
#               msg1 = "Approval will create an account for " + first_name + " " + last_name + " " + email + " with local ID " + person_id + " on project " + project_id + " with ACCESS ID " + access_id + " on resource " + cluster + "."
### Case: user account already active
            else:
                msg1 = "User's account status is already 'active'."
                continue
        else:
#  Do not activate a user account for an inactive project.
            if pi_proj_stat == 'inactive':
                msg1 = "Project " + project_id + " is inactive.  Accounts on inactive projects will not be activated.  Skipping approval."
            elif pi_proj_stat == 'pending':
                msg1 = "The status of project " + project_id + " is still 'pending'.  The project must be activated before user accounts can be activated."
            print(msg1)
            continue
        print(msg1)
        if override != 'N':
            print("  ")
            print("###################################################")
            print("WARNING: This project has the override value '",override,"'.")
            print("###################################################")

#override_warning_txt = """
####################################################
#WARNING: This project has the override value {override}
###################################################
#"""

#  Automatically change approval status to 'approved' in 'approval' table.
#        ts_approved = tstamp()
#        sql = "UPDATE approval SET approval_status = 'approved', ts_approved = %s WHERE trans_rec_id = %s"
#        ts_approved = tstamp()
#        data = (ts_approved, trans_rec_id,)
#        results = []
#        amiedb_call(sql,data,script_name,results)
#        print("The 'request_account_create' packet for ",person_id," on project_id = ",project_id," on cluster = ",cluster," has been automatically approved.")
##### EXIT
#        continue
        
#        sys.exit()
#  END OF IF-ELSIF CONSTRUCT

    print(" ")
    print(" -------------------------------------------------------------------------------------")
    app_status = input("Enter 'a' to approve (default), 's' to skip, 'r' to reject, or 'q' to exit: ")
#    app_status = input(" Enter 'approved' to approve this request or 'no' to deny it (default 'approved') or 'q' to exit: ")
    print(" -------------------------------------------------------------------------------------")
    if len(app_status) == 0:
        app_status = 'a'
    if app_status == 'a':
        print(" You have successfully entered 'a' or [Enter] to approve this request.")
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
            print("User name changed to ",new_name)
            print(" ")
        elif name_change == 'n':
            print("No name change.")
            print(" ")
        else:
            print("Something.")
#  Change approval status to 'approved' in 'approval' table.
        sql = "UPDATE approval SET approval_status = 'approved', ts_approved = %s WHERE trans_rec_id = %s"
        ts_approved = tstamp()
        data = (ts_approved, trans_rec_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
    elif app_status == 's':
        print("You have entered 's' to leave this unapproved request in the ACCESS queue.")
#        continue
    elif app_status == 'r':
        sure = input("You have entered 'r' to reject the request.  Are you sure you want to do this? Enter y (default) or n: ")
        if len(sure) == 0:
            sure = 'y'
        ys = ['y', 'Y', 'yes', 'Yes', 'YES']
        if sure in ys:
#            msg = input("You have confirmed rejection to change 'acct_stat' to 'reject' on all projects. Enter return to continue.")
#  Change approval status to 'reject' in 'approval' table.
            try:
                print(trans_rec_id,ts_approved)
                sql = "UPDATE approval SET approval_status = 'reject', ts_approved = %s WHERE trans_rec_id = %s"
                data = (ts_approved,trans_rec_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                print("Successfully updated approval table for rejection.")
                os.remove(lock_file_path)
                sys.exit()
            except Exception as e:
                print("Failed to update approval table for rejection.")
        else:
            msg = input("You have rejected rejection (or mistyped something) and this packet will remain in the ACCESS queue. Enter return to continue.")
        print("Exiting from rejection test.")
#        sys.exit()
#        continue
    elif (app_status == 'q'):
        print("Exiting script.")
        os.remove(lock_file_path)
        sys.exit()
    else:
        print(" You have entered the string '",app_status,"' which is not 'a', 's', 'r' or 'q'.  Exiting.")
        os.remove(lock_file_path)
        sys.exit()


    napp = napp + 1

#  Delete lock file.
#print("Removing lock file.")
os.remove(lock_file_path)
#print("Removed lock file.")



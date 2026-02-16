#!/usr/bin/python3

#  affiliation.py - Appends rows to usage_compute_fix table from usage_compute_***** tables.
#                    This requires tables with the 'id' renumbered via change_id_nos.py.

import sys
import os
import json
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

select_range = 0
diagnostics = 0

#  Command-line argument processing
change_last_index = 0
diag = 0
help_list = ['-h','--h','-H','--H','-help','--help','-Help','--Help']
#  Check for the presence of a command-line argument.
if len(sys.argv) > 1:
#  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(" Program: ",script_name," - Appends rows to usage_compute_fix table from usage_compute_***** tables.")
        print(" Arguments: ",help_list," - this message")
        print("             None - run the program")
        print("             c N - process only a maximum of N pending usage entries")
        print("             d - print diagnostics and exit")
        print("             1 - run the demo program with additional diagnostic output")
        print("             s - start at the ordinal number that follows 's'") 
        print(" ")
        sys.exit()
    else:
#  Check for the only presently valid command-line argument "1".
        if sys.argv[1] == '1':
#            print(" Command-line argument is ",sys.argv[1])
            diag = int(sys.argv[1])
#            print(" Printing diagnostics.")
        elif sys.argv[1] == 's':
            try:
                if len(sys.argv) == 4:
                    nend = sys.argv[3]
                nstart = sys.argv[2]
                select_range = 1
#                print("nstart = ",int(nstart)," nend = ",int(nend))
            except:
                print(" The 's' argument requires following starting and ending ordinal numbers.  No numbers supplied. Exiting.")
                sys.exit()
        elif sys.argv[1] == 'c':
            if len(sys.argv) != 3:
                print("The 'c' argument must be followed by an integer N.  Exiting.  Try again.")
                sys.exit()
            else:
                try:
                    no_of_entries_to_process = int(sys.argv[2])
                    change_last_index = 1
                except Exception as e:
                    print("The value ",sys.argv[2]," given for N is not an integer.  Exiting.  Try again.")
                    sys.exit()
#            sys.exit()
        elif sys.argv[1] == 'd':
            diagnostics = 1
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

cluster = 'launch'
test = 0

# PI - 425
# NON - 3237

#  Obtain login info for AMIE PostgreSQL db.
config_site = RawConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  NsfStatusCode

#  Dictionary linking academic status codes (NsfStatusCode) and academic status.

status = {'CN' : 'Center Non-Researcher Staff',
        'CR' : 'Center Researcher Staff',
        'F' :  'Faculty',
        'G' : 'Government User',
        'GS' :  'Graduate Student',
        'HS' : 'High School Student',
        'HT' : 'High School Teacher',
        'I' : 'Industrial User',
        'N' : 'Unaffiliated User',
        'NP' : 'Non-Profit User',
        'O' : ' Other User',
        'PD' : 'Postdoctorate',
        'UG' : 'Undergraduate Student',
        'UK' : 'Unknown',
        'UN' : 'University Non-Research Staff',
        'UR' : 'University Research Staff (non-postdoc)',
        'CA' : 'Community Account'}

# Find list of unique access IDs.

#cohort = 'pi';
#cohort = 'nonpi';

sql = "SELECT project_id,person_id FROM local_info WHERE cluster = 'launch' ORDER BY project_id"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
project_id_list = []
person_id_list = []
for projid in results:
#        print("projid = ",projid)
    project_id_list.append(projid[0])
#    person_id_list.append(projid[1])
#    print(project_id_list[0],person_id_list[0])
project_id_list = list(set(project_id_list))
project_id_list.sort()
print("Number of ACES users: ",len(project_id_list))
print("list = ",project_id_list)
#sys.exit()

print("project_id_list = ",project_id_list[0:30])

userinfo_api_key = "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"
userinfo_site_name = 'faster-mgt2.hprc.tamu.edu'
u_headers = {'XA-Agent' : 'userinfo', 'XA-Resource' : userinfo_site_name, 'XA-API-Key' : userinfo_api_key}
u_url_base = "https://allocations-api.access-ci.org/acdb/userinfo/v2/"

resource_name = 'launch'
resource_id = 'launch.tamu.access-ci.org'

no_archive = 0
count = 0

#project_id_list = ['p.bio240138.000']

found = 0
not_found = 0

project = 0
#  Iterate over projects.
for project_id in project_id_list:
    project = project + 1
    print("project_id = ",project_id)
#  Find all persons in the project.
    persons_in_project = []
    sql = "SELECT person_id FROM local_info WHERE project_id = %s and cluster = 'launch'"
    data = (project_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    for person in results:
        persons_in_project.append(person[0])
        persons = len(persons_in_project)
    print("persons_in_project = ",persons_in_project)

#  Iterate over persons in project.
    for person_id in persons_in_project:
#  Extract a plethora of info from 'proj_stat'.
        sql = "SELECT first_name,last_name,email,access_id,uid,gid,pi,proj_stat,acct_stat,override FROM local_info WHERE project_id = %s AND person_id = %s and cluster = 'launch'"
        data = (project_id,person_id,)
        results = []
        try:
            amiedb_call(sql,data,script_name,results)
            (first_name,last_name,email,access_id,uid,gid,pi,proj_stat,acct_stat,override) = results[0]
            print(first_name,last_name,email,access_id,uid,gid,pi)
        except Exception as e:
            print("PROBLEM accessing 'local_info' for ",person_id," on project ",project_id,".")
            print("ERROR: ",str(e))
            continue
#  Extract latest start and end dates from 'rpc_packets'.
        sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s AND cluster = 'launch' order by trans_rec_id limit 1"
        data = (project_id,)
        results = []
        try:
            amiedb_call(sql,data,script_name,results)
            (start_date,end_date) = results[0]
            print(start_date,end_date)
        except Exception as e:
            print("PROBLEM accessing 'rpc_packets' for ",person_id," on project ",project_id,".")
            print("ERROR: ",str(e))
            continue

#  Find a 'trans_rec_id' from 'approval' table using 'person_id'.
#  Use the last of all returned values to obtain most recent academic status.
        try:
            if pi == 'Y':
                sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND project_id = %s AND type_id = 'request_project_create' order by trans_rec_id desc limit 1"
            else:
                sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND project_id = %s AND type_id = 'request_account_create' order by trans_rec_id desc limit 1"
            data = (person_id,project_id,)
            results = []
            amiedb_call(sql,data,script_name,results)
            trans_rec_id = results[0][0]
        except Exception as e:
            print("PROBLEM: No 'trans_rec_id' for 'request_*_create' for ",person_id," on project ",project_id,".")
            print("ERROR: ",str(e))
            print("Trying to find user on another project.")
#  If there is no entry in the 'approval' table for a person, see if there's another 'approval' entry for this person for another project.
#            try:
#               if pi == 'Y':
#                    sql = "SELECT trans_rec_id,project_id FROM approval WHERE person_id = %s AND project_id != %s AND type_id = 'request_project_create' order by trans_rec_id desc limit 1"
#                else:
#                    sql = "SELECT trans_rec_id,project_id FROM approval WHERE person_id = %s AND project_id != %s AND type_id = 'request_account_create' order by trans_rec_id desc limit 1"
#                data = (person_id,project_id,)
#                results = []
#                amiedb_call(sql,data,script_name,results)
#                trans_rec_id = results[0][0]
#                print("User ",person_id," also has account with project ",project_id," and 'trans_rec_id' = ",trans_rec_id,)

        if len(results) != 0:
#  Use 'trans_rec_id' to grab NsfStatusCode value from archive directory.
            if pi == 'Y':
                arch_file = "/scratch/user/baum/AMIE/rpc/rpc" + str(trans_rec_id) + ".json"
            else:
                arch_file = "/scratch/user/baum/AMIE/rac/rac" + str(trans_rec_id) + ".json"
#            print("arch_file = ",arch_file)
#  Import JSON file as Python dictionary.
            try:
                with open(arch_file) as f:
                    arch_json = json.load(f)
#                print("arch_json = ",arch_json)
#                print("JSON file found.")
                found = found + 1
            except:
                print("File: ",arch_json," not found.")
                not_found = not_found + 1
                print("JSON file not found.  Skipping.")
                continue
#  Grab status code from dictionary and use 'status' dictionary to find corresponding status.
            try:
                academic_status = status[arch_json['body']['NsfStatusCode']]
#                print("academic_status = ",academic_status)
            except:
                academic_status = 'NA'
            try:
                degree_field = arch_json['body']['AcademicDegree'][0]['Field']
                if degree_field == "":
                    degree_field = 'NA'
            except:
                degree_field = 'NA'
            try:
                degree = arch_json['body']['AcademicDegree'][0]['Degree']
            except:
                degree = 'NA'
            try:
                country = arch_json['body']['UserCountry']
            except:
                country = 'NA'
            if pi == 'Y':
                try:
                    organization = arch_json['body']['PiOrganization']
                except:
                    organization = 'NA'
#                print("organization = ",organization," academic_status = ",academic_status," degree_field = ",degree_field)
            else:
                try:
                    organization = arch_json['body']['UserOrganization']
                except:
                    organization = 'NA'
        else:
#  If there is no entry in the 'approval' table for a person, see if there's another 'approval' entry for this person for another project.
            try:
                if pi == 'Y':
                    sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND project_id != %s AND type_id = 'request_project_create' order by trans_rec_id desc limit 1"
                else:
                    sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND project_id != %s AND type_id = 'request_account_create' order by trans_rec_id desc limit 1"
                data = (person_id,project_id,)
                results = []
                amiedb_call(sql,data,script_name,results)
                trans_rec_id = results[0][0]
                print("ALTERNATE ROUTE FOUND with 'trans_rec_id' = ",trans_rec_id)
                sys.exit()
            except Exception as e:
                print("No 'trans_rec_id' found in 'approval' table for ",person_id," Adding placeholders.")
            #        print("Archive file for ",person_id," not available.")
                no_archive = no_archive + 1
                degree_field = 'NA'
                degree = 'NA'
                country = 'NA'
                organization = 'NA'
                academic_status = 'NA'
#            continue

#        sys.exit()
#  Insert required info into 'user_info'.
#  Check if 'access_id' already exists in 'user_info'.
        print("person_id = ",person_id," project_id = ",project_id)
        if pi == 'Y':
            sql = "SELECT * FROM user_info_launch WHERE person_id = %s AND project_id = %s AND pi = 'Y'"
        else:
            sql = "SELECT * FROM user_info_launch WHERE person_id = %s AND project_id = %s AND pi = 'N'"
        data = (person_id,project_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
#        print("results=",results)
#        sys.exit()
#  Insert required info into 'user_info'.
        if len(results) == 0:
#            print("ADDING")
#            print(first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country,start_date,end_date,proj_stat,acct_stat,override)
#  sql = "SELECT first_name,last_name,email,access_id,uid,gid,pi,proj_stat,acct_stat,override FROM local_info WHERE project_id = %s AND person_id = %s and cluster = 'launch'"
            sql = "INSERT INTO user_info_launch (first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country,start_date,end_date,proj_stat,acct_stat,override) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            data = (first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country,start_date,end_date,proj_stat,acct_stat,override,)
            results = []
##### SKIP DB INSERTION
            amiedb_call(sql,data,script_name,results)
        else:
            print("The 'person_id' ",person_id," for project ",project_id," already exists in 'user_info'.")

        count = count + 1
#        print("project = ",project," persons = ",persons)
#        if project > 4:
#            sys.exit()

    print("no_archive = ",no_archive)
#    sys.exit()

print("found = ",found)
print("not_found = ",not_found)

##### TEMP
#    sys.exit()

sys.exit()


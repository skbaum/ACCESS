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

cluster = 'faster'
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
cohort = 'nonpi';

if cohort != 'pi':
# The unique non-PI entries can be selected by project_id/
    sql = "SELECT project_id,person_id FROM local_info WHERE pi = 'N' AND cluster = 'aces' ORDER BY project_id"
    data = ()
    results = []
    amiedb_call(sql,data,script_name,results)
    project_id_list = []
    person_id_list = []
    for projid in results:
#        print("projid = ",projid)
        project_id_list.append(projid[0])
        person_id_list.append(projid[1])
#    print(project_id_list[0],person_id_list[0])
    print("Number of non-PI aces users: ",len(project_id_list))
#    sys.exit()
else:
#  The unique PI entries can be selected by project_id/pi combinations.
    sql = "SELECT project_id FROM local_info WHERE pi = 'Y' AND cluster = 'aces' ORDER BY project_id"
#    sql = "SELECT access_id FROM local_info WHERE pi = 'Y' AND cluster = 'aces' ORDER BY access_id"
#    sql = "SELECT DISTINCT(access_id) FROM local_info WHERE pi = 'Y' AND cluster = 'aces' ORDER BY access_id"
    data = ()
    results = []
    amiedb_call(sql,data,script_name,results)
    project_id_list = []
    for projid in results:
        project_id_list.append(projid)
    print(project_id_list)
    print("Number of PI aces users: ",len(project_id_list))
#    access_id_list = []
#    for accid in results:
#        access_id_list.append(accid)
#    print(access_id_list)
#    print("Number of PI aces users: ",len(access_id_list))



#sys.exit()


userinfo_api_key = "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"
userinfo_site_name = 'faster-mgt2.hprc.tamu.edu'
u_headers = {'XA-Agent' : 'userinfo', 'XA-Resource' : userinfo_site_name, 'XA-API-Key' : userinfo_api_key}
u_url_base = "https://allocations-api.access-ci.org/acdb/userinfo/v2/"

resource_name = 'aces'
if resource_name == 'aces':
    resource_id = 'aces.tamu.access.org'
elif resource_name == 'faster':
    resource_id = 'faster.tamu.xsede.org'

no_archive = 0
count = 0

#project_id_list = ['p.nairr250246.000']

for project_id in project_id_list:
#for access_id in access_id_list:
#for access_id in access_id_list:

#    if count > 5:
#        sys.exit()

    print("project_id = ",project_id)
    sql = "SELECT person_id FROM local_info WHERE project_id = %s AND pi = 'Y'"
    data = (project_id,)
#    sql = "SELECT person_id FROM local_info WHERE access_id = %s"
#    data = (access_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    print("results = ",results)
    person_id = results[0][0]

#    u_url_full = u_url_base + "people/by_username/" + str(resource_id) + "/" + str(person_id) + "'"
#    response = requests.get('https://allocations-api.access-ci.org/acdb/userinfo/v2/people/by_username/faster.tamu.xsede.org/u.ac39485', headers=u_headers)
#    allit = response.json()
#    organization = allit['result']['organization']
#    email = allit['result']['email']

#  Find out if 'access_id' is a PI, user or both
#    sql = "SELECT pi FROM local_info WHERE access_id = %s"
#    data = (access_id,)
#    results = []
#    amiedb_call(sql,data,script_name,results)
#    plist = []
#    for r in results:
#        plist.append(r[0])

#    if 'Y' in plist:
#        if 'N' in plist:
#            print("B")
#        print("Y")
#    else:
#        print("N")
#    print(plist)
#    sys.exit()

#  Extract required info from 'local_info'.
    try:
        if cohort == 'pi':
            sql = "SELECT first_name,last_name,email,person_id,access_id,uid,gid,pi FROM local_info WHERE project_id = %s AND pi = 'Y'"
        else:
            sql = "SELECT first_name,last_name,email,person_id,access_id,uid,gid,pi FROM local_info WHERE project_id = %s AND pi = 'N'"
        data = (project_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        print(results)
        (first_name,last_name,email,person_id,access_id,uid,gid,pi) = results[0]
    except:
        print("Could not find project ID ",project_id," in 'local_info'.  Skipping.")
        continue

#  Find a 'trans_rec_id' from 'approval' table using 'person_id'.
#  Use the last of all returned values to obtain most recent academic status.
    try:
        if cohort == 'pi':
            sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND type_id = 'request_project_create'"
        else:
            sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND type_id = 'request_account_create'"
#        sql = "SELECT trans_rec_id FROM approval WHERE person_id = %s AND type_id = 'request_project_create'"
        data = (person_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        print("No.of results = ",len(results))
        if len(results) != 0:
            print(results)
            last_result = len(results) - 1
            trans_rec_id = results[last_result][0]
#  Use 'trans_rec_id' to grab NsfStatusCode value from archive directory.
            if cohort == 'pi':
                arch_file = "/scratch/user/baum/AMIE/rpc/rpc" + str(trans_rec_id) + ".json"
            else:
                arch_file = "/scratch/user/baum/AMIE/rac/rac" + str(trans_rec_id) + ".json"
#                arch_file = "rac/rac" + str(trans_rec_id) + ".json"
            print("arch_file = ",arch_file)
#  Import JSON file as Python dictionary.
            with open(arch_file) as f:
                arch_json = json.load(f)
            print("arch_json = ",arch_json)
#  Grab status code from dictionary and use 'status' dictionary to find corresponding status.
            try:
                academic_status = status[arch_json['body']['NsfStatusCode']]
                print("academic_status = ",academic_status)
            except:
                academic_status = 'NA'
            try:
                print("DEGREE")
                degree_field = arch_json['body']['AcademicDegree'][0]['Field']
                degree = arch_json['body']['AcademicDegree'][0]['Degree']
                print("degree_field = ",degree_field)
            except:
                degree_field = 'NA'
                degree = 'NA'
            try:
                country = arch_json['body']['UserCountry']
            except:
                country = 'NA'
            if cohort == 'pi':
                try:
                    organization = arch_json['body']['PiOrganization']
                except:
                    organization = 'NA'
            else:
                try:
                    organization = arch_json['body']['UserOrganization']
                except:
                    organization = 'NA'
        else:
            print("No 'trans_rec_id' found in 'approval' table for ",person_id,".")
            continue
    except:
        print("Archive file for ",person_id," not available.")
        no_archive = no_archive + 1
        degree_field = 'NA'
        degree = 'NA'
        country = 'NA'
        organization = 'NA'
        academic_status = 'NA'
#    print("degree_field=",degree_field," degree = ",degree)
#    print("country=",country," org = ",organization)
#    sys.exit()


#  Insert required info into 'user_info'.
#  Check if 'access_id' already exists in 'user_info'.
    if cohort == 'pi':
        sql = "SELECT * FROM user_info_aces_pi WHERE project_id = %s AND pi = 'Y'"
    else:
        sql = "SELECT * FROM user_info_aces_nonpi WHERE project_id = %s AND pi = 'Y'"
    data = (project_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    print("results=",results)
#    sys.exit()
#  Insert required info into 'user_info'.
    if len(results) == 0:
        print("ADDING")
        if cohort == 'pi':
            sql = "INSERT INTO user_info_aces_pi (first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        else:
            sql = "INSERT INTO user_info_aces_nonpi (first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        data = (first_name,last_name,email,person_id,project_id,uid,gid,access_id,pi,academic_status,degree_field,degree,organization,country,)
        results = []
##### SKIP DB INSERTION
        amiedb_call(sql,data,script_name,results)
    else:
        print("The 'access_id' ",access_id," already exists in 'user_info'.")

    count = count + 1

    print("no_archive = ",no_archive)
#    sys.exit()

##### TEMP
#    sys.exit()

sys.exit()


#!/usr/bin/python3

#  add_to_usage.py - Appends rows to usage_compute_fix table from usage_compute_***** tables.
#                    This requires tables with the 'id' renumbered via change_id_nos.py.

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

select_range = 0
diagnostics = 0

diag = 0
select_range = 0
diagnostics = 0

if len(sys.argv) == 1:
    print("You must enter a slurm account number on the command line, e.g. 145494770067.")
    sys.exit()
else:
    slurm_acct_no = str(sys.argv[1])
    print("slurm = ",slurm_acct_no)
#sys.exit()

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

#  Obtain login info for AMIE PostgreSQL db.
config_site = RawConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Get the max and min 'id' values from the usage_compute_******** table containing the missing entries for a given slurm account number.
sql = "SELECT MIN(id) min, MAX(id) max FROM usage_compute_" + slurm_acct_no
data = ()
results = []
amiedb_call(sql,data,script_name,results)
(start_tmp_id,end_tmp_id) = results[0]
print("start_id = ",start_tmp_id," end_id = ",end_tmp_id)

#  Find the maximum current 'id' value in 'usage_compute_fix' to check against the starting 'id' in 'usage_compute_*'.
sql = "SELECT id FROM usage_compute_fix ORDER BY id DESC LIMIT 1"
data = ()
results = []
amiedb_call(sql,data,script_name,results)
next_id_in_usage_compute = results[0][0] + 1
if next_id_in_usage_compute == start_tmp_id:
    print("The next available ID in usage_compute_fix ",next_id_in_usage_compute," and the first id in usage_compute_* ",start_tmp_id," match.  We continue.")
else:
    print("The next available ID in usage_compute_fix ",next_id_in_usage_compute," and the first id in usage_compute_* ",start_tmp_id," do not match.  Exiting.")
    sys.exit()

#  Change the 'id' values into those required for appending to 'usage_compute_fix'.
for idrow in range(start_tmp_id,end_tmp_id+1):

#  Check to be sure the 'id' isn't already in use in 'usage_compute_fix'.
    sql = "SELECT * FROM usage_compute_fix WHERE id = %s"
    data = (idrow,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) != 0:
        print("This 'id' is already in use in 'usage_compute_fix'.  Exiting.")
        sys.exit()

    print("Now inserting new entry with 'id' ",idrow," into 'usage_compute_fix'.")
    sql = "INSERT INTO usage_compute_fix SELECT * FROM usage_compute_" + slurm_acct_no + " WHERE id = %s"
    data = (idrow,)
    results = []
    amiedb_call(sql,data,script_name,results)

#    sys.exit()

sys.exit()

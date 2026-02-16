#!/usr/bin/python3

#  Creates a daily comparison of ACES usage from myproject, usage_compute_aces, and ACCESS.
#
#  Requires myproj.py results from ACES login node:
#
#    scp baum@login.aces.hprc.tamu.edu:/home/baum/aces_myproject_charges.txt ./
#
#  Produces:
#
#    usage_comparison_aces-2025-07-21.txt

from configparser import ConfigParser
from datetime import datetime, date

import sys
import logging
import os
import psycopg2
import mysql.connector
import requests
import subprocess
from configparser import RawConfigParser

# script_name = sys.argv[0][2:]
script_name = "aces_totals.py"

ACCESS_HOME = "/home/baum/AMIE"
log_file = ACCESS_HOME + "/access_usage.log"
#  Configuration and log file processing.
if not os.path.isfile(log_file):
    print(" Log file ", log_file, " doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(
    filename=log_file, format="%(asctime)s - %(message)s", level=logging.INFO
)

#  Configuration and log file processing.
log_file = "/home/baum/AMIEDEV/amiedev.log"
if not os.path.isfile(log_file):
    print(" Log file ", log_file, " doesn't exist.  Creating it.")
    os.system("touch " + log_file)
#  Establish log file location.
logging.basicConfig(
    filename=log_file, format="%(asctime)s - %(message)s", level=logging.INFO
)

dir_dev = "/home/baum/AMIEDEV"
dir_pro = "/home/baum/AMIE"
cwd = os.getcwd()

if cwd == dir_pro:
    amie_config = "/home/baum/AMIE/amie.ini"
    print(" PRODUCTION MODE!!!!")
elif cwd == dir_dev:
    amie_config = "/home/baum/AMIEDEV/amiedev.ini"
    print(" DEVELOPMENT MODE!!!!!")
else:
    print(" Not running in either ", dir_dev, " or ", dir_pro, ".  Exiting.")
    sys.exit()


#  Obtain login info for SLURM MySQL db.
slurm_config = "/home/baum/AMIE/slurm.ini"
config_site = RawConfigParser()
config_site.read(slurm_config)
m_site_con = config_site["SLURM"]
m_pw = m_site_con["pw"]
m_dbase = m_site_con["dbase"]
m_dbuser = m_site_con["dbuser"]
m_localhost = m_site_con["local_address"]

#  Command-line argument processing
diag = 0
help_list = ["-h", "--h", "-H", "--H", "-help", "--help", "-Help", "--Help"]
#  Check for the presence of a command-line argument.
if len(sys.argv) > 1:
    #  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(
            " Program: ",
            script_name,
            " - compares ACES usage totals from three sources",
        )
        print(" Arguments: ", help_list, " - this message")
        print("             None - Program runs.")
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


def datetime_string_to_unix_timestamp(datetime_str, format_str):
    """
    Converts a date-time string to a Unix timestamp.
    Args:
        datetime_str (str): The date-time string to convert.
        format_str (str): The format of the date-time string (e.g., "%Y-%m-%d %H:%M:%S").
    Returns:
        float: The Unix timestamp (seconds since the epoch) as a float.
               Returns None if the string cannot be parsed.
    """
    try:
        # Parse the date-time string into a datetime object
        dt_object = datetime.strptime(datetime_str, format_str)

        # Convert the datetime object to a Unix timestamp
        unix_timestamp = dt_object.timestamp()
        return unix_timestamp
    except ValueError as e:
        print(f"Error parsing date-time string: {e}")
        return None


def amiedb_call(sql, data, script_name, results):
    if diag > 0:
        print(" AMIEDB_CALL: sql = ", sql)
        print(" AMIEDB_CALL: data = ", data)
    try:
        conn = psycopg2.connect(
            host="localhost", database=dbase, user=dbuser, password=pw
        )
    except Exception as e:
        print(" AMIEDB_CALL: Error connecting to database in ", script_name)
        print(" AMIEDB_CALL: Problematic SQL: ", sql)
    cursor = conn.cursor()
    try:
        cursor.execute(sql, data)
        conn.commit()
        try:
            matches = cursor.fetchall()
            len_matches = len(matches)
            if diag > 0:
                print(
                    " AMIEDB_CALL: No. of matches: ",
                    len_matches,
                    " match(es) = ",
                    matches,
                )
            for match in matches:
                results.append(match)
        except Exception as e:
            print("ERROR: ",str(e))
            results = []
            if diag > 0:
                print(" AMIEDB_CALL: No cursor.fetchall() results to process.")
    except (Exception, psycopg2.DatabaseError):
        logging.error(
            "*************** DB transaction error ***************: ", exc_info=True
        )
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
    print("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def tstamp():
    """Function for timestamp."""
    tst = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return tst

def tdate():
    """Function for date stamp."""
    tdt = str(datetime.now().strftime("%Y-%m-%d"))
    return tdt



test = 0

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site["TAMU"]
site_name = site_con["site_name"]
api_key = site_con["api_key"]
pw = site_con["pw"]
dbase = site_con["dbase"]
dbuser = site_con["dbuser"]

# Constant values for Python request module.
userinfo_site_name = "faster-mgt2.hprc.tamu.edu"
xdusage_api_key = "IVyD2kiOUX8ciuku4oyZuor7Hg4hTyPASXR7W+zLqn49CYYlBFtDmMZ5kGwSFHP5"
userinfo_api_key = "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"
site_name = "faster.tamu.xsede.org"
x_headers = {
    "XA-Agent": "xdusage",
    "XA-Resource": site_name,
    "XA-API-Key": xdusage_api_key,
}
x_url_base = "https://allocations-api.access-ci.org/acdb/xdusage/v2/"
u_headers = {
    "XA-Agent": "userinfo",
    "XA-Resource": userinfo_site_name,
    "XA-API-Key": userinfo_api_key,
}

# ACES
resource_id = "3362"
end_date = datetime.now().date()

start_date = "2025-01-13"

sql = "SELECT DISTINCT(project_id) FROM usage_compute_aces"
data = ()
results = []
amiedb_call(sql, data, script_name, results)
aces_project_ids = []
# print(results)
id_track = 0
for proj_id in results:
    pro = str(proj_id[0])
    #    print("pro = ",pro)
    #  Filter out NAIRR project.
    #    if not pro.startswith("p.nairr"):
    aces_project_ids.append(results[id_track][0])
    id_track = id_track + 1
aces_project_ids.sort()
no_aces_project_ids = len(aces_project_ids)
print(no_aces_project_ids)

print("aces_project_ids = ", aces_project_ids)
print(" ")

sql = "SELECT DISTINCT(project_id) FROM local_info WHERE cluster = 'aces' AND proj_stat = 'active'"
data = ()
results = []
amiedb_call(sql, data, script_name, results)
aces_project_ids_all = []
id_track = 0
for proj_id in results:
    pro = str(proj_id[0])
    #    print("pro = ",pro)
    #  Filter out NAIRR project.
    #    if not pro.startswith("p.nairr"):
    aces_project_ids_all.append(results[id_track][0])
    id_track = id_track + 1
aces_project_ids_all.sort()
no_aces_project_ids_all = len(aces_project_ids_all)

print(no_aces_project_ids_all)
print("aces_project_ids_all = ", aces_project_ids_all)


#  Read the file containing the myproject results from ACES.
with open("/home/baum/AMIE/aces_myproject_charges.txt") as infile:
    lines = infile.readlines()
line_no = 0

mdate = tstamp().split(" ")[0]
tmpfile = "/home/baum/AMIE/usage_comp_aces.txt"
sortfile = "/home/baum/AMIE/usage_comparison_aces-" + mdate + ".txt"
fout = open(tmpfile, "a")
checkfile = "/home/baum/AMIE/aces_access_checks.txt"
gout = open(checkfile, "w")

#fout.write("\n")
#fout.write("POOP")
#fout.write(" \n")
#fout.write("ACES Usage Comparison for DATE\n")
#fout.write(" \n")
#fout.write("PostgreSQL usage - total usage from AMIE database\n")
#fout.write("MySQL usage - total usage from SLURM database")
#fout.write("ACCESS usage - total usage from ACCESS via xdusage")
#fout.write("myproj usage - total usage from myproject command")
#fout.write(" ")
#fout.write("person_id    project_id      access_id        slurm_acct    PostgreSQL    MySQL     ACCESS    myproj      pct     latest       UNIX")
#fout.write("                                                              usage       usage      usage     usage      diff     start       start")
#fout.write(" ")

# TESTING FOR ONE PROJECT
#  For testing with just one project.
# aces_project_ids = ['p.phy230021.000'
#aces_project_ids = ['p.bio210175.000','p.phy230021.000']

#aces_project_ids = aces_project_ids[0:30]
#aces_project_ids = ['p.agr250019.000']

sql = "SELECT DISTINCT(project_id) FROM local_info WHERE project_id like 'p.nair%%'"
data = ()
results = []
amiedb_call(sql, data, script_name, results)
#print("results = ",results)
#  Convert list of tuples to flat list.
nairr_project_ids = [item[0] for item in results]
print("NAIRR project IDs:")
print(nairr_project_ids)
#sys.exit()

nairr = 0
if nairr == 1:
    aces_project_ids = nairr_project_ids
#print("aces: ",aces_project_ids)

slurm_acct_list = []
mysql_tot_list = []
postgresql_tot_list = []
postgresql_count_list = []
myproject_tot_list = []
access_tot_list = []
for project_id in aces_project_ids:
    sql = "SELECT slurm_acct FROM local_info WHERE project_id = %s AND pi = 'Y' and cluster = 'aces'"
    data = (project_id,)
    results = []
    amiedb_call(sql, data, script_name, results)
    slurm_acct = results[0][0]
    print("*********************************")
    print(project_id," ",slurm_acct)

    #  Extract most recent start and end dates for this project from 'rpc_packets'.
    sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s ORDER BY trans_rec_id desc limit 1"
    data = (project_id,)
    results = []
    amiedb_call(sql, data, script_name, results)
    (start_date, end_date) = results[0]
    latest_start = start_date.date()
    latest_end = end_date.date()
    print("Project ID: ", project_id)
    print("The most recent start date: ", latest_start, ".")
    print("The most recent end date: ", latest_end, ".")
    print("The present date is ", datetime.now().date(), ".")
    utc_latest_start = datetime_string_to_unix_timestamp(str(latest_start), "%Y-%m-%d")
    print("slurm_acct = ", slurm_acct, " utc_latest_start = ", utc_latest_start)

    conn = mysql.connector.connect(
        user=m_dbuser, password=m_pw, host=m_localhost, database=m_dbase
    )
    cursor = conn.cursor()

    #  Find all 'job_db_inx' values in 'aces_job_table' for the given (Slurm) 'account' number.
    sql = "SELECT job_db_inx FROM aces_job_table WHERE account = %s"
    data = (slurm_acct,)
    cursor.execute(sql, data)
    ids_for_slurm_no = []
    for values in cursor:
        ids_for_slurm_no.append(str(values[0]))
    print("No. of IDs for Slurm no.: ", len(ids_for_slurm_no))

    #    sys.exit()

#  Find total SU from inner join of MySQL tables.
    sql = "SELECT sum(service_units) FROM aces_job_table INNER JOIN aces_job_processing ON aces_job_table.job_db_inx = aces_job_processing.job_table_record_id where aces_job_processing.state = '1' and account = %s and service_units > 0 AND time_submit > %s"
    data = (slurm_acct, str(utc_latest_start))
    cursor.execute(sql, data)
    matches = cursor.fetchall()
    mysql_charges = matches[0][0]
    if mysql_charges is None:
        mysql_charges = 0.00
    print("SU usage from MySQL: ", mysql_charges)
    #    sys.exit()

#person_id    project_id      access_id        slurm_acct    PostgreSQL    MySQL     ACCESS    myproj      pct     latest       UNIX
#                                                              usage       usage      usage     usage      diff     start       start
#u.zl330554 p.chm250083.000   zli65            158305549167   81307.16   30612.28   26104.35  265620.29    917.53 2025-08-19   1755579600.0

#  Find total SU and count from PostgreSQL table.
    project_id = str(project_id)
    sql = "SELECT sum(charge),count(*) FROM usage_compute_aces WHERE project_id = %s AND submit_time > %s"
    utc_latest_start = datetime_string_to_unix_timestamp(str(latest_start), "%Y-%m-%d")
    data = (
        project_id,
        utc_latest_start,
    )
    results = []
    amiedb_call(sql, data, script_name, results)
    print("results = ",results)
    postgresql_charges = results[0][0]
    postgresql_count = results[0][1]
    if postgresql_charges is None:
        postgresql_charges = 0.0
    print("SU usage from PostgreSQL: ", postgresql_charges," count: ",postgresql_count,".")

    sql = "SELECT person_id,slurm_acct,access_id FROM local_info WHERE project_id = %s AND pi = 'Y' AND cluster = 'aces'"
    data = (project_id,)
    results = []
    amiedb_call(sql, data, script_name, results)
    (person_id, slurm_acct, access_id) = results[0]

#  Extract myproject charges from file.
    found = 0
    for line in lines:
        (myproj_user, myproj_slurm, myproj_charges, pending, no_users) = line.split()
        if str(myproj_user) == str(person_id) and str(myproj_slurm) == str(slurm_acct):
            myproject_charges = myproj_charges
            found = 1
            break
    if found == 1:
        print("Found it: ",myproject_charges,".")

    else:
        print("No match for project ",project_id," .  Skipping.")
        continue

    #  Extract grant_number from project_id.
    grant_number = project_id.split(".")[1].upper()

    #  Find the 'project_id' using the grant number and cluster name.
    x_url_full = (
        x_url_base
        + "projects/?projects="
        + grant_number
        + "&resource_id="
        + resource_id
    )
    response = requests.get(x_url_full, headers=x_headers)
    access_project_id = response.json()["result"][0]["project_id"]
#    print("Before xdusage: start_date = ", start_date, " end_date = ", end_date)
    #    sys.exit()
    #  Find the total usage.
    x_url_full = x_url_full = (
        x_url_base
        + "usage/by_dates/"
        + str(access_project_id)
        + "/"
        + str(resource_id)
        + "/"
        + str(start_date)
        + "/"
        + str(end_date)
    )
    response = requests.get(x_url_full, headers=x_headers)
    xdusage_charges = response.json()["result"][0]["su_used"]
    print("Raw SU usage from xdusage: ", xdusage_charges)
    try:
        xdusage_charges = float(xdusage_charges)
    except Exception as e:
        xdusage_charges = 0.0
    print("Float SU usage from xdusage: ",xdusage_charges)


#    try:
#        myproject_charges = float(myproject_charges)
#    except Exception as e:
#        myproject_charges = 0.0
#    if myproject_charges > 0.0001:
    go = 'go'
    if go == 'go':
        my_real = float(myproject_charges)
        if xdusage_charges > 0.0:
            pctdiff = abs((my_real - xdusage_charges) / float(xdusage_charges) * 100.0)
        else:
            pctdiff = float(999.9)
        print("postgresql_charges = ", postgresql_charges)
        print("mysql_charges = ", float(mysql_charges))
        print("xdusage_charges = ", float(xdusage_charges))
        print("myproject_charges = ", float(myproject_charges))
        print(
            "latest_start = ",
            latest_start,
            " pctdiff = ",
            pctdiff,
            " myproject_charges = ",
            myproject_charges,
        )
            #            print("STR = ",str(latest_start))
            #            print(person_id.ljust(10),"  ",project_id.ljust(16),"  ",access_id.ljust(16)," ",str(slurm_acct).ljust(10)," ",str("%.2f" % postgresql_charges).rjust(10)," ",str("%.2f" % float(xdusage)).rjust(10)," ",str("%.2f" % float(myproject_charges)).rjust(10)," ",str("%.2f" % pctdiff).rjust(9))

#person_id    project_id      access_id        slurm_acct    PostgreSQL    MySQL     ACCESS    myproj      pct     latest       UNIX
#                                                              usage       usage      usage     usage      diff     start       start
#u.zl330554 p.chm250083.000   zli65            158305549167   81307.16   30612.28   26104.35  265620.29    917.53 2025-08-19   1755579600.0

    slurm_acct_list.append(slurm_acct)
    mysql_tot_list.append(mysql_charges)
    postgresql_tot_list.append(postgresql_charges)
    postgresql_count_list.append(postgresql_count)
    myproject_tot_list.append(myproject_charges)
    access_tot_list.append(xdusage_charges)
    print("slurm = ",slurm_acct," access = ",xdusage_charges," psql = ",postgresql_charges)

#    print("uls = ",utc_latest_start," ",str(utc_latest_start))
    utc_latest_start_str = str(utc_latest_start)
    print(person_id,project_id,access_id,slurm_acct,mysql_charges,postgresql_charges,xdusage_charges,myproject_charges,pctdiff,latest_start,utc_latest_start)
    results_str = (
                person_id.ljust(10)
                + " "
                + project_id.ljust(16)
                + "  "
                + access_id.ljust(16)
                + " "
                + str(slurm_acct).ljust(10)
                + " "
                + str("%.2f" % float(mysql_charges)).rjust(10)
                + " "
                + str("%.2f" % postgresql_charges).rjust(10)
                + " "
                + str("%.2f" % float(xdusage_charges)).rjust(10)
                + " "
                + str("%.2f" % float(myproject_charges)).rjust(10)
                + " "
                + str("%.2f" % pctdiff).rjust(9)
                + " "
                + str(latest_start).ljust(12)
                + " "
#                + str(utc_latest_start).ljust(10) + "\n"
                + utc_latest_start_str + "\n"
            )
    fout.write(results_str)
    print("**************************")
#        else:
#            continue

fout.close()
print("closed fout")

#    mysql_tot_list.append(mysql_charges)
#    postgresql_tot_list.append(postgresql_charges)
#    myproject_tot_list.append(myproject_charges)
#    access_tot_list.append(xdusage_charges)

print("access_tot_list = ",access_tot_list)
print("postgresql_tot_list = ",postgresql_tot_list)
print("postgresql_count_list = ",postgresql_count_list)

corrections = 1
if corrections == 1:
    usage_psql_list = []
    count_psql_list = []
    slurm_check_list = []
#    print("corrections = ",corrections)
    count = 0
    print("slurm_acct_list = ",slurm_acct_list)
#  Loop through slurm account numbers to check for ACCESS usage totals less than 'usage_compute_aces' totals.
    for slurm_no in slurm_acct_list:
#  Find where ACCESS total is less than 'usage_compute_aces' total.
        acc = float(access_tot_list[count])
        pos = float(postgresql_tot_list[count])
        print("slurm = ",slurm_no," acc = ",acc," pos = ",pos)
        try:
            if acc/(0.99*pos) < 1.0:
#                print("ratio = ",acc/(0.99*pos))
                slurm_check_list.append(slurm_no)
#  Get usage amount corresponding to slurm number.
#            print("count = ",count)
#            print("slurm_acct_list[count] = ",slurm_acct_list[count])
#            print("postgresql_tot_list = ",postgresql_tot_list)
#            print("postgresql_count_list = ",postgresql_count_list)
#            print("count = ",count," postgresql_tot_list[count] = ",postgresql_tot_list[count]," postgresql_count_list[count] = ",postgresql_count_list[count])
                usage_psql_list.append(postgresql_tot_list[count])
                count_psql_list.append(postgresql_count_list[count])
            count =  count + 1
        except Exception as e:
            continue
#            print("Divide by zero for Slurm acct. ",slurm_no)
#        print("count = ",count)

 #   print("usage_psql_list = ",usage_psql_list)
 #   print("count_psql_list = ",count_psql_list)

#slurm_acct_list = []
#mysql_tot_list = []
#postgresql_tot_list = []
#myproject_tot_list = []
#access_tot_list = []

    print("ADDING PSQL NUMBERS.")
#  Find counts and usage values for each ACCESS account where usage is less than that in 'usage_compute_aces'.
    counts_access_list = []
    usage_access_list = []
#    print("slurm_check_list = ",slurm_check_list)
    for slurm in slurm_check_list:
#        print("slurm = ",slurm)
        sl = int(slurm)
        command = "/home/baum/AMIE/xdusage_auto.py usage aces {}".format(sl)
        result = subprocess.check_output(command, shell=True)
        str_result = result.decode("utf-8").strip()
#        print("access usage = ",str_result)
        usage_access_list.append(str_result)
        command = "/home/baum/AMIE/xdusage_auto.py counts aces {}".format(sl)
        result = subprocess.check_output(command, shell=True)
        str_result = result.decode("utf-8").strip()
#        print("access count = ",str_result)
        counts_access_list.append(str_result)
    print("slurm = ",slurm_check_list)
    print("access usage = ",usage_access_list)
    print("access counts = ",counts_access_list)
    print("psql usage = ",usage_psql_list)
    print("psql count = ",count_psql_list)
    sys.exit()

    n = 0
    print("slurm = ",str(slurm_check_list[n]))
    for items in slurm_check_list:
        cpl = int(count_psql_list[n])
        cal = int(counts_access_list[n])
        try:
            cdiff = cpl - cal
            cdiff = int(count_psql_list[n]) - int(counts_access_list[n])
        except Exception as e:
            cdiff = 0
        print("cdiff = ",cdiff," cpl = ",cpl," cal =",cal)
        try:
            upl = str(round(float(usage_psql_list[n]),2))
        except Exception as e:
            upl = 0.0
        try:
            ual = str(round(float(usage_access_list[n]),2))
        except Exception as e:
            ual = 0.0
        print("upl = ",upl," ual = ",ual)
        results_str = (str(slurm_check_list[n]).ljust(10)
        + " "
        + ual.rjust(13)
        + " "
        + str(cal).rjust(10)
        + " "
        + upl.rjust(13)
        + " "
        + str(cpl).rjust(9)
        + " "
        + str(cdiff).rjust(10) + "\n")

        
        gout.write(results_str)
        n = n+1

#    sys.exit()

#  Sort the output file (tmpfile) by the percent difference column to create the sorted file (sortfile).
# my_cmd = ['/usr/bin/sort','-rn','-k17',tmpfile]
my_cmd = ["/usr/bin/sort", "-rn", "-k9", tmpfile]
with open(sortfile, "w") as outfile:
    subprocess.run(my_cmd, stdout=outfile)
# os.system("rm /home/baum/AMIE/usage_comp_aces.txt")

os.system("rm usage_comp_aces.txt")
tdat = str(tdate())
print(tdat)
os.system("cp usage_comparison_aces.template aces.template")
os.system("""sed -i 's/DATE/""" + tdate() + """/g' aces.template""")
#sys.exit()

os.system(
    "cat aces.template usage_comparison_aces-" + tdate() + ".txt > temp_file.txt && mv temp_file.txt usage_comparison_aces-" + tdate() + ".txt"
)


sys.exit()


# usage_comparison_aces.template
# usage_comparison_aces-2025-11-14.txt

# To prepend the content of one file (e.g., file1.txt) to another file (e.g., file2.txt):
# cat file1.txt file2.txt > temp_file.txt && mv temp_file.txt file2.txt


# FILTER OUT THE ENTRIES IN usage_compute_aces THAT ARE IN usage_compute_aces_backup.
#sql = "select id from usage_compute_aces where status = 'N' order by id"
#data = ()
#results = []
#amiedb_call(sql, data, script_name, results)
#ids_to_process = []
#id_track = 0
#for id_no in results:
#    ids_to_process.append(results[id_track][0])
#    id_track = id_track + 1

#s#ql = "SELECT serial_no FROM usage_compute_aces_backup"
#data = ()
#results = []
#amiedb_call(sql, data, script_name, results)
# print(results)

#for serial_no in results:
#    #    print(serial_no[0])
#    change_no = serial_no[0]
#    sql = "UPDATE usage_compute_aces SET status = 'Y' WHERE serial_no = %s"
#    data = (change_no,)
#    results = []
#    amiedb_call(sql, data, script_name, results)
#    sys.exit()


# sys.exit()

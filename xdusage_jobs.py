#!/usr/bin/python3

#  Script for extracting a list of the job IDs ACCESS has for a given project as well as a list of those IDs in 'usage_compute_aces'.
#  The lists are compared and a list of those in 'usage_compute_aces' that aren't known to ACCESS is create, after which the status
#  of the missing IDs is changed to 'N' in 'usage_compute_aces' and the reports sent to ACCESS again.

from configparser import ConfigParser
from datetime import datetime,date
from collections import namedtuple

from pprint import pprint
import inspect
import sys
import logging
import os
import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime,date,timedelta
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
import mysql.connector

diag = 0

#script_name = sys.argv[0][2:]
script_name = 'xdusage_jobs.py'

cluster_names = ['aces','faster','launch']
info_types = ['counts','usage','jobs']

amie_config = "/adm/AMS/ACCESS/bin/amie.ini"

skip_int = 0
#  Command-line argument processing
help_list = ['-h','--h','-H','--H','-help','--help','-Help','--Help']
#  Check for the presence of a command-line argument.
#print("Number of args: ",len(sys.argv))
if len(sys.argv) > 1:
#  Check for a help message request.
    if sys.argv[1] in help_list:
        print(" ")
        print(" Program: ",script_name," - cross-check reports sent from TAMU against those reported as received by ACCESS")
        print(" Arguments: ",help_list," - this message")
        print("             specify a Slurm account number")
        print(" ")
        sys.exit()
    else:
        if len(sys.argv) < 6:
            print("Specify a slurm account number ('155485226413'), cluster name ('aces'), type ('usage') and start and end dates ('2025-06-01 2025-06-10') Try again.")
            sys.exit()
        else:
            print(sys.argv)
            slurm_no = str(sys.argv[1])
            cluster = str(sys.argv[2])
            resource_name = 'cluster'
            itype = str(sys.argv[3])
            if cluster not in cluster_names:
                print("The cluster names are ",cluster_names,". Try again.")
                sys.exit()
            if itype not in info_types:
                print("The info types are ",info_types,". Try again.")
                sys.exit()
            print("Slurm no. = ",slurm_no," cluster = ",cluster)
            skip_int = 1
#            sys.exit()
else:
    print("Proceed in interactive mode.")
#    print("Specify a slurm account number (e.g. '155485226413'), cluster (e.g. 'aces') and type (e.g. 'usage'). Try again.")
#    sys.exit()

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

#  Home page for ACCESS-CI ACDB APIs.
#  https://allocations-api.access-ci.org/acdb/
#  The API is presently designed to support three agents: spacct, userinfo and xdusage. The API acts on behalf of these
#  agents to make queries in the ACCESS Allocations Database and return the results of those queries to the agent. 
#
#  API - https://allocations-api.access-ci.org/acdb/apidoc/1.0.html

#  resource_id - faster.tamu.xsede.org - 3295

userinfo_hash = "$2a$10$FpI3zE6sR1VWVwKh8ijAXegk5CYXvRiV/oE.IXmAcfXRbCgXJAbli"
userinfo_api_key = "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"
userinfo_site_name = 'faster-mgt2.hprc.tamu.edu'
userinfo_curl_header_v = 'curl -v -X GET -H "XA-Agent: userinfo" -H "XA-Resource: ' + userinfo_site_name + '" -H "XA-API-Key: ' + userinfo_api_key + '"'
userinfo_curl_header = 'curl -X GET -H "XA-Agent: userinfo" -H "XA-Resource: ' + userinfo_site_name + '" -H "XA-API-Key: ' + userinfo_api_key + '"'
userinfo_curl_string = userinfo_curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/v2/"

xdusage_api_key = "IVyD2kiOUX8ciuku4oyZuor7Hg4hTyPASXR7W+zLqn49CYYlBFtDmMZ5kGwSFHP5"
site_name = 'faster.tamu.xsede.org'
curl_header_v = 'curl -v -X GET -H "XA-Agent: xdusage" -H "XA-Resource: ' + site_name + '" -H "XA-API-Key: ' + xdusage_api_key + '"'
curl_header = 'curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: ' + site_name + '" -H "XA-API-Key: ' + xdusage_api_key + '"'
curl_string = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/accounts/"
#  curl -v -X GET [headers] -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/accounts/22867/3295' | python3 -m json.tool > fred

if skip_int == 1:
    sql = "SELECT project_id,grant_number FROM local_info WHERE slurm_acct = %s"
    data = (slurm_no,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("Slurm account number ",slurm_no," is not a valid ",cluster," number. Try again.")
        sys.exit()
    (tamu_project_id,grant_number) = results[0]

if  skip_int == 0:
    print(" ")
    print("This script enables the use of the xdusage agent of the ACCESS allocations API to obtain various")
    print("type of information about projects and accounts.")
    print(" ")
    print("The following list describes what kind of information you get with each type of request.")
    print(" ")
    print("             counts - find the number of jobs reported for a project between given dates")
    print("             usage - find the total usage of a project between given dates")
    print("             jobs - find details of each job on a project between given dates (this can be huge)")
    print(" ")

# Constant values for Python request module.
x_headers = {'XA-Agent' : 'xdusage', 'XA-Resource' : site_name, 'XA-API-Key' : xdusage_api_key}
x_url_base = "https://allocations-api.access-ci.org/acdb/xdusage/v2/"
u_headers = {'XA-Agent' : 'userinfo', 'XA-Resource' : userinfo_site_name, 'XA-API-Key' : userinfo_api_key}

cluster_names = ['faster','aces','launch']
curl_string_base = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/"
userinfo_curl_string_base = userinfo_curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/v2/"
userinfo_curl_string_auth = userinfo_curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/"

print_str = " | python3 -m json.tool"

#sys.exit()

print("Starting choices.")

if skip_int == 0:
#  Choose an info type.
    itype = input("Enter an info type (counts,usage,jobs): ")
    if itype not in info_types:
        print("Selection not one of [counts,usage,jobs]. Try again.")
        sys.exit()
    print(' ')
    slurm_no = input("Enter a 'slurm_acct' number: ")
#  Choose a 'slurm_acct' number.
    sql = "SELECT project_id,grant_number FROM local_info WHERE slurm_acct = %s"
    data = (slurm_no,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("The 'slurm_acct' number ",slurm_no," does not presently exist in the TAMU ACCESS project database. Exiting.")
        sys.exit()
    tamu_project_id = str(results[0][0])
    grant_number = results[0][1]
    print(" ")
    resource_name = input("Enter a resource name (faster,aces,launch): ")
    print(" ")
#  Choose a cluster.
    resource_map  = {'faster': '3295', 'aces': '3362', 'launch': '3659'}
    if resource_name not in resource_map.keys():
        print("The resource ",resource_name," is not available.  Exiting.")
        sys.exit()
    else:
        resource_id = resource_map[resource_name]
        print("resource_id = ",resource_id)
#  Find if grant number has a project on the given resource, and if the project is inactive.
    sql = "SELECT proj_stat FROM local_info WHERE slurm_acct = %s AND cluster = %s"
    data = (slurm_no,resource_name,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("The grant ",grant_number," does not have a project on ",resource_name,". Exiting.")
        sys.exit()
    else:
        if results[0][0] == 'inactive':
            print("The project for grant ",grant_number," on ",resource_name," is present inactive.")

#  TIME/DATE STUFF
#  Get start and end dates from the packet that started the project.
    sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s ORDER BY trans_rec_id limit 1"
    data = (tamu_project_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    (first_start_date,first_end_date) = results[0]
    first_start = first_start_date.date()
#  Get most recent start and end dates.
    sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s ORDER BY trans_rec_id desc limit 1"
    data = (tamu_project_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    (start_date,end_date) = results[0]
    latest_start = start_date.date()
    latest_end = end_date.date()
    print("The project started on ",first_start,".")
    print("The present time extent of the project is ",latest_start," to ",latest_end,".")
    print("The present date is ",datetime.now().date(),".")
# The project started on  2024-05-28 .
# The present time extent of the project is  2024-05-28  to  2026-05-01 .
# The present date is  2025-10-09 .
#    sys.exit()
#  Set or override default dates.
    start = first_start
    end = datetime.now().date()
    start_date = input("Enter a start date of the form: '2024-06-23' (default is project start date): ")
    if start_date == "":
        start_date = str(start)
    end_date = input("Enter an end date of the form: '2024-07-28: (default is today): ")
    if end_date == "":
        end_date = str(end)
    print("Using: start_date = ",start_date," end_date = ",end_date)
#    sys.exit()
#  Work around lack of time in date/time component of ACCESS Xdusage API
#
#  Equivalent:  Xdusage         2025-02-02 - 3035-02-02
#               PostgreSQL      2025-02-02 - 2025-02-03

    (y,m,d) = end_date.split('-')
    print(y,m,d)
    end_date_object = datetime(int(y),int(m),int(d))
    previous_day = end_date_object - timedelta(days=1)
    xdusage_end_date = str(previous_day).split(' ')[0]
    sql_end_date = end_date
    print("sql_end_date = ",sql_end_date," xdusage_end_date = ",xdusage_end_date)
#    sys.exit()

#  Find the 'access_project_id' using the grant number and cluster name.
    x_url_full = x_url_base + "projects/?projects=" + grant_number + "&resource_id=" + resource_id
    print("x_headers = ",x_headers)
    response = requests.get(x_url_full, headers=x_headers)
    access_project_id = response.json()['result'][0]['project_id']

    print("access_project_id = ",access_project_id)
    print("itype = ",itype)
    print("resource_id = ",resource_id)

#  Find the count of usage reports on a project using the 'access_project_id', 'resource_id', 'start_date' and 'end_date'.
if itype == 'counts':
    print("resource_id = ",resource_id)
    x_url_full = x_url_base + "counts/by_dates/" + str(access_project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(xdusage_end_date)
    print("x_url_full = ",x_url_full)
    response = requests.get(x_url_full, headers=x_headers)
    no_of_jobs = response.json()['result'][0]['n']
    print(" ")
    print("Number of jobs received by ACCESS ",start_date," to ",end_date," for grant ",grant_number," on ",resource_name,": ",no_of_jobs)
elif itype == 'usage':
#    print("ALL: ",access_project_id,resource_id,start_date,end_date)
    x_url_full = x_url_full = x_url_base + "usage/by_dates/" + str(access_project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(xdusage_end_date)
    print("x_url_full = ",x_url_full)
    response = requests.get(x_url_full, headers=x_headers)
    usage = response.json()['result'][0]['su_used']
    print(" ")
    print("Total ACCESS SU usage from ",start_date," to ",end_date," for grant ",grant_number," on ",resource_name,": ",usage)
else:
#  Find list of job report numbers received by ACCESS.
    x_url_full = x_url_full = x_url_base + "jobs/by_dates/" + str(access_project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(xdusage_end_date)
#    print("x_url_full = ",x_url_full)
    response = requests.get(x_url_full, headers=x_headers)
    job_ids = response.json()['result']
    nid = 0
    access_local_job_id = []
    for jids in job_ids:
        access_local_job_id.append(str(jids['local_job_id']))
        nid = nid + 1
    print("Number of usage reports received by ACCESS for grant ",grant_number,":",len(access_local_job_id))
# Example usage:
# date_time_string1 = "2023-10-27 10:30:00"
# format_string1 = "%Y-%m-%d %H:%M:%S"
# unix_time1 = datetime_string_to_unix_timestamp(date_time_string1, format_string1)
# if unix_time1 is not None:
#    print(f"The Unix timestamp for '{date_time_string1}' is: {unix_time1}")
#  Find list of job report numbers ostensibly sent by TAMU.
    print("resource_name = ",resource_name)
    if resource_name == 'aces':
        print("ACES")
        utc_start_date = datetime_string_to_unix_timestamp(start_date, "%Y-%m-%d") 
        utc_end_date = datetime_string_to_unix_timestamp(sql_end_date, "%Y-%m-%d")
        print("UTC: start = ",utc_start_date," end = ",utc_end_date," project_id = ",tamu_project_id)
#        sys.exit()
#        sql = "SELECT serial_no FROM usage_compute_aces WHERE project_id = %s AND status = 'Y'"
#        data = (str(tamu_project_id),)
        sql = "SELECT serial_no FROM usage_compute_aces WHERE project_id = %s AND status = 'Y' AND submit_time > %s AND submit_time < %s"
        data = (str(tamu_project_id),int(utc_start_date),int(utc_end_date),)
    elif resource_name == 'faster':
        sql = "SELECT serial_no FROM usage_compute_fix WHERE project_id = %s AND status = 'Y' AND submit_time > %s AND submit_time < %s"
        data = (str(tamu_project_id),start_date,sql_end_date,)
#        sql = "SELECT serial_no FROM usage_compute_fix WHERE project_id = %s AND status = 'Y'"
#        data = (str(tamu_project_id),)
    results = []
    amiedb_call(sql,data,script_name,results)
    nid = 0
    tamu_local_job_id = []
    for jids in results:
        tamu_local_job_id.append(str(jids[0]))
    print("Number of usage reports sent by TAMU for grant ",grant_number,":",len(tamu_local_job_id))
#    sys.exit()

#  Find jobs report numbers sent by TAMU but not received by ACCESS.
    unreceived_serial_nos = [val for val in tamu_local_job_id if val not in access_local_job_id]
    print("Number of missing reports: ",len(unreceived_serial_nos))

#  Find serial_no values in 'usage_compute_aces' corresponding to 'id' values.
    unreceived_ids = []
    for ser in unreceived_serial_nos:
#         print("ser = ",ser)
        if resource_name == 'aces':
            sql = "select id from usage_compute_aces where serial_no = %s"
        elif resource_name == 'faster':
            sql = "select id from usage_compute_fix where serial_no = %s"
        data = (ser,)
        results = []
        amiedb_call(sql,data,script_name,results)
#        print("results = ",results)
        unreceived_ids.append(results[0][0])
    print("unreceived IDs: ",sorted(unreceived_ids))

    update = input("Enter Y or N to update the missing ACCESS entries: ") 
    if update == 'N':
        sys.exit()
    elif update == 'Y':
        batchsize = input("How many of the missing reports do you want to process?: ")

        for ids in unreceived_ids:
            if resource_name == 'aces':
                sql = "UPDATE usage_compute_aces SET status = 'N' where id = %s"
            elif resource_name == 'faster':
                sql = "UPDATE usage_compute_fix SET status = 'N' where id = %s"
            data = (ids,)
            results = []
            amiedb_call(sql,data,script_name,results)

#  Find SU total for unreceived reports.
    su = 0.0
#        for nr in unreceived_serial_nos:
#            if resource_name == 'aces':
#                sql = "SELECT charge FROM usage_compute_aces WHERE serial_no = %s AND status = 'Y'"
#            elif resource_name == 'faster':
#                sql = "SELECT charge FROM usage_compute_fix WHERE serial_no = %s"
#            data = (nr,)
#            results = []
#            amiedb_call(sql,data,script_name,results)
#            su = su + float(results[0][0])
#            print("no.: ",nr," usage: ",results[0][0])
#        print("total ACCESS missing SUs: ",su)
#        (start_date,end_date) = results[0]

    sys.exit()

    sys.exit()



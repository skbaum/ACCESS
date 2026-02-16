#!/usr/bin/python3

#  Script for using the xdusage agent of the ACCESS allocations API.

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
from datetime import datetime,date
import psycopg2
from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors
import mysql.connector

diag = 0

#script_name = sys.argv[0][2:]
script_name = 'xdusage.py'

amie_config = "/adm/AMS/ACCESS/bin/amie.ini"

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

#  A line.
reqs = ['allocations','projects','last_name','access_id','person_id','resources','accounts','authtest','userinfo']

print(" ")
print("This script enables the use of the xdusage agent of the ACCESS allocations API to obtain various")
print("type of information about projects and accounts.")
print(" ")
print("The following list describes what kind of information you get with each type of request.")
print(" ")
#print("             allocations - obtain current and total allocation status of project_id on resource_id")
print("             projects - obtain information about a specific project")
print("             last_name - search for users with a given 'last_name'")
print("             access_id - find 'first_name', 'last_name' and 'person_id' using the 'access_id' (aka 'portal_username')")
print("             person_id - find 'first_name', 'last_name' and 'person_id' using the local 'person_id' (aka 'username')")
print("             resources - find resource names using a wild card string")
print("             accounts - find all accounts associated with a project on a cluster")
print("             counts - find the number of jobs reported for a project between given dates")
print("             usage - find the total usage of a project between given dates")
print("             jobs - find details of each job on a project between given dates (this can be huge)")
print("             authtest - tests if the authorization is valid")
print("             failed - get failed records")
print("             userinfo - returns person information given cluster/person_id or access_id")
print(" ")
req = input("Enter one of the request types listed above: ")
print(" ")
if req not in reqs:
    print("You entered '",req,"' which is not in the list of available requests.  Exiting.")

# Constant values for Python request module.
x_headers = {'XA-Agent' : 'xdusage', 'XA-Resource' : site_name, 'XA-API-Key' : xdusage_api_key}
x_url_base = "https://allocations-api.access-ci.org/acdb/xdusage/v2/"
u_headers = {'XA-Agent' : 'userinfo', 'XA-Resource' : userinfo_site_name, 'XA-API-Key' : userinfo_api_key}

cluster_names = ['faster','aces','launch']
curl_string_base = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/"
userinfo_curl_string_base = userinfo_curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/v2/"
userinfo_curl_string_auth = userinfo_curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/"

print_str = " | python3 -m json.tool"

print("Starting choices.")

#  Enter selected user and cluster.
if req == 'userinfo':
    print("Making a 'userinfo' API request.")
    person_id = input("Enter a 'person_id': ")
    print(" ")
    resource_name = input("Enter a resource name (faster,aces,launch): ")
    print(" ")
    print(" ")
    if resource_name == 'faster':
        resource_id = 'faster.tamu.xsede.org'
    elif resource_name == 'aces':
        resource_id = 'aces.tamu.access.org'

# Check if selected user has account on selected cluster.
    sql = "SELECT * FROM local_info WHERE person_id = %s AND cluster = %s"
    data = (person_id,resource_name,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("User ",person_id," doesn't have an account on ",resource_name,". Please try again.")
        sys.exit()

# curl -X GET -H "XA-Agent: xdusage" -H "XA-Resource: 'faster-mgt2.hprc.tamu.edu'" -H 'XA-API-Key: "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"' -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/v2/people/by_username/faster.tamu.access-ci.org/u.ar199956'

#  Find the accounts on a project using the 'project_id' and the 'resource_id'.
    userinfo_curl_header_v = 'curl -X GET -H "XA-Agent: userinfo" -H "XA-Resource: ' + userinfo_site_name + '" -H "XA-API-Key: ' + userinfo_api_key + '"'
    userinfo_curl_string_base = userinfo_curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/userinfo/v2/"
    userinfo_curl_string = userinfo_curl_string_base + "people/by_username/" + str(resource_id) + "/" + str(person_id) + "'" + print_str
    os.system(userinfo_curl_string)

    x_headers = {'XA-Agent' : 'xdusage', 'XA-Resource' : site_name, 'XA-API-Key' : xdusage_api_key}
    x_url_base = "https://allocations-api.access-ci.org/acdb/xdusage/v2/"


    userinfo_api_key = "82D3X+A7TF+zIj/JXfq2LNDUH+p0im5YCyTt4F4YXh6hEMfQvoFQ+LIqdDvWJrZY"
    userinfo_site_name = 'faster-mgt2.hprc.tamu.edu'
    u_headers = {'XA-Agent' : 'userinfo', 'XA-Resource' : userinfo_site_name, 'XA-API-Key' : userinfo_api_key}
    u_url_base = "https://allocations-api.access-ci.org/acdb/userinfo/v2/"

    u_url_full = u_url_base + "people/by_username/" + str(resource_id) + "/" + str(person_id) + "'"
    print(u_url_full)
    print('https://allocations-api.access-ci.org/acdb/userinfo/v2/people/by_username/faster.tamu.xsede.org/u.ac39485')
    response = requests.get('https://allocations-api.access-ci.org/acdb/userinfo/v2/people/by_username/faster.tamu.xsede.org/u.ac39485', headers=u_headers)
    allit = response.json()
    organization = allit['result']['organization']
    print("org = ",organization)
    status = allit['result']['status']
    print("status = ",status)
    email = response.json()['result']['email']
    print("email = ",email)

#    response = requests.get(x_url_full, headers=x_headers)
#    no_of_jobs = response.json()['result'][0]['n']
#  

elif req == 'counts' or req == 'usage' or req == 'jobs':
    print("Starting request for counts, usage or jobs.")
    print(" ")
    gn = input("Enter a 'grant_number': ")
    grant_number = gn.upper()
#  Check if grant number exists in 'local_info'.
    sql = "SELECT * FROM local_info WHERE grant_number = %s"
    data = (grant_number,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("The grant number ",grant_number," does not presently exist in the TAMU ACCESS project database. Exiting.")
        sys.exit()
    print(" ")
    proj_id = "p." + grant_number.lower() + ".000"
#    print("proj_id = ",proj_id)
    resource_name = input("Enter a resource name (faster,aces,launch): ")
    print(" ")
    resource_map  = {'faster': '3295', 'aces': '3362', 'launch': '3659'}
    if resource_name not in resource_map.keys():
        print("The resource ",resource_name," is not available.  Exiting.")
        sys.exit()
    else:
        resource_id = resource_map[resource_name]
        print("resource_id = ",resource_id)
#        sys.exit()
#  Find if grant number has a project on the given resource, and if the project is inactive.
    sql = "SELECT proj_stat FROM local_info WHERE grant_number = %s AND cluster = %s"
    data = (grant_number,resource_name,)
    results = []
    amiedb_call(sql,data,script_name,results)
    if len(results) == 0:
        print("The grant ",grant_number," does not have a project on ",resource_name,". Exiting.")
        sys.exit()
    else:
        if results[0][0] == 'inactive':
            print("The project for grant ",grant_number," on ",resource_name," is present inactive.")
#  Get start and end dates from the packet that started the project.
    sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s ORDER BY trans_rec_id limit 1"
    data = (proj_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    (first_start_date,first_end_date) = results[0]
    first_start = first_start_date.date()
#  Get most recent start and end dates.
    sql = "SELECT start_date,end_date FROM rpc_packets WHERE project_id = %s ORDER BY trans_rec_id desc limit 1"
    data = (proj_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    (start_date,end_date) = results[0]
    latest_start = start_date.date()
    latest_end = end_date.date()
    print("The project started on ",first_start,".")
    print("The present time extent of the project is ",latest_start," to ",latest_end,".")
    print("The present date is ",datetime.now().date(),".")
#  Set or override default dates.
    start = first_start
    end = datetime.now().date()
    start_date = input("Enter a start date of the form: '2024-06-23' (default is project start date): ")
    if start_date == "":
        start_date = start
    end_date = input("Enter an end date of the form: '2024-07-28: (default is today): ")
    if end_date == "":
        end_date = end
    print("start = ",start_date," end = ",end_date)

#  Find the 'project_id' using the grant number and cluster name.
#    x_url_base = "https://allocations-api.access-ci.org/acdb/xdusage/v2/"
    x_url_full = x_url_base + "projects/?projects=" + grant_number + "&resource_id=" + resource_id
#    print("x_url_full = ",x_url_full)
    print("x_headers = ",x_headers)
    response = requests.get(x_url_full, headers=x_headers)
    project_id = response.json()['result'][0]['project_id']

#  Find the count of usage reports on a project using the 'project_id', 'resource_id', 'start_date' and 'end_date'.
    if req == 'counts':
        x_url_full = x_url_full = x_url_base + "counts/by_dates/" + str(project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(end_date)
        response = requests.get(x_url_full, headers=x_headers)
        no_of_jobs = response.json()['result'][0]['n']
        print(" ")
        print("Number of jobs from ",start_date," to ",end_date," for grant ",grant_number," on ",resource_name,": ",no_of_jobs)
    elif req == 'usage':
        print("ALL: ",project_id,resource_id,start_date,end_date)
        x_url_full = x_url_full = x_url_base + "usage/by_dates/" + str(project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(end_date)
        response = requests.get(x_url_full, headers=x_headers)
        usage = response.json()['result'][0]['su_used']
        print(" ")
        print("Total SU usage from ",start_date," to ",end_date," for grant ",grant_number," on ",resource_name,": ",usage)
    else:
#  Find list of job report numbers received by ACCESS.
        x_url_full = x_url_full = x_url_base + "jobs/by_dates/" + str(project_id) + "/" + str(resource_id) + "/" + str(start_date) + "/" + str(end_date)
        print("x_url_full = ",x_url_full)
#        sys.exit()
        response = requests.get(x_url_full, headers=x_headers)
        job_ids = response.json()['result']
        nid = 0
        access_local_job_id = []
        for jids in job_ids:
            access_local_job_id.append(str(jids['local_job_id']))
            nid = nid + 1
        print("Number of usage reports received by ACCESS for grant ",grant_number,":",len(access_local_job_id))
#        print("Reports in ACCES: ",access_local_job_id)
#  Find list of job report numbers ostensibly sent by TAMU.
        if resource_name == 'aces':
            sql = "SELECT serial_no FROM usage_compute_aces WHERE project_id = %s AND status = 'Y'"
        elif resource_name == 'faster':
            sql = "SELECT serial_no FROM usage_compute_fix WHERE project_id = %s"
        data = (proj_id,)
        results = []
        amiedb_call(sql,data,script_name,results)
        nid = 0
        tamu_local_job_id = []
        for jids in results:
            tamu_local_job_id.append(str(jids[0]))
        print("Number of usage reports sent by TAMU for grant ",grant_number,":",len(tamu_local_job_id))
#        print("Reports in usage_compute_aces: ",tamu_local_job_id)
#  Find jobs report numbers sent by TAMU but not received by ACCESS.
        unreceived_serial_nos = [val for val in tamu_local_job_id if val not in access_local_job_id]
        print("Number of missing reports: ",len(unreceived_serial_nos))
#        print("non-reports:",unreceived_serial_nos)
#  Find serial_no values in 'usage_compute_aces' corresponding to 'id' values.
        unreceived_ids = []
        for ser in unreceived_serial_nos:
#            print("ser = ",ser)
            if resource_name == 'aces':
                sql = "select id from usage_compute_aces where serial_no = %s"
            elif resource_name == 'faster':
                sql = "select id from usage_compute_fix where serial_no = %s"
            data = (ser,)
            results = []
            amiedb_call(sql,data,script_name,results)
#            print("results = ",results)
            unreceived_ids.append(results[0][0])
        print("unreceived IDs: ",sorted(unreceived_ids))

        update = input("Enter Y or N to update the missing ACCESS entries: ") 
        if update == 'N':
            sys.exit()
        elif update == 'Y':
            batchsize = input("How many of the missing reports do you want to process?: ")

            unreceived_sub =  unreceived_ids[:int(batchsize)]
            for ids in unreceived_sub:
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
        sys.exit()
#        (start_date,end_date) = results[0]

        sys.exit()

    sys.exit()

elif req == 'accounts':
    print("Starting request for accounts.")
    print(" ")
    proj_info = input("Enter a 'grant_number', 'person_id', 'project_id' or 'access_id': ")
    print(" ")
    cluster = input("Enter a cluster name (faster,aces,launch): ")
    print(" ")
#  Find the 'resource_id' from the cluster name.
    if cluster not in cluster_names:
        print("The name '",cluster,"' is not a valid cluster name.  Exiting")
        sys.exit()
    if cluster == 'faster':
        resource_id = '3295'
    elif cluster == 'aces':
        resource_id = '3362'
    else:
        resource_id = 'unknown'
#  All user IDs start with 'u.'.
    if proj_info.startswith('u.'):
        print("This is a 'person_id'.")
        sql = "SELECT grant_number FROM local_info WHERE person_id = %s AND cluster = %s AND pi = 'Y'"
        data = (proj_info,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if len(results) == 0:
            print("The 'person_id' ",proj_info," does not match any PI with a project on ",cluster,". Exiting.")
            sys.exit()
        else:
            grant_number = results[0][0]
#        sys.exit()
#  All project IDs start with 'p.'.
    elif proj_info.startswith('p.'):
        print("This is a 'project_id'.")
        sql = "SELECT grant_number FROM local_info WHERE project_id = %s AND cluster = %s AND pi = 'Y'"
        data = (proj_info,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if len(results) == 0:
            print("The 'project_id' ",proj_info," does not match any PI project on ",cluster,". Exiting.")
            sys.exit()
        else:
            grant_number = results[0][0]
#        sys.exit()
#  Upper case letters are indicative of a grant number.
    elif any(ele.isupper() for ele in proj_info):
        print("This is a 'grant_number'.")
        grant_number = proj_info
        sql = "SELECT * FROM local_info WHERE grant_number = %s AND cluster = %s"
        data = (grant_number,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if len(results) == 0:
            print("There is no grant number ",grant_number," on cluster ",cluster,". Exiting.")
            sys.exit()
#        sys.exit()
#  If it's not one of the above, it's lower case letters and an ACCESS ID.
    else:
        print("This is an 'access_id'.")
        sql = "SELECT grant_number FROM local_info WHERE access_id = %s AND cluster = %s AND pi = 'Y'"
        data = (proj_info,cluster,)
        results = []
        amiedb_call(sql,data,script_name,results)
        if len(results) == 0:
            print("The 'access_id' ",proj_info," does not match any PI project on ",cluster,". Exiting.")
            sys.exit()
        else:
            grant_number = results[0][0]
#        sys.exit()
    print("grant_number = ",grant_number)
#    sys.exit()

#  Find the 'project_id' using the grant number and cluster name.
    proj_str = "?projects=" + grant_number + "&resource_id=" + resource_id + "'"
    curl_string = curl_string_base + "projects/" + proj_str
    alloc_filename = 'alloc.txt'
    curl_string = curl_string + " > " + alloc_filename + " 2> err.txt"
    os.system(curl_string)
#    print(curl_string)
    f = open(alloc_filename)
    data = json.load(f)
    results = data['result']
    project_id = results[0]['project_id']
#    print("project_id = ",project_id)
#  Find the accounts on a project using the 'project_id' and the 'resource_id'.
    curl_string = curl_string_base + "accounts/" + str(project_id) + "/" + str(resource_id) + "'" + print_str
    print(curl_string)
    os.system(curl_string)
    sys.exit()

elif req == 'projects':
    print("Starting request for projects.")
#    print("WOO HOO ")
    cluster = input("Enter a cluster name (faster,aces,launch): ")
    print(" ")
    if cluster not in cluster_names:
        print("The name '",cluster,"' is not a valid cluster name.  Exiting")
        sys.exit()
    if cluster == 'faster':
        resource_id = '3295'
    elif cluster == 'aces':
        resource_id = '3362'
    elif cluster == 'launch':
        resource_id = '3659'
    else:
        resource_id = 'unknown'
    grant_no = input("Enter a grant number: ")
    grant_no = grant_no.upper()
    sql = "SELECT * FROM local_info WHERE grant_number = %s AND cluster = %s"
    data = (grant_no,cluster,)
    results = []
    amiedb_call(sql,data,script_name,results)
    is_proj = len(results)
    if is_proj == 0:
        print("There is no grant number ",grant_no," on cluster ",cluster,". Exiting.")
        sys.exit()

    proj_str = "?projects=" + grant_no + "&resource_id=" + resource_id + "'"
    print_str = " | python3 -m json.tool"
    print("proj_str = ",proj_str)
    curl_string = curl_string_base + "projects/" + proj_str + print_str
    os.system(curl_string)
    sys.exit()
    curl_string = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/projects/" + proj_str
    alloc_filename = 'alloc.txt'
    curl_string = curl_string + " > " + alloc_filename + " 2> err.txt"
    print(curl_string)
    os.system(curl_string)
    f = open(alloc_filename)
    data = json.load(f)
    results = data['result']
    project_id = results[0]['project_id']
    print("project_id = ",project_id)
    sys.exit()

elif req == 'resources':
    print(" ")
    resource_wild = input("Enter a wild card string for a resource (e.g. 'fas' or 'tamu' for 'faster.tamu.xsede.org'): ")
    print(" ")
    resource = '%25' + resource_wild + '%25'
    curl_string = curl_string_base + "resources/" + resource + "'"
    curl_string = curl_string + print_str
    os.system(curl_string)
    sys.exit()

elif req == 'person_id':
    print(" ")
    person_id = input("Enter a local 'person_id': ")
    print(" ")
    cluster = input("Enter a cluster name (faster,aces,launch): ")
    if cluster == 'faster':
        resource = 'faster.tamu.xsede.org'
    elif cluster == 'aces':
        resource = 'aces.tamu.access.org'
    elif cluster == 'launch':
        resource = 'launch.tamu.access.org'
    sql = "SELECT * FROM local_info WHERE person_id = %s AND cluster = %s"
    data = (person_id,cluster,)
    results = []
    amiedb_call(sql,data,script_name,results)
    is_proj = len(results)
    if is_proj == 0:
        print(" ")
        print("There is no 'person_id' ",person_id," on cluster ",cluster,". Exiting.")
        sys.exit()
    curl_string = curl_string_base + "people/by_username/" + resource + "/" + person_id + "'"
    curl_string = curl_string + print_str
    os.system(curl_string)
    sys.exit()

elif req == 'access_id':
    print(" ")
    access_id = input("Enter an ACCESS ID: ")
    print(" ")
    sql = "SELECT * FROM local_info WHERE access_id = %s"
    data = (access_id,)
    results = []
    amiedb_call(sql,data,script_name,results)
    is_proj = len(results)
    if is_proj == 0:
        print(" ")
        print("The 'access_id' ",access_id," is not in 'local_info'. Exiting.")
        sys.exit()
    curl_string = curl_string_base + "people/by_portal_username/" + access_id + "'"
    curl_string = curl_string + print_str
    os.system(curl_string)
    sys.exit()

#  Find all ACCESS users with a given last name.
elif req == 'last_name':
    print(" ")
    last_name = input("Enter a last name (case insensitive) (Smith,Jones,McGillicuddy): ")
    print(" ")
    curl_string = curl_string_base + "people/by_lastname/" + last_name + "'"
    curl_string = curl_string + print_str
    print(curl_string)
    os.system(curl_string)
    sys.exit()

elif req == 'allocations':
    alloc_filename = 'alloc.txt'
    print("Attempting to obtain allocation information.")
#    curl_string = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/accounts/" + xdusage_str + "'"
#  Put the JSON response in a file named 'project_id'_'resource_id'.json.
#    curl_string = curl_string + " > " + alloc_filename
    curl_string = curl_string + " > " + alloc_filename + " 2> err.txt"
#  SEND THE BLOODY REQUEST ALREADY!
    print("curl_string = ",curl_string)
    os.system(curl_string)
    f = open(alloc_filename)
#  Creates a Python dictionary from a JSON object.
    data = json.load(f)
#  Extract results section from dictionary.
    results = data['result']
#  Extract allocation and charge summary for project.
    ca = results[0]['current_allocation']
    ta = results[0]['total_allocation']
    pc = results[0]['project_charges']
    pb = results[0]['project_balance']
    pi_un = results[0]['pi_portal_username']
    fn = results[0]['first_name']
    ln = results[0]['last_name']
    gn = results[0]['grant_number']
    pn = "p." + gn.lower() + ".000"
    print(" ")
#  results[0] =  {'project_id': 22867, 'grant_number': 'CHM220016', 'resource_id': 3295, 'resource_name': 'faster.tamu.xsede.org', 'project_account_id': 30346, 'project_start': '2023-01-01', 'project_end': '2024-06-30', 'project_state': 'active', 'is_expired': False, 'total_allocation': '20120660.0', 'current_allocation': '20120660.0', 'project_charges': '5672492.97053459', 'project_balance': '14448167.02946541', 'pi_person_id': 89668, 'pi_first_name': 'fanglin', 'pi_middle_name': '', 'pi_last_name': 'che', 'pi_portal_username': 'fc0109', 'person_id': 89668, 'first_name': 'fanglin', 'middle_name': '', 'last_name': 'che', 'portal_username': 'fc0109', 'is_pi': True, 'project_user_id': 85230, 'account_state': 'active', 'account_charges': '0.0'}
    print("Present state of project for PI ",fn,ln," with local username ",pi_un," on project ",pn,".")
    print(" ")
    print(" Current allocation: ",ca)
    print("   Total allocation: ",ta)
    print("    Project charges: ",pc)
    print("    Project balance: ",pb)
    print(" ")
#  Extract charges for each account user.
    print("Charges per user.")
    print(" ")
    tot_ch = 0.0
    for res in results:
        pu = res['portal_username']
        tot_ch = tot_ch + float(res['account_charges'])
        ch = int(float(res['account_charges']))
        print(pu.ljust(20),"    ",str(ch).rjust(10))
    print(" ")
    print(str(int(tot_ch)).rjust(36))
#  Remove the temporary results and error files.
    rf = "rm " + alloc_filename + " err.txt"
    os.system(rf)
    sys.exit()

# /acdb/xdusage/v2/accounts/<project_id>/<resource_id> where project_id = p.mch240011.000 and resource_id = faster.tamu.xsede.org
# curl -H "XA-Agent: xdusage" -H "XA-RESOURCE: faster.tamu.access.org" -H "XA-API-Key: XXX" -X GET https://allocations-api.access-ci.org/acdb/xdusage/v2/accounts/p.mch240011.000/faster.tamu.xsede.org

#  GET /xdusage/v2/projects?projects=CHE080068N,CHE150034&person_id=55&resource_id=2796,2801
#elif sys.argv[1] == 'projects':
#    curl_string = curl_header + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/v2/" + xdusage_str + "'"

#  GET /acdb/xdusage/auth_test
elif req == 'authtest':
    curl_string = curl_header_v + " -X GET 'https://allocations-api.access-ci.org/acdb/xdusage/auth_test'"
    curl_string = curl_string + " > std.out 2> err.out"
    os.system(curl_string)
    print("curl_string=",curl_string)
    okay = null = 0
    with open('err.out') as eo:
        for line in enumerate(eo):
            if str(line).find("200 OK") != -1:
                okay = 1
    with open('std.out') as so:
        for line in enumerate(so):
            if str(line).find('"message":null') != -1:
                null = 1
    if okay == 1 and null == 1:
        print("The authorization test succeeded.")
#    rf = "rm err.out std.out"
#    os.system(rf)

elif req == 'failed':
    print("Attempting to send get failed records request.")
    curl_string = 'curl -X GET -H "XA-Site: ' + site_name + '" -H "XA-API-Key: ' + api_key + '" '
    curl_string = curl_string + "'https://usage.access-ci.org/api/v1/usage/failed'"
    print("curl_string = ",curl_string)
    os.system(curl_string)

elif req == 'status':
    print("Attempting to send status request.")
    start_date = start_obj
    end_date = end_obj
    print("Start date = ",start_date," End date = ",end_date)
    curl_string = 'curl -X GET -H "XA-Site: ' + site_name + '" -H "XA-API-Key: ' + api_key + '" '
    curl_string = curl_string + "'https://usage.access-ci.org/api/v1/usage/status?FromTime="
    curl_string = curl_string + str(start_date) + "&ToTime=" + str(end_date) + "'"
    print("curl_string = ",curl_string)
    os.system(curl_string)


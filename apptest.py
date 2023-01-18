#!/usr/bin/python3

#  Read pending packets and dump required info to 'approval' table for interactive approval process.

import psycopg2
from amieclient import AMIEClient
from configparser import ConfigParser
from datetime import datetime
import sys
import json
import logging
import os.path
import random
from pprint import pprint

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

def local_no():
    locran = "%0.12d" % random.randint(0,999999999999)
    return locran

#  Define a suitably short time stamp macro.
def tstamp():
    tst = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return tst

#  Extract the value of any key in a nested JSON tree.
#  Note: This doesn't work when the value is a list.
def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []
    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr
    values = extract(obj, arr, key)
    return values

def amiedb_data_tbl(packet_rec_id,tag,subtag,seq,value):
    try:
#        conn = psycopg2.connect(host='inquire.tamu.edu',database=dbase,user=dbuser,password=pw)
        conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
        cursor = conn.cursor()
        try:
#            statement = """INSERT INTO data_tbl (packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s) on conflict (packet_rec_id) do nothing;"""
            statement = """INSERT INTO data_tbl (packet_rec_id,tag,subtag,seq,value) VALUES (%s, %s, %s, %s, %s);"""
            data = (packet_rec_id, tag, subtag, seq, value)
            cursor.execute(statement,data)
            conn.commit()
            print(" Inserted data for packet_rec_id ",packet_rec_id," for tag ",tag," into data_tbl.")
        except:
            print(" Data for packet_rec_id ",packet_rec_id," tag ",tag," already exists in data_tbl.")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while connecting to AMIEDB table data_tbl: ", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
    return

#  List of transaction types that need to be flagged for approval before processing.
approvalpack = ['request_project_create','request_account_create','request_project_inactivate','request_project_reactivate','request_account_inactivate','request_account_reactivate','request_user_modify','request_person_merge']
allpack = ['request_project_create','request_account_create','request_project_inactivate','request_project_reactivate','request_account_inactivate','request_account_reactivate','request_user_modify','request_person_merge','data_project_create','data_account_create','inform_transaction_complete']

# Establish a log file.
logging.basicConfig(filename='amie.log', level=logging.INFO)
logging.info(' Running ' + sys.argv[0] + ' on ' + tstamp())

#  Obtain the 'site-name', 'api_key' and 'amie_url' from the config file 'config.ini'.
config_site = ConfigParser()
config_site.read(amie_config)
site_con = config_site['TAMU']
pw = site_con['pw']
dbase = site_con['dbase']
dbuser = site_con['dbuser']

#  Create the AMIE client.

amie_client = AMIEClient(site_name=site_con['site_name'],
                         amie_url=site_con['amie_url'],
                         api_key=site_con['api_key'])

#trans = amie_client.get_transaction(trans_rec_id='108088921')

print(' ')
print(' Running ',sys.argv[0],' to process incoming packets at ',tstamp())
print(' ')

#  Get all the available packets.
#  The 'packets' variable contains a list of objects.
#  The first object element of that list is 'packets[0]'.
packets = amie_client.list_packets().packets
npack = len(packets)
print(' Number of packets to process: ',npack)
print(' ')
if (npack == 0):
    print(" No packets to process. Exiting.")
    sys.exit()

#  Loop through presently available packets.
pnum = 1
for packet in packets:
#    print(" pnum = ",pnum)

#  Test case with test.json file.
#    f = open('test.json')
#    pdict = json.load(f)['result'][0]
#    print(' pdict = ',pdict)

#  Read cases from incoming stream.
    pdict = json.loads(packet.json())
#    print(' pdict = ',pdict)
#    sys.exit()

#  Check if packet has been locally approved before sending any return packets.

#  Yoink trans_rec_id and grant_number out of packet to compare to 'approval' table data.
    trans_rec_id = packet.trans_rec_id
    type_id = json_extract(pdict,'type')[0]

    if type_id in approvalpack:

        print(" Packet no. ",pnum," of type ",type_id," with transaction record ID: ",trans_rec_id)
        if type_id == 'request_account_create':
            first_name = packet.UserFirstName
            last_name = packet.UserLastName
            global_id = packet.UserGlobalID
            grant_number = packet.GrantNumber
            organization = packet.UserOrganization
            print(" Request for ",first_name,last_name," of ",organization," with global ID ",global_id," and grant number ",grant_number)
        elif type_id == 'request_project_create':
            first_name = packet.PiFirstName
            last_name = packet.PiLastName
            global_id = packet.PiGlobalID
            grant_number = packet.GrantNumber
            organization = packet.PiOrganization
        elif type_id == 'request_project_inactivate':
            print("")
        elif type_id == 'request_project_reactivate':
            print("")
        elif type_id == 'request_account_inactivate':
            print("")
        elif type_id == 'request_account_reactivate':
            print("")
        elif type_id == 'request_user_modify':
            print("")
        elif type_id == 'request_person_merge':
            print("")
        
#approvalpack = ['request_project_create','request_account_create','request_project_inactivate','request_project_reactivate','request_account_inactivate','request_account_reactivate','request_user_modify','request_person_merge']

        print(" Request for ",first_name,last_name," of ",organization," with global ID ",global_id," and grant number ",grant_number)
#    print(" Request for ",first_name,last_name," with global ID ",global_id," and grant number ",grant_number)
#    print(' trans_rec_id = ',trans_rec_id,' type_id = ',type_id)
#        grant_number = json_extract(pdict,'grant_number')

#  Check 'approval' table status to decide whether to process the packet, i.e. 'approved' means yes.
        try:
            conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
            cur = conn.cursor()
            sql = "SELECT * FROM approval WHERE trans_rec_id = %s"
            data = (trans_rec_id,)
            cur.execute(sql,data)
            status = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as err:
            print(' An exception has occurred: ',error)
            print(' Exception type: ',type(error))

        if (len(status) == 0):
            print(" No entry in 'approval' table for transaction record #",trans_rec_id)
        else:
            app_status = status[0][4]
#  Skip to next packet loop iteration if packet is not approved.
            if (app_status != 'approved'):
                print(" Approval status for Transaction #",trans_rec_id," is: ",app_status,". Moving to next packet.")
                print(' ')
                pnum = pnum + 1
                continue
#  Move on to process response packet if request packet is approved, and add reply date to 'approval' table.
            else:
                print(" Approval status for Transaction #",trans_rec_id," is: ",app_status,". Starting reply process.")
                try:
                    conn = psycopg2.connect(host='localhost',database=dbase,user=dbuser,password=pw)
                    cur = conn.cursor()
                    sql = 'UPDATE approval SET ts_reply = %s, reply_status = %s WHERE trans_rec_id = %s'
                    ts_reply = tstamp()
                    data = (ts_reply, '1', trans_rec_id)
                    cur.execute(sql,data)
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as err:
                    print(' An exception has occurred: ',error)
                    print(' Exception type: ',type(error))

        print(' ')
        pnum = pnum + 1




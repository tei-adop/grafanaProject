 
# compass project:
# by Joseph Woo
# 
# Responsible for retrieving SQL databases directory from the Amazon Athena AWS, and storing them
# in temporary databases within ec2. This script will then modify those databases and create tables which will be
# easier for grafana to interpret and convert into visuals. 

import re
import os
import socket
import IP2Location
from pyathena import connect
from collections import Counter
from datetime import datetime
import mysql.connector

# For timestamps dealing with hourly2
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
hour = now.strftime("%H")

# This cursor is for running querries on the AWS Athena Server
aCursor = connect(aws_access_key_id='AKIAWNMRMGA2NNX5KYBS',
                 aws_secret_access_key='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',
                 s3_staging_dir='s3://adop-dmp/Data/basic',
                 region_name='ap-northeast-2').cursor()
# Will retrieve the first 10000 entries of the required columns, then closes the connection
print("Connected")
hourlyQuerry = "SELECT zid, acid, dev, dt, ip, brow, os, loc, lang, title, w, h, ctry, net FROM \"basic\".\"hourly2\" WHERE year='" + year + "' AND month='" + month + "' AND day = '"+day+"' AND HOUR = '"+hour+"';"
tempHourly = "SELECT zid, acid, dev, dt, ip, brow, os, loc, lang, title, w, h, ctry, net FROM \"basic\".\"hourly2\" limit 10000;"
aCursor.execute(tempHourly) #acid
allResults = aCursor.fetchall()
print("hourly data retrieved")
aCursor.execute("SELECT unit_nm, com_req, req, imp, clk, earn, exchange, fee, sdate FROM \"data_ocean\".\"report_daily\" WHERE date_parse(sdate, '%Y-%m-%d') >= cast(current_date - interval '90' day as timestamp);")
eResults = aCursor.fetchall()
aCursor.close()
print("report_daily data retrieved")
# This cursor is for running querries on the ec2 mysql Server
serverDB = mysql.connector.connect(
    host="localhost",
    user="root",
    password="adop1234"
)

# This cursor is for create the database in the local ec2 instance.
serverDB.autocommit = True
sCursor = serverDB.cursor(buffered=True)
print("connected to local mysql")

sCursor.execute('CREATE DATABASE IF NOT EXISTS grafanaDB;')
sCursor.execute('USE grafanaDB;')

# creates a table with acid as the key values. If there's already a table, then it is not made and a status message is printed out

tablecreation = """CREATE TABLE IF NOT EXISTS mtable(keyZID VARCHAR(60) NOT NULL, keyACID VARCHAR(40) NOT NULL, device VARCHAR(20),
aTime VARCHAR(20), ip VARCHAR(20) NOT NULL, browser VARCHAR(40), OS VARCHAR(20), website VARCHAR(100),
language VARCHAR(10), title VARCHAR(200) CHARACTER SET utf8, width VARCHAR(5), height VARCHAR(5), ctry VARCHAR(5),
network VARCHAR(30), formTime DATETIME, formWidth SMALLINT(6), formHeight SMALLINT(6), PRIMARY KEY (keyZID, keyACID));"""

etablecreate = """CREATE TABLE IF NOT EXISTS etable(unit_nm VARCHAR(50) NOT NULL, CompassReq INT(20), totalRequests INT(20) NOT NULL, 
totalImpress INT(20) NOT NULL, clicks INT(20), earning DEC(12, 2), exrate DEC(10,4), fee INT(10), rawDate VARCHAR(20), formDate DATE, PRIMARY KEY (unit_nm, totalRequests, totalImpress));"""

loctablecreate = """CREATE TABLE IF NOT EXISTS loctable(ip VARCHAR(20), longitude DECIMAL(12,8), latitude DECIMAL(12,8));"""
print("connected to local database")

sCursor.execute(tablecreation)
sCursor.execute(etablecreate)
sCursor.execute(loctablecreate)

# inserts the values retrieved by the athena querry into the database
valueInsertPlace = """INSERT IGNORE INTO mtable(keyZID, keyACID, device, aTime, ip, browser, OS, website,
language, title, width, height, ctry, network) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
evalInsertion = """INSERT IGNORE INTO etable(unit_nm, CompassReq, totalRequests, totalImpress, clicks, earning, exrate, fee, rawDate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"""
sCursor.executemany(valueInsertPlace, allResults)
sCursor.executemany(evalInsertion, eResults)
print("entries inserted into local databases")

#miscellanius updating
sCursor.execute("UPDATE mtable SET formTime = STR_TO_DATE(aTime, '%Y%m%d%H%i%s');")
sCursor.execute("UPDATE etable SET formDate = STR_TO_DATE(rawDate, '%Y-%m-%d');")
sCursor.execute("UPDATE mtable SET formWidth = CONVERT(width, UNSIGNED);")
sCursor.execute("UPDATE mtable SET formHeight = CONVERT(height, UNSIGNED);")
sCursor.execute("UPDATE mtable SET language = SUBSTRING(language,1,2);")
sCursor.execute("SHOW WARNINGS;")
print("Did conversions")

sCursor.execute("SELECT ip FROM mtable;")
iplists = sCursor.fetchall()
ipformatlist = []
longlist = []
latlist = []
IP2LocObj = IP2Location.IP2Location()
IP2LocObj.open("IP2LOCATION-LITE-DB5.IPV6.BIN")

for x in iplists:
    try:
        temp = re.sub(r"[()',]",'',str(x))
        ipformatlist.append(temp)
        rec = IP2LocObj.get_all(temp)
    except socket.error as e:
        print("Error:", e)
        longlist.append(None)
        latlist.append(None)
    else:
        longlist.append(rec.longitude)
        latlist.append(rec.latitude)
print("Grabbed and converted IP")
sCursor.execute("DELETE FROM loctable;")
index = 0
while index < len(iplists):
    sCursor.execute("INSERT INTO loctable(ip, longitude, latitude) VALUES (%s, %s, %s);", (ipformatlist[index], longlist[index], latlist[index]))
    index += 1
#Table for grafana to read

print("success")

serverDB.close()
sCursor.close()


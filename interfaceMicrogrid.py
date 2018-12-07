#!/usr/bin/python3

#PyMySQL
import pymysql.cursors
#JSON for exporting
import json

# Connect to the database
connection = pymysql.connect(host='10.9.0.4',
                             user='bcpg', #our "select query only" account
                             password='bcpg_mysql_pass',
                             db='openPDC',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

try:
    with connection.cursor() as cursor:

        #Opens necessary files
        file = open('someFile.txt','a')
        constraints = open('constraints.txt','r')
        lines = constraints.read().split('\n')

        #Our loop that runs through the PMUs listed in the constraints file, expandable
        for x in range(16):
            #Creates the applicable variables for our queries
            items = lines[x].split(',')
            pmu = items[0]
            pmuNum = items[1]
            dataType = items[2]
            dataMin = items[3]
            dataMax = items[4]
            
            #Base SQL query
            cursor.execute("SELECT Value,tsmID FROM timeseriesmeasurement WHERE SignalID = %s AND Value <= %s AND Value >= %s AND tsmID >= (SELECT MAX(tsmID) FROM timeseriesmeasurement)-600 LIMIT 1", (pmu,dataMax,dataMin))
            result = cursor.fetchone()
            
            #Writes to a file in a readable format
            file.write("PMU "+str(pmuNum)+" "+str(dataType)+" Reading: "+str(result)+"\n")

            #Prints to console
            print(result)
            
        #Closes all files    
        constraints.close()
        file.write("\n")
        file.close()
        
        
finally:
    connection.close()

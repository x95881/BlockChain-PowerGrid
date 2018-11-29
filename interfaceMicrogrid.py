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
        # Test a select query
        sql = "SELECT tsmID, Timestamp FROM timeseriesmeasurement WHERE tsmID = (SELECT MAX(tsmID) FROM timeseriesmeasurement)"

        cursor.execute(sql)
        result = cursor.fetchone()
        
        file = open('someFile.txt','a')
        file.write(str(result)+"\n")
        file.close()
        print(result)
        
finally:
    connection.close()

import sys
import logging
import pymysql
import json
import os
from math import sin, cos, sqrt, atan2, radians
import geopy.distance

#rds settings
rds_endpoint = os.environ['rds_endpoint']
username=os.environ['username']
password=os.environ['password']
db_name=os.environ['db_name']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Connection
try:
    connection = pymysql.connect(host=rds_endpoint, user=username,
        passwd=password, db=db_name)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()
logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def handler(event, context):
    cur = connection.cursor()  
## Retrieve Data
    latt1 = radians(float(event['latt']))
    longt1 = radians(float(event['longt']))
    query = "SELECT now()"
    cur.execute(query)
    connection.commit()
    record = cur.fetchone()
    print(record)
    query = "SELECT b.id,latt,longt \
        FROM Branch b, Queue q, OpeningHours op \
        WHERE b.id=q.branchId AND b.id=op.branchID \
        AND current_time+ interval 8 hour between opens and closes and op.dayOfWeek=dayofweek(now()) \
        GROUP BY b.id;"    
    cur.execute(query)
    connection.commit()
## Construct body of the response object
# https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude
# https://stackabuse.com/how-to-sort-dictionary-by-value-in-python/
    branchList = []
    rows = cur.fetchall()
    if len(rows) >0:
        id = 0
        latt = 0
        longt = 0
        ## Distance calculation
        distanceDict = dict()
        for row in rows:
            print("TEST {0} {1} {2}".format(row[0],row[1],row[2]))
            id = row[0]
            latt2 = radians(row[1])
            longt2 = radians(row[2])
            coords_1=(latt1,longt1)
            coords_2=(latt2,longt2)
            distance = geopy.distance.distance(coords_1, coords_2).km
            print ("GEOPY DISTANCE: ", distance)
            distanceDict[id] = distance
        sorted_distanceDict = dict()
        ## Order according to distance, shortest distance first
        sorted_keys = sorted(distanceDict, key=distanceDict.get)
        count = 1
        
        for w in sorted_keys:
            print("Dict Key Branch ID: {0}".format(w))
            ## Queue length
            query = "select count(*) from Queue where branchId={} and status='Q'".format(w)
            cur.execute(query)
            connection.commit()
            row = cur.fetchone()
            sorted_distanceDict[w] = count + row[0]/10
            count += 1
        bestBranch = min(sorted_distanceDict,key=sorted_distanceDict.get)
        print("bestBranch ID = {0}".format(bestBranch))

        # Best Branch details
        query = "SELECT b.*,sum(q.status='Q'),op.opens,op.closes FROM Branch b, Queue q, OpeningHours op WHERE b.id=q.branchId AND b.id={} AND b.id=op.branchId and op.dayOfWeek=dayofweek(now()) GROUP BY b.id;".format(bestBranch)
        cur.execute(query)
        connection.commit()
        rows = cur.fetchall()
        for row in rows:
            print("TEST {0} {1} {2} {3} {4} {5}".format(row[0],row[1],row[2],row[3],row[4],row[5]))
            transactionResponse = {}
            transactionResponse['id'] = row[0]
            transactionResponse['name'] = row[1]
            transactionResponse['district'] = row[2]
            transactionResponse['addr'] = row[3]
            transactionResponse['postal'] = row[4]
            transactionResponse['contactNo'] = row[5]
            transactionResponse['latt'] = row[6]
            transactionResponse['longt'] = row[7]
            transactionResponse['clinicId'] = row[8]
            transactionResponse['queueLength'] = row[9]
            transactionResponse['opens']=str(row[10])
            transactionResponse['closes']=str(row[11])
            branchList.append(transactionResponse)

# Construct http response object
    responseObject = {}
    responseObject['data']= branchList
    return responseObject


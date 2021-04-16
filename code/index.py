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
    query = "SELECT b.id,latt,longt, count(b.id) as queue \
        FROM Branch b, Queue q, OpeningHours op \
        WHERE b.id=q.branchId AND b.id=op.branchID \
        AND status='Q' \
        AND current_time- interval 16 hour between opens and closes and op.dayOfWeek=dayofweek(now()) \
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
        # Distance and Queue length calculation
        distanceDict = dict()
        for row in rows:
            print("TEST {0} {1} {2} {3}".format(row[0],row[1],row[2],row[3]))
            id = row[0]
            latt2 = radians(row[1])
            longt2 = radians(row[2])
            coords_1=(latt1,longt1)
            coords_2=(latt2,longt2)
            distance = geopy.distance.distance(coords_1, coords_2).km
            print ("GEOPY DISTANCE: ", distance)
            distanceDict[id] = distance
        sorted_distanceDict = dict()
        sorted_keys = sorted(distanceDict, key=distanceDict.get)
        count = 1
        for w in sorted_keys:            
            if(row[3] > 5):
                sorted_distanceDict[w] = count + 1
            else:
                sorted_distanceDict[w] = count
            count += 1
        bestBranch = min(sorted_distanceDict,key=sorted_distanceDict.get)

        # Distance and Queue length calculation
        query = "SELECT b.*,count(b.id) as queue FROM Branch b, Queue q WHERE b.id=q.branchId AND b.id={} AND q.status='Q' GROUP BY b.id;".format(bestBranch)
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
            branchList.append(transactionResponse)

# Construct http response object
    responseObject = {}
    # responseObject['statusCode'] = 200
    # responseObject['headers'] = {}
    # responseObject['headers']['Content-Type']='application/json'
    # responseObject['headers']['Access-Control-Allow-Origin']='*'
    responseObject['data']= branchList
    # responseObject['body'] = json.dumps(transactionResponse, sort_keys=True,default=str)
    
    #k = json.loads(responseObject['body'])
    #print(k['uin'])

    return responseObject


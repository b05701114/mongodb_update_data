'''
Usage: [step 2] upsert pair data to mongodb us.pair_v2
Version: 1.1.0
Author: Ling
Email: r10725024@ntu.edu.tw 
'''
import pymongo
import pprint
import csv
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
import pandas as pd
import json
import os
import sys
import ijson
import datetime
path = '/media/james/Database/pair_raw/'
# import mongodb config info
with open(path+'config.json','r', encoding = 'utf8') as config:
    dic = json.load(config)
client = MongoClient(dic["IPAddress"], dic["portNum"],
username=dic["userName"],
password=dic["passWord"],
authSource=dic["dbName"],
authMechanism='DEFAULT')
db = client.get_database(dic["dbName"])

# read all json files
files = [f for f in os.listdir(path+'data') if os.path.isfile(path+'data/'+f)]
is_file_err = 0
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 2] Starts')
##print(files)

for i in sorted(files):
    if i.split('.')[1] == "json" and i not in ["config.json", "structure_pair.json", "config_test.json"]:
        missing = open(path+'log/missing.txt', 'a+')
        logfile = open(path+'log/pair-log.txt', 'a+')
        count = 0
        with open(path+'data/' + i,'r', encoding="utf-8") as json_file:
            logfile.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ' + i + " started" + "\n")
            try:
                l = ijson.items(json_file, 'PatentBulkData.item',use_float=True)
                for data in l:
                    try:
                        data["_id"] = data['patentCaseMetadata']['applicationNumberText']['value'] # set field _id to application number
                        db.pair_v2.update_one({"_id":data["_id"]},{"$set":data},upsert=True) # do upsert
                        count += 1
                    except Exception as err:
                        missing.write(i + str(count) + " insert field error" + "\n")
                logfile.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' ' + i + " finished" + "\n")
            except Exception as err:
                missing.write(i + " ijson can not read file error" + "\n")
                print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 2] File cannot read year: '+ i ) # if one json file cannot read pass first but print
                is_file_err = 1
if is_file_err == 0:
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 2] Finished')
else:
    sys.exit(1)

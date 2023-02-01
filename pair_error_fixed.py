'''
Usage: Convert data with Decimal data type to string and upsert to mongodb
Version: 1.1.0
Author: Ling
Email: r10725024@ntu.edu.tw
'''

import pymongo
import pprint
import csv
from pymongo import MongoClient, IndexModel, ASCENDING, DESCENDING
import pandas as pd
import json, decimal
import os
import ijson
from decimal import Decimal
import numpy as np
import datetime
import sys
path = '/media/james/Database/pair_raw/'
with open(path+'config.json','r', encoding = 'utf8') as config:
    dic = json.load(config)
client = MongoClient(dic["IPAddress"], dic["portNum"],
username=dic["userName"],
password=dic["passWord"],
authSource=dic["dbName"],
authMechanism='DEFAULT')
db = client.get_database(dic["dbName"])


def get_tag_structure(json_file):
    '''
    Convert fields that is not int or string to string
    Input: Dict
    Output: Dict
    '''
    all_tag = {}
    if isinstance(json_file,dict):
        for i in json_file.keys():
            if not isinstance(json_file[i],dict):
                if isinstance(json_file[i],list):
                    all_tag[i] = get_tag_structure(json_file[i][0])
                else:
                    if not (isinstance(json_file[i],int) or isinstance(json_file[i],str)):
                            all_tag[i] = str(json_file[i])
                    else:
                        all_tag[i] = json_file[i] 
            else:
                all_tag[i] = {}
                all_tag[i] = get_tag_structure(json_file[i])
    else:
        return json_file
    return all_tag

# read missing file to get step 2 error data
error_list = open(path+"log/missing.txt","r")
error_list = error_list.read()
error_list = error_list.split("\n")
error_list = error_list[:-1]
years_error = {}
for l in error_list:
    if l.split("json")[0] + "json" not in years_error.keys():
        years_error[l.split("json")[0] + "json"] = []
    years_error[l.split("json")[0] + "json"].append(int(l.split("json")[1].split(" ")[0]))
missing = open(path+'log/missing_fixed.txt', 'a+')
logfile = open(path+'log/pair-log.txt', 'a+')
total_count = 0
is_data_error = 0

# start upsert error data again
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[Step 3] Starts')
for e in years_error.keys():
    insert_file = path+'data/' + e
    logfile.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')+ e +' Total count: ' + str(len(years_error[e])) + '\n')
    with open(insert_file,'r') as json_file:
        l = ijson.items(json_file, 'PatentBulkData.item', use_float=True)
        years_error[e].sort()
        years_error[e] = list(np.unique(years_error[e]))
        targetCnt = years_error[e][0]
        index = 0
        count = 0
        try:
            for data in l:
                if count == targetCnt:
                    try:
                        curr_json = get_tag_structure(data) # change data type to string
                        curr_json['_id'] = curr_json['patentCaseMetadata']['applicationNumberText']['value']
                        db.pair_v2.update_one({"_id":curr_json['_id']},{"$set":curr_json},upsert=True)
                        count += 1
                        total_count += 1
                        index += 1
                        if index + 1 >= len(years_error[e]):
                            break
                        while years_error[e][index] <= targetCnt and index < len(years_error[e]):
                            index += 1 
                            targetCnt = years_error[e][index]
                        targetCnt = years_error[e][index]
                        #logfile.write(e + " " + str(count))
                    except Exception as err:
                        count += 1
                        missing.write(e + " " + str(count)+ " " + str(err)+"\n")
                        is_data_error = 1
                        print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[Step 3] Json file write error', e)
                else:
                    count += 1
        except Exception as err:
            missing.write(e + " " + str(count)+ " " + str(err)+"\n")
            is_data_error = 1
            print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[Step 3] Json file read error', e) 
            pass
print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '[Step 3] Finished')
if is_data_error==1:
    sys.exit(1)
else:
    sys.exit(0)

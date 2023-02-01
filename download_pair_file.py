'''
Usage: [Step 1] pair data download and unzip
Version: 1.0.0
Update Time: 2022-12-04
Author: Ling
Email: r10725024@ntu.edu.tw
'''
import requests
import zipfile
import sys
import json
from datetime import datetime, timedelta
import wget
import os

path = '/media/james/Database/pair_raw/'
url = "https://ped.uspto.gov/api/"
payload={}
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
response = requests.request("GET", url, headers=headers, data=payload) # get all the json zip filenames
if response.status_code != 200:
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 1] cannot get download filename')
    sys.exit(1)
else:
    files_raw = json.loads(response.text)
    files = files_raw['jsonDownloadMetadata']
    update_time = datetime.strptime(files[1]['lastUpdated'][:15], '%a %d %b %Y')
    to_sat = datetime.now() - timedelta(days = 3)

    # check if the file is new
    if update_time >= to_sat:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 1] Starts')
        for f in files:
            if f["fileName"][:4] not in ["1900", "pair", '1920', '1940']: # files that are too old
            #if f["fileName"][:4] in ["1960"]:
                try:
                    url_download = url + 'full-download?fileName=' + f["fileName"]
                    wget.download(url_download, out=path+'data/' + f["fileName"] + '.zip') # download
                    with zipfile.ZipFile(path+'data/' + f["fileName"] + '.zip', 'r') as zip_ref: # unzip
                        zip_ref.extractall(path+'data/')
                except Exception as err:
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 1] download error')
                    print(err)
                    sys.exit(1)
        all_years_len = int(to_sat.year)-1960+1
        files_num = len([f for f in os.listdir(path+'data') if os.path.isfile(path+'data/'+f)])
        if files_num < all_years_len:
            print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '[Step 1] download file number not right')
            sys.exit(1)
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 1] Finished')
    else:
        print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' [Step 1] Download file not yet update')
        sys.exit(1)

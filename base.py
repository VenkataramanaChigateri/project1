import pymongo
import requests
import csv
from bson import ObjectId

"""" Downloading file"""

def download_file(url,fileName):
    try:
        r = requests.get(url, allow_redirects=True)
        print (r.headers.get('content-type'))
        open(fileName, 'wb').write(r.content)
        print("downloading file and writing it...")
    except Exception as e:
        print("unable to download and write !!!")
        print(e)

""" DB setup """
def DB_setup(fileName):
    try:
        client = pymongo.MongoClient('localhost',27017)
        data_File = csv.DictReader(open(fileName))
        db= client['NSEDB']
        print("Setting up the NSE data DB...")
    except Exception as e:
        print(e)
    rows = []
    for row in data_File:
        rows.append(row)

    for i in rows:
       original_id = ObjectId()
       db.company_info_collection.insert_one({"_id": original_id,"SYMBOL":i["SYMBOL"],"DATE1": i[" DATE1"]})
       db.stock_Info_collection.insert_one({"Company_id": original_id,"PREV_CLOSE":i[" PREV_CLOSE"],"OPEN_PRICE": i[" OPEN_PRICE"],"HIGH_PRICE": i[" HIGH_PRICE"],"LOW_PRICE": i[" LOW_PRICE"],"LAST_PRICE": i[" LAST_PRICE"],"CLOSE_PRICE": i[" CLOSE_PRICE"],"AVG_PRICE": i[" AVG_PRICE"],"TTL_TRD_QNTY": i[" TTL_TRD_QNTY"],"TURNOVER_LACS": i[" TURNOVER_LACS"],"NO_OF_TRADES": i[" NO_OF_TRADES"],"DELIV_QTY": i[" DELIV_QTY"],"DELIV_PER": i[" DELIV_PER"]})
    print("Inserting columns from CSV to DB....")


if __name__ == "__main__":
    #URL addr and filename is set here
    url='https://www.nseindia.com/products/content/sec_bhavdata_full.csv'
    fileName='NSEDataFile.csv'

    download_file(url,fileName)
    DB_setup(fileName)

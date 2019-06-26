import pymongo
import requests
import csv
from bson import ObjectId

"""" Downloading file"""

url='https://www.nseindia.com/products/content/sec_bhavdata_full.csv'
fileName='asd.csv'
r = requests.get(url, allow_redirects=True)
print (r.headers.get('content-type'))
open(fileName, 'wb').write(r.content)

""" DB setup """
rows = []
client = pymongo.MongoClient('localhost',27017)
data_File = csv.DictReader(open(fileName))
db= client['example']

for row in data_File:
    rows.append(row)

"""Inserting columns from CSV to DB"""
for i in rows:
   original_id = ObjectId()
   db.collection1.insert_one({"_id": original_id,"SYMBOL":i["SYMBOL"],"DATE1": i[" DATE1"]})
   db.collection2.insert_one({"Company_id": original_id,"PREV_CLOSE":i[" PREV_CLOSE"],"OPEN_PRICE": i[" OPEN_PRICE"],"HIGH_PRICE": i[" HIGH_PRICE"],"LOW_PRICE": i[" LOW_PRICE"],"LAST_PRICE": i[" LAST_PRICE"],"CLOSE_PRICE": i[" CLOSE_PRICE"],"AVG_PRICE": i[" AVG_PRICE"],"TTL_TRD_QNTY": i[" TTL_TRD_QNTY"],"TURNOVER_LACS": i[" TURNOVER_LACS"],"NO_OF_TRADES": i[" NO_OF_TRADES"],"DELIV_QTY": i[" DELIV_QTY"],"DELIV_PER": i[" DELIV_PER"]})



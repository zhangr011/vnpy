# encoding: UTF-8

import sys, os, copy, csv, signal

vnpy_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

sys.path.append(vnpy_root)

from vnpy.data.mongo.mongo_data import MongoData

mongo_data = MongoData(host='127.0.0.1', port=27017)

db_name = 'FutureRenko'
collections = mongo_data.get_collections(db_name=db_name)

for collection_name in collections:

    #if collection_name != 'AU99_K10':
    #    continue

    filter = {'date': {'$gt': "2020-04-20"}}
    print(f'removing {collection_name} with filter: {filter}')
    mongo_data.db_delete(db_name=db_name, col_name=collection_name, flt= filter)


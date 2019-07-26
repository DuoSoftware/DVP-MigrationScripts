import time
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import configs.ConfigHandler as conf
default_data = conf.get_conf('config.ini','DefaultData')
mongoconf = conf.get_conf('config.ini','mongo')

db = None

def usergroups():
    try:
        db['usergroups'].update_many({}, {'$set': {'businessUnit': default_data['bu']}})
    except Exception as err:
        print(err)

# def todos():
#     try:
#         db['todos'].update_many({}, {'$set': {'businessUnit': default_data['bu']}})
#     except Exception as err:
#         print(err)

# todos - No usage, ticketviews - active, order are present in singer db no need of migration

# ticketstatusnodes - discuss and update category field

# phoneconfigs - phoneType only one ?

# personalmessages - session?

# packages setupFee, spaceLimit, billingType only 2 docs ?

# codecAccessLimits, ownerRef, spaceLimit, timeZone, unitDetails, languages, packageDetails

# customersatisfactions - submitter, requester, ticket

# comments - author_external ?

# cases ignore, some docs have the missing keys

# bulkoperations only one doc. should ignore?

# def engagementsessions():
#     try:
#         docs = db.engagements.find({})
#         for i in docs:
#             print(i)
#             for j in i["engagements"]:
#                 print(j)
#                 if isinstance(i["profile"], str):
#                     db.engagementsessions.update_one({"_id" : j}, {"$set": {"profile_id": i["profile"], "has_profile": True}})
#                 else:
#                     db.engagementsessions.update_one({"_id" : j}, {"$set": {"profile_id": str(i["profile"]), "has_profile": True}})
#
#     except Exception as err:
#         print(err)

limit = 1000
offset = 0

def engagementsessions(limit, offset):
    try:
        docs = db.engagements.find({}).limit(limit).skip(offset)
        print(docs.count(True))
        if docs.count(True) > 0:
            for i in docs:
                print(i)
                for j in i["engagements"]:
                    print(j)
                    if isinstance(i["profile"], str):
                        db.engagementsessions.update_one({"_id" : j}, {"$set": {"profile_id": i["profile"], "has_profile": True}})
                    else:
                        db.engagementsessions.update_one({"_id" : j}, {"$set": {"profile_id": str(i["profile"]), "has_profile": True}})
            time.sleep(1)
            offset += 1000
            engagementsessions(limit, offset)
        else:
            return 0
    except Exception as err:
        print(err)

def contacts():
    try:
        db['contacts'].update_many({}, {'$set': {'businessUnit': default_data['bu']}})
    except Exception as err:
        print(err)

def user_accounts(limit, offset):
    try:
        docs = db.users.find({}).limit(limit).skip(offset)
        print(docs.count(True))
        if docs.count(True) > 0:
            for i in docs:
                print(i)
                db.useraccounts.insert({"userref": ObjectId("{0}".format(i['_id'])),
                                        "user_meta": i.get("user_meta"),
                                        "user": i.get("username"),
                                        "tenant": i.get("tenant"),
                                        "company": i.get("company"),
                                        "resource_id": i.get("resourceid"),
                                        "created_at": i.get("created_at"),
                                        "updated_at": i.get("updated_at"),
                                        "multi_login": i.get("multi_login"),
                                        "client_scopes": i.get("client_scopes"),
                                        "user_scopes": i.get("user_scopes"),
                                        "veeryaccount": i.get("veeryaccount"),
                                        "verified": i.get("verified"),
                                        "active": i.get("Active"),
                                        "allowed_file_categories": i.get("allowed_file_categories"),
                                        "joined": i.get("joined"),
                                        "allowoutbound": i.get("allowoutbound")
                                        })
            offset += 1000
            engagementsessions(limit, offset)
        else:
            return 0
    except Exception as err:
        print(err)


if __name__ == "__main__":

    HOST = mongoconf['database_2_host']
    DB  = mongoconf['database_2_name']
    username  = mongoconf['database_2_username']
    password  = mongoconf['database_2_password']

    # mgo1 = MongoClient('mongodb://duo:DuoS123@104.236.231.11:27017/dvpdb')
    mgo = MongoClient(HOST)
    # mgo1 = MongoClient('mongodb://{0}:{1}@{2}:{3}/dvpdb'.format(mongoconf['database_1_username'], mongoconf['database_1_password'], HOST1, mongoconf['database_1_port']))
    # mgo2 = MongoClient('mongodb://{0}:{1}@{2}:{3}/dvpdb'.format(mongoconf['database_2_username'], mongoconf['database_2_password'], HOST2, mongoconf['database_2_port']))

    db = mgo[DB]
    db.authenticate(username, password)

    # usergroups()
    # engagementsessions(limit, offset)
    user_accounts(limit, offset)

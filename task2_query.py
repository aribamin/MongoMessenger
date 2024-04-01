# must run and display the output for requested queries

# should implement step 3 and 4 of task 1 it seems
import pymongo 
from pymongo import MongoClient
import sys
import time

def connect_to_mongodb(port):
    try:
        client = pymongo.MongoClient(f"mongodb://localhost:{port}/")
        print("Connected successfully to MongoDB")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
        sys.exit(1)

def query1(db):
    count = db.messages.count_documents({"text": {"$regex": "ant"}})  # Case-insensitive search
    print(f"Number of messages containing 'ant': {count}")
def query2(db):
    pipeline = [
        {"$group": {"_id": "$sender_info.sender_id", "totalMessages": {"$sum": 1}}},
        {"$sort": {"totalMessages": -1}},
        {"$limit": 1}
    ]
    result = list(db.messages.aggregate(pipeline))
    if result:
        print(f"Sender with the most messages: {result[0]['_id']} (Messages sent: {result[0]['totalMessages']})")
    else:
        print("No data found.")
def query3(db):
    count = db.messages.count_documents({"sender_info.credit": 0})
    print(f"Number of messages from senders with credit 0: {count}")
def query4(db):
    messages = db.messages.find({"sender_info.credit": {"$lt": 100}})
    for message in messages:
        new_credit = message["sender_info"]["credit"] * 2
        db.messages.update_one({"_id": message["_id"]}, {"$set": {"sender_info.credit": new_credit}})
    print("Credits updated.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    client = connect_to_mongodb(port)
    db = client["MP2Embd"]

    query1(db)
    print("------------------------------------------")
    query2(db)
    print("------------------------------------------")
    query3(db)
    print("------------------------------------------")
    query4(db)

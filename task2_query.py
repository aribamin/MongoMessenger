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
    try:
        start_time = time.time()
        count = db.messages.count_documents({"text": {"$regex": "ant"}}, maxTimeMS=120000) #case sensitive
        end_time = time.time()
        print(f"Number of messages containing 'ant': {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 1 took more than 2 minutes.")

def query2(db):
    try:
        start_time = time.time()
        pipeline = [
            {"$group": {"_id": "$sender", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        result = list(db.messages.aggregate(pipeline, maxTimeMS = 120000))
        end_time = time.time()
        if result:
            print(f"Sender with the most messages: {result[0]['_id']} (Messages sent: {result[0]['count']})")
        else:
            print("No messages found.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 2 took more than 2 minutes.")

from bson.objectid import ObjectId

def query3(db):
    try:
        start_time = time.time()

        # get senders
        senders_with_zero_credit = db.senders.find({"credit": 0}, {"_id": 1, "sender_id": 1}).max_time_ms(120000)

        #extract their ids
        sender_ids = []
        for sender in senders_with_zero_credit:
            s = str(sender['sender_id'])
            sender_ids.append(s)

        #add msg count for each sender
        message_counts = 0
        ms = 0
        for sender in sender_ids:
            ms = db.messages.count_documents({"sender": sender})
            message_counts += ms

        print(f"Number of messages from senders with credit 0: {message_counts}")
        end_time = time.time()
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 3 took more than 2 minutes.")

def query4(db):
    try:
        start_time = time.time()
        result = db.senders.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})
        end_time = time.time()
        print(f"Updated {result.modified_count} senders' credits.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 4 took more than 2 minutes.")

def create_indices(db):
    try:
        start_time = time.time()
        db.messages.create_index([("sender", pymongo.ASCENDING)])
        db.messages.create_index([("text", pymongo.TEXT)])
        db.senders.create_index([("sender_id", pymongo.ASCENDING)])
        end_time = time.time()
        print("Indices created successfully.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Creating indices took more than 2 minutes.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    client = connect_to_mongodb(port)
    db = client["MP2Norm"]

    query1(db)
    print("------------------------------------------")
    query2(db)
    print("------------------------------------------")
    query3(db)
    print("------------------------------------------")
    query4(db)

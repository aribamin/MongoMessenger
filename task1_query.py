# must run and display the output for requested queries

# should implement step 3 and 4 of task 1 it seems
import pymongo 
#import MongoClient
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
        # correct regex pattern; no need for '*' wildcard characters.
        # using 'i' option for case-insensitive search.
        #count = db.messages.count_documents({"text": {"$regex": "*ant*"}}, maxTimeMS=120000)
        count = db.messages.count_documents({"text": {"$regex": "ant", "$options": "i"}}, maxTimeMS=120000)
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
        result = list(db.messages.aggregate(pipeline, maxTimeMS=120000))
        end_time = time.time()
        if result:
            print(f"Sender with the most messages: {result[0]['_id']} (Messages sent: {result[0]['count']})")
        else:
            print("No messages found.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 2 took more than 2 minutes.")

#Q3: Return the number of messages where the sender’s credit is 0.
def query3(db):
    try:
        # start_time = time.time()
        # count = db.senders.count_documents({"credit": 0}, maxTimeMS=120000)
        # end_time = time.time()
        
        # first find the senders with credit 0
        start_time = time.time()
        senders_with_zero_credit = db.senders.find({"credit": 0}, {"_id": 1})
        sender_ids = [sender['_id'] for sender in senders_with_zero_credit]
        
        # then count messages from these senders
        count = db.messages.count_documents({"sender_id": {"$in": sender_ids}})
        end_time = time.time()
        print(f"Number of senders with credit 0: {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 3 took more than 2 minutes.")

def query4(db):
    try:
        start_time = time.time()
        #can't accept maxtime input -> typeerror 
        #result = db.senders.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}}, maxTimeMS=120000)
        #this should fix it below
        result = db.senders.update_many({"credit": {"$lt": 100}}, {"$mul": {"credit": 2}})
        end_time = time.time()
        print(f"Updated {result.modified_count} senders' credits.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 4 took more than 2 minutes.")

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

import pymongo
import sys
import time

def connect_to_mongodb(port):
    """
    Connects to MongoDB server.

    Parameters:
        port (int): The port number on which MongoDB server is running.

    Returns:
        pymongo.MongoClient: A MongoClient instance representing the connection to MongoDB.
    """
    try:
        client = pymongo.MongoClient(f"mongodb://localhost:{port}/")
        print("Connected successfully to MongoDB")
        return client
    except pymongo.errors.ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        sys.exit(1)

def query1(db):
    """
    Q1: Return the number of messages that have “ant” in their text.

    Parameters:
        db (pymongo.database.Database): The MongoDB database to query.
    """
    try:
        start_time = time.time()
        count = db.messages.count_documents({"text": {"$regex": "ant"}}, maxTimeMS=120000) # Case sensitive
        end_time = time.time()
        print(f"Number of messages containing 'ant': {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 1 took more than 2 minutes.")

def query2(db):
    """
    Q2: Find the nick name/phone number of the sender who has sent the greatest number of messages. Return the nick name/phone number and the number of messages sent by that sender. You do not need to return the senders name or credit.

    Parameters:
        db (pymongo.database.Database): The MongoDB database to query.
    """
    try:
        start_time = time.time()
        pipeline = [
            {"$group": {"_id": "$sender_info.sender_id", "totalMessages": {"$sum": 1}}},
            {"$sort": {"totalMessages": -1}},
            {"$limit": 1}
        ]
        result = list(db.messages.aggregate(pipeline, maxTimeMS=120000))
        end_time = time.time()
        if result:
            print(f"Sender with the most messages: {result[0]['_id']} (Messages sent: {result[0]['totalMessages']})")
        else:
            print("No data found.")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 2 took more than 2 minutes.")

def query3(db):
    """
    Q3: Return the number of messages where the sender’s credit is 0.

    Parameters:
        db (pymongo.database.Database): The MongoDB database to query.
    """
    try:
        start_time = time.time()
        count = db.messages.count_documents({"sender_info.credit": 0}, maxTimeMS=120000)
        end_time = time.time()
        print(f"Number of messages from senders with credit 0: {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query 3 took more than 2 minutes.")

def query4(db):
    """
    Q4: Double the credit of all senders whose credit is less than 100.

    Parameters:
        db (pymongo.database.Database): The MongoDB database to query.
    """
    try:
        start_time = time.time()
        db.messages.update_many({"sender_info.credit": {"$lt": 100}}, {"$mul": {"sender_info.credit": 2}})
        end_time = time.time()
        print(f"Double credit of senders that have credit less than 100: {(end_time - start_time) * 1000} milliseconds")

    except pymongo.errors.ExecutionTimeout:
        print("Query 4 took more than 2 minutes.")

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

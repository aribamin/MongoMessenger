# This isn't mentioned in the instructions but the deliverables says we will need one :/

import pymongo
import json
import sys
import time

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 task2_queries.py <port_number>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
    except ValueError:
        print("Port must be an integer.")
        sys.exit(1)

    # Connect to MongoDB
    client = pymongo.MongoClient('localhost', port)

    # Access the MP2Embd database and messages collection
    db = client["MP2Embd"]
    messagesCol = db["messages"]
    sendersCol = db["senders"]

    # Q1: Number of messages that have "ant" in their text
    print("Q1: Number of messages that have 'ant' in their text")
    start_time = time.time()
    try:
        count = messagesCol.count_documents({"text": {"$regex": "ant"}})
        end_time = time.time()
        print(f"Number of messages with 'ant' in text: {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query took more than two minutes to execute.")

    # Q2: Nickname/phone number of sender who has sent the greatest number of messages
    print("\nQ2: Nickname/phone number of sender with most messages")
    start_time = time.time()
    try:
        result = messagesCol.aggregate([
            {"$group": {"_id": "$sender", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ])
        end_time = time.time()
        for doc in result:
            print(f"Sender: {doc['_id']}, Number of messages sent: {doc['count']}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query took more than two minutes to execute.")

    # Q3: Number of messages where sender's credit is 0
    print("\nQ3: Number of messages where sender's credit is 0")
    start_time = time.time()
    try:
        count = messagesCol.count_documents({"sender_info.credit": 0})
        end_time = time.time()
        print(f"Number of messages where sender's credit is 0: {count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query took more than two minutes to execute.")

    # Q4: Double the credit of all senders whose credit is less than 100
    print("\nQ4: Double the credit of all senders whose credit is less than 100")
    start_time = time.time()
    try:
        result = sendersCol.update_many(
            {"credit": {"$lt": 100}},
            {"$mul": {"credit": 2}}
        )
        end_time = time.time()
        print(f"Number of senders whose credit was doubled: {result.modified_count}")
        print(f"Time taken: {(end_time - start_time) * 1000} milliseconds")
    except pymongo.errors.ExecutionTimeout:
        print("Query took more than two minutes to execute.")

if __name__ == "__main__":
    main()

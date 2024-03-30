# must take a json file in the current directory and constructs a MongoDB collection
# must take a port number under which the MongoDB server is running as a command line argument
# must connect to the mongod server and will create a database named MP2Embd (if it does not exist)

# implement step 1 and 2?

import pymongo
import json
import sys
import time

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 task2_build.py <port_number>")
        sys.exit(1)
    
    try:
        port = int(sys.argv[1])
    except ValueError:
        print("Port must be an integer.")
        sys.exit(1)

    # Connect to MongoDB
    client = pymongo.MongoClient('localhost', port)

    # Create or access the MP2Embd database
    db = client["MP2Embd"]

    # Drop messages collection if it exists and create a new one
    db.messages.drop()
    messages_col = db["messages"]

    # Load sender information into memory
    with open('senders.json', 'r') as sender_file:
        senders = json.load(sender_file)

    # Process messages.json in batches and embed sender information
    batch_size = 10000
    start_time = time.time()
    with open('messages.json', 'r') as message_file:
        batch = []
        for line in message_file:
            data = json.loads(line)
            sender_info = next((sender for sender in senders if sender["sender_id"] == data["sender"]), None)
            if sender_info:
                data["sender_info"] = sender_info
                batch.append(data)
            if len(batch) == batch_size:
                messages_col.insert_many(batch)
                batch = []

        # Insert remaining documents
        if batch:
            messages_col.insert_many(batch)

    end_time = time.time()
    print(f"Time taken to read data and create collection: {end_time - start_time} seconds")

if __name__ == "__main__":
    main()

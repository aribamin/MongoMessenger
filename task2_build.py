import pymongo
from pymongo import MongoClient
import sys
import json
import time

def load_senders(filepath):
    with open(filepath, 'r') as file:
        senders = json.load(file)
    sender_lookup = {sender['sender_id']: sender for sender in senders}
    return sender_lookup

def embed_sender_info(sender_lookup, message):
    sender_info = sender_lookup.get(message['sender'])
    if sender_info:
        message['sender_info'] = sender_info
    return message

def insert_messages_batch(messagesCol, batch):
    if batch:  # Check if the batch is not empty
        messagesCol.insert_many(batch)

def process_messages(filepath, messagesCol, sender_lookup, batch_size=5000):
    batch = []
    with open(filepath, 'r') as file:
        for line in file:
            try:
                message = json.loads(line.strip())
                enriched_message = embed_sender_info(sender_lookup, message)
                batch.append(enriched_message)
                if len(batch) >= batch_size:
                    insert_messages_batch(messagesCol, batch)
                    batch = []
            except json.JSONDecodeError:
                continue
    # Insert any remaining messages
    insert_messages_batch(messagesCol, batch)

def main(db_port):
    client = MongoClient('localhost', db_port)
    db = client["MP2Embd"]
    messagesCol = db["messages"]
    messagesCol.drop()  # Drop existing collection if it exists

    start_time = time.time()
    
    sender_lookup = load_senders('senders.json')
    process_messages('messages.json', messagesCol, sender_lookup)
    
    end_time = time.time()
    print(f"Total time to read data and create the collection: {end_time - start_time} seconds")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 task2_build.py <database_port>")
        sys.exit(1)
    
    db_port = int(sys.argv[1])
    main(db_port)
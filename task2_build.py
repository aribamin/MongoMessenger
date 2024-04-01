import pymongo
from pymongo import MongoClient
import sys
import json
import time

def load_senders(filepath):
    """
    Loads sender information from a JSON file.

    Parameters:
        filepath (str): The path to the JSON file containing sender information.

    Returns:
        dict: A dictionary mapping sender IDs to sender information.
    """
    with open(filepath, 'r') as file:
        senders = json.load(file)
    return {sender['sender_id']: sender for sender in senders}

def embed_sender_info(sender_lookup, message):
    """
    Embeds sender information into a message.

    Parameters:
        sender_lookup (dict): A dictionary mapping sender IDs to sender information.
        message (dict): The message to embed sender information into.

    Returns:
        dict: The message with sender information embedded.
    """
    sender_info = sender_lookup.get(message['sender'])
    if sender_info:
        message['sender_info'] = sender_info
    return message

def insert_messages_batch(messagesCol, batch):
    """
    Inserts a batch of messages into the messages collection.

    Parameters:
        messagesCol (pymongo.collection.Collection): The collection to insert messages into.
        batch (list): A list of messages to insert.
    """
    if batch:  # Check if the batch is not empty
        messagesCol.insert_many(batch)

def process_messages(filepath, messagesCol, sender_lookup, batch_size=5000):
    """
    Processes messages from a file and inserts them into the messages collection.

    Parameters:
        filepath (str): The path to the file containing messages.
        messagesCol (pymongo.collection.Collection): The collection to insert messages into.
        sender_lookup (dict): A dictionary mapping sender IDs to sender information.
        batch_size (int): The size of each batch of messages to insert.
    """
    batch = []
    with open(filepath, 'r') as file:
        while True:
            line = file.readline()
            if not line:
                break  # Exit the loop if no more lines
            
            # Find the start and end of a valid JSON entry
            itemBeginning = line.find('{')
            itemEnd = line.rfind('}')
            if itemBeginning == -1 or itemEnd == -1:
                continue  # Skip line if no valid entry
            
            # Extract and parse the valid JSON string
            line = line[itemBeginning : itemEnd + 1]
            try:
                message = json.loads(line)
                enriched_message = embed_sender_info(sender_lookup, message)
                batch.append(enriched_message)
                if len(batch) >= batch_size:
                    insert_messages_batch(messagesCol, batch)
                    batch = []  # Reset batch after inserting
            except json.JSONDecodeError:
                continue

    # Insert any remaining messages
    insert_messages_batch(messagesCol, batch)

def main(db_port):
    """
    Main function to build the MongoDB collection.

    Parameters:
        db_port (int): The port number of the MongoDB database.
    """
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
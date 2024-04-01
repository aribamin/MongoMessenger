import pymongo 
from pymongo import MongoClient
import sys
import json
import time

# ------------------------- Error checking ----------------------------------
if len(sys.argv) != 2:
    print("Usage: python3 task2_build.py <port>")
    sys.exit(1)

try:
    DB_PORT = int(sys.argv[1])
except:
    print("Port must be an integer")
    sys.exit(1)
# ---------------------------------------------------------------------------

# -------------------- Open database and collections ------------------------
# Create a client and connect to db
client = MongoClient('localhost', DB_PORT)
db = client["MP2Embd"]

# Open the messages collection, drop if it exists
messagesCol = db["messages"]
messagesCol.drop()

# Open the senders collection
sendersCol = db["senders"]

print("Database connection established.")

# -------------------------- Read senders data -----------------------------
senders_data = {}
try:
    with open('senders.json', 'r') as senders_file:
        senders_data = json.load(senders_file)
        print("Sender data loaded successfully.")
except Exception as e:
    print(f"Error loading senders data: {e}")
    sys.exit(1)
# ---------------------------------------------------------------------------

# ----------------------- Process messages data ----------------------------
batch_size = 10000  # Adjust batch size according to system memory
messages_batch = []

start_time = time.time()

try:
    with open('messages.json', 'r') as messages_file:
        for line in messages_file:
            message = json.loads(line)

            # Embed sender info
            sender_id = message['sender']
            if sender_id in senders_data:
                sender_info = senders_data[sender_id]
                message['sender_info'] = sender_info

            messages_batch.append(message)

            # Insert batch into database when it reaches batch size
            if len(messages_batch) == batch_size:
                messagesCol.insert_many(messages_batch)
                messages_batch = []

    # Insert any remaining messages
    if messages_batch:
        messagesCol.insert_many(messages_batch)

    end_time = time.time()
    print(f"Messages inserted successfully. Time taken: {end_time - start_time} seconds.")

except Exception as e:
    print(f"Error processing messages data: {e}")
    sys.exit(1)
# ---------------------------------------------------------------------------

# ------------------------- Index creation ---------------------------------
# No index creation in this step
# ---------------------------------------------------------------------------

# Close database connection
client.close()
print("Database connection closed.")
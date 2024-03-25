'''
This program opens up a MongoDB database on a specific port number give through a command line arguments, and
inserts data into the database.
- This affects the senders and messages collection, which will have all their documents dropped and re-entered

- REQUIRES messages.json AND senders.json to be present in the current directory (too large for github)
The messages.json file will be read and all of its documents will be uploaded to the messages collection
The senders.json file will be read and all of its documents will be uploaded to the senders collection

Note: MUST BE ABLE TO BUILD THE DATABASE IN UNDER 5 MINUTES

To run the code:
1. Start the database by running something like 'mongod --port 27012 --dbpath ~/mongodb_data_folder &' on a DIFFERENT TERMINAL- this will make the server run in background
    - You can change the port number if you want
2. Run this script using 'python3 task1_build.py 27012'
    - If you changed the port number, adjust it here
'''

from pymongo import MongoClient
import sys
import ijson

# ------------------------- Error checking ----------------------------------
if len(sys.argv) != 2:
    print("Usage: python3 main.py <database_name>")
    sys.exit(1)

try:
    DB_PORT = int(sys.argv[1])
except:
    print("Port must be in integer format")
    sys.exit(1)
# ---------------------------------------------------------------------------

# --------------------------- Open database ---------------------------------
# Create a client and connect to db
client = MongoClient('localhost', DB_PORT)
db = client["MP2Norm"]

# ---------------------- Messages collection --------------------------------
# Open the messages collection, drop if it exists
messagesCol = db["messages"]
messagesCol.drop() # drop if exists


# Now load everything from messages.json into the messages mongo collection
# Use ijson to parse the file as an input stream rather than loading the entire file
# https://pythonspeed.com/articles/json-memory-streaming/
loadedDocuments = []
with open('messages.json', 'rb') as file:
    for document in ijson.items(file, 'item'):
        loadedDocuments.append(document)

        # If 5000 messages loaded, insert into the database and clear from memory
        if len(loadedDocuments) == 5000:
            messagesCol.insert_many(loadedDocuments)
            loadedDocuments = []

# Insert any leftover documents into the database
if len(loadedDocuments) > 0:
    messagesCol.insert_many(loadedDocuments)
    loadedDocuments = []

# ---------------------------------------------------------------------------

# ----------------------- Senders collection --------------------------------
# Open the senders collection, drop if it exists
sendersCol = db["senders"]
sendersCol.drop() # drop if exists

# Load the messages and insert into the database
# We aren't concerned about the file being too large this time
loadedDocuments = []
with open('senders.json', 'rb') as file:
    for document in ijson.items(file, 'item'):
        loadedDocuments.append(document)

sendersCol.insert_many(loadedDocuments)


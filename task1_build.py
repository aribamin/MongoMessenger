'''
This program opens up a MongoDB database on a specific port number give through a command line arguments, and
inserts data into the database.
- This affects the senders and messages collection, which will have all their documents dropped and re-entered

- REQUIRES messages.json AND senders.json to be present in the current directory (too large for github)
The messages.json file will be read and all of its documents will be uploaded to the messages collection
The senders.json file will be read and all of its documents will be uploaded to the senders collection

Note: MUST BE ABLE TO BUILD THE DATABASE IN UNDER 5 MINUTES

To run the code:
(0. If you haven't, run 'mkdir ~/mongodb_data_folder')
1. Start the database by running something like 'mongod --port 54321 --dbpath ~/mongodb_data_folder &' - this will make the server run in background (should do on different terminal)
    - You can change the port number if you want
2. Run this script using 'python3 task1_build.py 54321'
    - If you changed the port number, adjust it here
'''

from pymongo import MongoClient
import sys
import json
import time

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

# -------------------- Open database and collections ------------------------
# Create a client and connect to db
client = MongoClient('localhost', DB_PORT)
db = client["MP2Norm"]

# Open the messages collection, drop if it exists
messagesCol = db["messages"]
messagesCol.drop()

# Open the senders collection, drop if it exists
sendersCol = db["senders"]
sendersCol.drop()

# START THE TIMER (by getting the current time)
startTime = time.time()
# ---------------------- Messages collection --------------------------------

# messages.json will have only one item per line, so we can iterate line by line
loadedDocuments = []
file = open('messages.json', 'r')
while True:
    line = file.readline()

    # Stop reading if line is empty
    if not line:
        break
    
    # Find first '{' and last '}' to find the entry
    itemBeginning = line.find('{')
    itemEnd = line.rfind('}')

    # Skip line if there is no valid entry
    if itemBeginning == -1 or itemEnd == -1:
        continue

    # Entry has been deemed valid, so append it to the list of documents
    line = line[itemBeginning : itemEnd + 1]
    loadedDocuments.append(json.loads(line))

    # If 5000 messages loaded, insert into the database and clear from memory
    if len(loadedDocuments) == 5000:
        messagesCol.insert_many(loadedDocuments)
        loadedDocuments = []

# Insert any leftover documents into the database
if len(loadedDocuments) > 0:
    messagesCol.insert_many(loadedDocuments)
    loadedDocuments = []


file.close()

# ---------------------------------------------------------------------------

# ----------------------- Senders collection --------------------------------

# senders.json does not have a guaranteed formatting, so load the entire file and parse
loadedDocuments = []
file = open('senders.json', 'r')
string = file.read()
file.close()

while True:
    itemBeginning = string.find('{')
    itemEnd = string.find('}')

    if itemBeginning == -1 or itemEnd == -1:
        break
    
    # Append the found document into the array and remove it from the data string
    loadedDocuments.append(json.loads(string[itemBeginning : itemEnd + 1]))
    string = string[itemEnd + 1 :]

# Insert the json documents now that we've parsed through them
#print(loadedDocuments)
sendersCol.insert_many(loadedDocuments)

# ---------------------------------------------------------------------------

endTime = time.time()
print(f"Total time elapsed to read and populate into messages and senders: {endTime - startTime} seconds")

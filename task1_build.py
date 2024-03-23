# must take a json file in the current directory and constructs MongoDB collections
# must take a port number under which the MongoDB server is running as a command line argument
# must connect to the mongod server and will create a database named MP2Norm (if it does not exist)

# and more

# should implement step 1 and 2 of task 1 it seems

# Note: MUST BE ABLE TO BUILD THE DATABASE IN UNDER 5 MINUTES or else

# if you keep getting connection refused (didn't work for me yet though): https://stackoverflow.com/questions/7744147/pymongo-keeps-refusing-the-connection-at-27017

'''
To run the code:
1. Create the database by running something like 'mongod --port 27012 --dbpath ~/mongodb_data_folder &' - this will make it run in background
    - You can change the port number if you want
2. Run this script using 'python3 task1_build.py 27012'
    - If you changed the port number, adjust it here
'''

from pymongo import MongoClient
#import pymongo
import sys

if len(sys.argv) != 2:
    print("Usage: python3 main.py <database_name>")
    sys.exit(1)

try:
    DB_PORT = int(sys.argv[1])
except:
    print("Port must be in integer format")
    sys.exit(1)

# Create a client and connect to db
client = MongoClient('localhost', DB_PORT)
db = client["MP2Norm"] # Note a DATABASE DOES NOT GET CREATED UNTIL IT GETS CONTENT
print("Database opened successfully")

#print(db.list_collection_names())

# Open the messages collection, drop if it exists
messagesCol = db["messages"]
messagesCol.insert_one({"test": "please"})
print("insert done!")
print(db.list_collection_names())

# print("collection is working?")
# messagesCol.drop()
# print("Messages collection dropped")

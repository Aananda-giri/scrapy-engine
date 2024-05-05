import re
import os
import time
import pymongo
from pymongo.server_api import ServerApi

from dotenv import load_dotenv
load_dotenv()

# Creating mangodb
from pymongo import MongoClient


class MongoDBHandler:
    def __init__(self, collection_name="pdf_collection", db_name="pdf_engine"):
        self.db = MongoDBHandler.get_database(db_name=db_name)
        self.collection = self.db[collection_name]
        self.db_name = db_name
        # # suitable for equality and range queries 
        # self.collection.create_index([("children_list", pymongo.ASCENDING)])
        
        # suitable for full  text search
        # Create a text index on 'children_list' field
        # self.collection.create_index([("children_list", pymongo.TEXT)])
        
        # Create a unique index on the 'id' field
        # self.collection.create_index([("id", pymongo.ASCENDING)], unique=True)

    @staticmethod
    def get_database(db_name='pdf_engine'):
        # Provide the mongodb atlas url to connect python to mongodb using pymongo
        # CONNECTION_STRING = "mongodb+srv://user:pass@cluster.mongodb.net/myFirstDatabase"
        import os
        uri = f"mongodb+srv://{os.environ.get('mongo_username')}:{os.environ.get('mongo_password')}@scrapy-engine.cnaygdb.mongodb.net/?retryWrites=true&w=majority&appName=scrapy-engine"
        # uri = f"mongodb+srv://{os.environ.get('user_heroku')}:{os.environ.get('pass_heroku')}@cluster0.dgeujbs.mongodb.net/?retryWrites=true&w=majority"
        
        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(uri, server_api=ServerApi('1'))
        
        # Create the database for our example (we will use the same database throughout the tutorial
        return client[db_name]
        
    def insert_one(self, data, collection_name=None, add_timestamp=False):
        """Insert a single document into the collection."""
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        
        if add_timestamp:
            if type(data) == dict:
                data['timestamp'] = time.time()
            else:
                data = {'data': data, 'timestamp': time.time()}
        collection.insert_one(data)

    def get_items_before_timestamp(self, timestamp, collection_name=None):
        """Retrieve all entries from the collection."""
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        result = collection.find({"timestamp": {"$lt": timestamp}})
        return list(result)
    
    def insert_many(self, data, collection_name=None):
        """Insert multiple documents into the collection."""
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        if type(data) == list:
            collection.insert_many(data)
        elif type(data) == dict:
            # Prepare a list of documents with custom _id values
            documents_to_insert = [
                {'_id': doc_id, 'data': doc_data} for doc_id, doc_data in data.items()
            ]
            
            # Insert documents into the collection
            result = collection.insert_many(documents_to_insert)
            
            # Print the inserted document ids
            print(f"Inserted documents with ids: {result.inserted_ids[:10]}")

            # Close the MongoDB connection
            client.close()
    def exists(self, field, vlaue, collection_name=None):
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        count = collection.count_documents({field: vlaue})
        return count > 0
        # return result
        return True if list(result) else False
    def delete_all(self, collection_name=None):
        """Delete all documents in the collection."""
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        collection.delete_many({})

    def delete_one(self, value, field="id", collection_name=None):
        """Delete a document by its ID."""
        if collection_name:
            self.db[collection_name].delete_one({"id": value})
        else:
            self.collection.delete_one({"id": value})
    
    # async def search(self, query):
    #     """Perform an asynchronous text search based on the given query."""
    #     import asyncio
    #     from motor.motor_asyncio import AsyncIOMotorClient
    #     regex_pattern = f"{' '.join(query.split())}"
    #     query_result = await self.collection.find({'data.children_list': {'$regex': regex_pattern}}).to_list(length=None)
    #     return [result['data'] for result in query_result]
    #     #     async for result in query_result:
    #     #         yield result['data']
    # def search(self, query):
    #     """Perform a text search based on the given query."""
        
    #     # Define a regex pattern with spaces between elements
    #     regex_pattern = f"{' '.join(query.split())}"
    #     # Perform the search using $regex
    #     query_result = self.collection.find({'data.children_list': {'$regex': regex_pattern}})
        
    #     # Use list comprehension to collect results in a list and return it
    #     return [result['data'] for result in query_result]
    

    def search(self, query, collection_name=None):
        """Perform a case-insensitive text search based on the given query."""
        
        if not collection_name:
            collection = self.collection
        else:
            collection = self.db[collection_name]
        # Define a regex pattern with spaces between elements
        regex_pattern = f"{' '.join(query.split())}"
        # Perform the case-insensitive search using $regex with re.IGNORECASE
        # $options parameter is set to 'i' (case-insensitive)
        query_result = collection.find({'data.children_list': {'$regex': regex_pattern, '$options': 'i'}})
        # Yield results as they are found
        for result in query_result:
            yield result['data']
        # result = collection.find({"$text": {"$search": query}})
        # 12 seconds
        # result = collection.find({
        #     "children_list": {
        #         "$elemMatch": {
        #             "$regex": query,
        #             "$options": "i"  # Case-insensitive
        #         }
        #     }
        # })
        # result = collection.find({
        #     "$or": [
        #         {"children_list": {"$elemMatch": {"$regex": query, "$options": "i"}}},
        #         {"data": {"$regex": query, "$options": "i"}}
        #     ]
        # })
        # result = collection.find({
        #     "children_list": {
        #         "$regex": query,
        #         "$options": "i"  # Case-insensitive
        #     }
        # })
        # return list(result)
    def get_by_ids(self, id_list, collection_name=None):
        """Retrieve documents by their IDs."""
        if collection_name:
            collection = self.db[collection_name]
        else:
            collection = self.collection

        # from bson.objectid import ObjectId
        results = collection.find({"_id": {"$in": id_list}})
        return [result['data'] for result in list(results) if 'data' in result]

    def count_entries(self, collection_name=None):
        """
        Counts number of entries int the database
        """
        if collection_name:
            collection = self.db[collection_name]
        else:
            collection = self.collection

        # Count the number of documents in the collection
        count = collection.count_documents({})
        # Print the count
        print(f"Number of entries in the collection: {count}")
        return count
        
        
    def get_all_entries(self, collection_name=None):
        """Retrieve all entries from the collection."""
        if not collection_name:
            collectioin = self.collection
        else:
            collection = self.db[collection_name]
        result = collection.find()
        return list(result)
     
    

# Example usage:
if __name__ == "__main__":
    # Instantiate the MongoDBHandler with the collection name
    mongo_handler = MongoDBHandler(collection_name = "pdf_collection", db_name="pdf_engine")
    mongo_handler.count_entries()
    # --------------
    # Insert Data
    # --------------
    # with open("/home/anon/weekly-projects/pdf_engine/ec2-upload/search/search_data.json",'r') as f:
    #     data = json.load(f)
    # mongo_handler.insert_many(data)
    # data_sample = {}
    # for n, d in enumerate(data):
    #     if n<10:
    #         data_sample[d] = data[d]
    # mongo_handler.insert_many(data_sample)

    # -----------------
    # Get all entries
    # -----------------
    # all_entries = mongo_handler.get_all_entries()
    # print("All Entries:", all_entries)

    # ------------------
    # Search
    # ------------------
    # start_time = time.time()
    # print("Results:")
    # import time
    # i=1
    # initial_time = time.time()
    # for result in mongo_handler.search("physics"):
    #         if i==1:
    #             start_time = time.time()
    #             i+=1
    #         print(result)
    # print(f"Search Results: \n time:{start_time - time.time()} \n total time: {time.time() - initial_time}")
    # # print(f"Search Results: {search_results} \n time:{start_time - time.time()}")
    # ------------------
    # Get by ids
    # ------------------
    ids = ["1zESpo4jn7Q3OQ9N2EimtzAE05D7eJAnw" ,"12aaGB6Bj0Xggvq7RdIXrqpkEdH5xIy1O"]
    results = mongo_handler.get_by_ids(ids)
    print(results)
    import json
    with open('test_file.json', 'w') as file:
        json.dump(results, file)

    # # ------------------
    # # Delete one
    # # ------------------
    # mongo_handler.delete_one("data")
    
    # -------------------------------
    # Delete all (use with caution)
    # -------------------------------
    # mongo_handler.delete_all()

    # # -----------------
    # # Get all entries
    # # -----------------
    # all_entries = mongo_handler.get_all_entries()
    # print("All Entries:", all_entries)

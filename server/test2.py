import motor.motor_asyncio

uri = f"mongodb+srv://jokeleopedia:dnMnzGGKHqKChdx4Dqdf7dMYYAfyQdPL@scrapy-engine.cnaygdb.mongodb.net/?retryWrites=true&w=majority&appName=scrapy-engine"
client = motor.motor_asyncio.AsyncIOMotorClient(uri)

db = client["test_database"]

collection = db.test_collection

# inserting a document
async def do_insert():
    document = {"key": "value"}
    result = await db.test_collection.insert_one(document)
    print("result %s" % repr(result.inserted_id))


import asyncio
loop = client.get_io_loop()
loop.run_until_complete(do_insert())

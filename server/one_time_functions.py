# ------------------------------------------------------------------------------
# Remove url for url in local_mongo['to_crawl'] if url in oscar_bloom_filter
# ------------------------------------------------------------------------------
from mongo import Mongo
local_mongo = Mongo(local=True)


from bloom import BloomFilterThread

# load oscar dataset bloom filter (contains urls that are already crawled by oscar_2201 dataset from huggingface)
oscar_bloom_filter = BloomFilterThread(save_file='oscar_bloom_filter.pkl')

# Find all documents with 'status' = 'to_crawl'
documents_to_check = local_mongo.collection.find({'status': 'to_crawl'})

count = 0
deleted = 0
# Iterate over the documents and remove if the URL is in oscar_bloom_filter
for doc in documents_to_check:
    count += 1
    # print(doc)
    # if count >2:
    #     break
    url = doc['url']
    if url in oscar_bloom_filter:
        local_mongo.collection.delete_one({'_id': doc['_id']})
        # print(f'removed')
    if count % 10000 == 0:
        print(f'{count}: deleted: {deleted}')    

print("Removal process completed.")



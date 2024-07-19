from mongo import Mongo

class Backup():
    '''
    * function to Save `urls data` from local mongoDB to a csv file
    * function to load `urls data` from a csv file to local mongoDB

    # note:
    structure of data inside local mongo:
    
    {
        'url': entry['url'],
        'status': 'to_crawl',
        'timestamp': entry['timestamp']
    },
    '''

    def __init__(self, csv_filename="local_mongo.csv"):
local_mongo = Mongo(local=True)
batch_size = 20000
offset = 0
csv_filename = csv_filename
    
    def decorated_print(self, the_text):
        '''
        * function to decorate the text
        '''
        print('-'*(len(the_text) + 16))
        print(f'\t {the_text}')
        print('-'*(len(the_text) + 16), end='\n\n') 

    def save_to_csv(self, delete_from_local_mongo=True):
        '''
        * function to Save `urls data` from local mongoDB to a csv file
        '''
        
        while True:
            # Get 20000 urls a t a time
            # .sort('_id', 1) -> sort by id in ascending order
            entries = list(local_mongo.collection.find().sort('_id', 1).skip(offset).limit(batch_size))
            
        
            if not entries:
                decorated_print("\t saved all data to {csv_filename}")
                break
        
            # Save to csv
            file_exists = os.path.isfile(csv_filename)
            with open(csv_filename, 'a') as f:
                writer = csv.DictWriter(f, fieldnames=['url', 'status', 'timestamp'])
                if not file_exists:
                    writer.writeheader()
                for entry in entries:
                    writer.writerow(entry)
            
            if delete_from_local_mongo:
                # Delete from local mongo
                local_mongo.delete_entries(entries)

            # Increase the offset for the next batch
            offset += batch_size

            print(f"\t saved {offset} entries to {csv_filename}")
            break

if __name__ == '__main__':
    backup = Backup()
    backup.save_to_csv(delete_from_local_mongo=False)
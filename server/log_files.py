# # Experiment-1: save to and load from  pickle file
# import pickle
# import time

# # Save to pickle
# with open("crawled_data/test2.pkl", 'wb') as f:
    
#     pickle.dump('hi there', f)

# # load pickle file
# with open('crawled_data/test1.pkl', 'rb') as f:
#     print(pickle.load(f))

#  # Experiment-2: what happens when trying to load a file that is still being written to
# import time
# import pickle
# import threading

# def producer(sleep_time, file_name):
#     with open(file_name, 'wb') as f:
#         for i in range(10):
#             time.sleep(sleep_time)
#             pickle.dump(i, f)
#             print(f'Produced {i}')

# def consumer(sleep_time, file_name):
#     with open(file_name, 'rb') as f:
#         while True:
#             try:
#                 print(f'Consumed {pickle.load(f)}')
#                 time.sleep(sleep_time)
#             except EOFError:
#                 print('EOFError')
#                 time.sleep(1)

# producer_thread = threading.Thread(target=producer, args=(1, 'crawled_data/test1.pkl'))
# consumer_thread = threading.Thread(target=consumer, args=(2, 'crawled_data/test1.pkl'))

# producer_thread.start()
# consumer_thread.start()
# producer_thread.join()


# Experiment-3: Do not open files in write mode in multiple threads


# Experiment-4: Create lock while opening in write mode
import filelock
import time
import pickle
import threading

# def producer(sleep_time, file_name):
#     lock = filelock.FileLock(self.file_name)
#     try:
#         with lock.acquire(timeout=10):
#             with open(file_name, 'wb') as f:
#                 for i in range(10):
#                     time.sleep(sleep_time)
#                     pickle.dump(i, f)
#                     print(f'Produced {i}')
#     except lock.Timeout:
#         print("init_cache timeout", traceback.format_exc())

# def consumer(sleep_time, file_name):
#     with open(file_name, 'rb') as f:
#         while True:
#             try:
#                 print(f'Consumed {pickle.load(f)}')
#                 time.sleep(sleep_time)
#             except EOFError:
#                 print('EOFError')
#                 time.sleep(1)

# producer_thread = threading.Thread(target=producer, args=(1, 'crawled_data/test1.pkl'))
# consumer_thread = threading.Thread(target=consumer, args=(2, 'crawled_data/test1.pkl'))

# producer_thread.start()
# consumer_thread.start()
# producer_thread.join()


def producer(sleep_time, file_name, lock):
    for i in range(100):
        time.sleep(sleep_time)
        with lock:
            with open(file_name, 'ab') as f:
                pickle.dump(i, f)
                print(f'Produced {i}')

def consumer(sleep_time, file_name, lock):
    while True:
            with open(file_name, 'rb') as f:
                try:
                    print(f'Consumed {pickle.load(f)}')
                    time.sleep(sleep_time)
                except EOFError:
                    print('EOFError')
                    time.sleep(1)

lock = threading.Lock()
producer_thread = threading.Thread(target=producer, args=(1, 'crawled_data/test1.pkl', lock))
consumer_thread = threading.Thread(target=consumer, args=(2, 'crawled_data/test1.pkl', lock))

producer_thread.start()
consumer_thread.start()
producer_thread.join()
consumer_thread.join()
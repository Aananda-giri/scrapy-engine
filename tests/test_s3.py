from s3_v2 import get_s3_client, Ec2Functions
import unittest

# @classmethod
# def setUpClass(self):
#     self.ec2_functions = Ec2Functions()

import concurrent.futures
import os
import time

class TestS3Functions(unittest.TestCase):
    def setUp(self):
        # self.s3_client = get_s3_client()
        self.ec2_functions = Ec2Functions()
    
    def test_upload_file(self):
        # test uploading a file to s3
        
        # write a text file 'test_1.txt'
        with open('test_1.txt', 'w') as file:
            file.write('hello world')
        
        # upload to s3
        self.ec2_functions.upload_file(file_path='test_1.txt', bucket_name='1b-bucket', object_key='test_1.txt')
        
        # make sure the file is in the bucket
        file_keys = self.ec2_functions.list_files('1b-bucket')
        self.assertIn('test_1.txt', file_keys)

    def test_delete_file(self):
        # delete the file
        self.ec2_functions.delete_file('1b-bucket','test.txt')

        file_keys = self.ec2_functions.list_files('1b-bucket')

        # make sure the file is not in the bucket
        self.assertNotIn('test.txt', file_keys)
    
    def test_concurrent_upload(self):
        print(f' concurrent upload test')
        # Create 1000 dummy files
        for i in range(1000):
            with open(f'test_{i}.txt', 'w') as file:
                file.write('hello world')

        # Calculate the start time
        start_time = time.time()

        # Function to upload a single file
        def upload_file(file_path):
            self.ec2_functions.upload_file(file_path=file_path, bucket_name='1b-bucket', object_key=file_path)

        # Upload the files concurrently
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for file_path in os.listdir():
                if file_path.startswith('test_'):
                    futures.append(executor.submit(upload_file, file_path))

            # Wait for all uploads to complete
            concurrent.futures.wait(futures)

        # Calculate the end time
        end_time = time.time()

        # Calculate the average time taken per file upload
        total_time = end_time - start_time
        average_time = total_time / 1000

        # Print the average time taken per file upload
        print(f'Average time taken per file upload: {average_time} seconds')

        # Make sure all files are in the bucket
        ec2_instance_file_keys = self.ec2_functions.list_files('1b-bucket')
        uploaded_files = ['test_{i}.txt' for i in range(1000)]
        self.assertIn(uploaded_files, ec2_instance_file_keys)

        # Remove the dummy files
        for file_path in uploaded_files:
            os.remove(file_path)
        
        # Remove dummy files from s3
        for file_path in uploaded_files:
            self.ec2_functions.delete_file('1b-bucket', file_path)
        
        print('remaining files in s3:', self.ec2_functions.list_files('1b-bucket'))
if __name__ == '__main__':
    unittest.main()



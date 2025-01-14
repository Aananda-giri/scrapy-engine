from dotenv import load_dotenv
import os

load_dotenv()
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
print(f'keys available: {AWS_ACCESS_KEY != None and AWS_SECRET_KEY !=None}')

AWS_REGION="us-east-1"
s3_bucket_name = "1b-bucket"
local_file_path = "test.txt"
name_for_s3 = "test.txt"

import boto3
import os, sys
import boto3
import asyncio

class Ec2Functions:
        @staticmethod
        def get_s3_client():
            # print("in main")
            s3_client=boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY,
                region_name=AWS_REGION
            )
            return s3_client
            # response = s3_client.list_buckets()
            # response = s3_client.upload_file(local_file_path, s3_bucket_name, name_for_s3)
            # print(f"response: {response}")
        async def _upload_file_to_s3(file_path, bucket_name, object_key):
            # Function to upload a file to S3 and return the link
            '''
            object key is the path to the file in the bucket
            '''
            
            s3_client = Ec2Functions.get_s3_client()# boto3.client('s3')
            try:
                s3_client.upload_file(file_path, bucket_name, object_key)
                # object_url = f"https://{bucket}.s3.{region_name}.amazonaws>
                object_url = s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': bucket_name, 'Key': object_key},
                    ExpiresIn=172800
                )
                return object_url
            except Exception as e:
                print(f"Error uploading file '{file_path}' to S3: {e}")
                return None
        
        @staticmethod
        def upload_file(file_path, bucket_name, object_key):
            url = asyncio.run(Ec2Functions._upload_file_to_s3(file_path, bucket_name, object_key))
            return url
        
        @staticmethod
        def upload_folder(file_path, bucket_name):
            urls = asyncio.run(Ec2Functions._upload_folder_to_s3(file_path=file_path, bucket_name=bucket_name))
            return urls
        
        async def _upload_folder_to_s3(folder_path = None, bucket_name=None):
            # Upload folder contents to S3
            uploaded_files = []
            if not folder_path:
                folder_path = '/home/ubuntu/saneora/cogs/downloads'
            if not bucket_name:
                bucket_name = 'discord-bot'
            for root, dirs, files in os.walk(folder_path):
                for file_name in files:
                    file_path = os.path.join(root, file_name)
                    print(file_path)
                    object_key = os.path.relpath(file_path, folder_path)
                    object_url = await Ec2Functions.upload_file_to_s3(file_path, bucket_name, object_key)
                    if object_url:
                        uploaded_files.append(object_url)

            # Print the list of uploaded file links
            # for file_url in uploaded_files:
            # for file_url in uploaded_files:
            #     print(file_url)
            return uploaded_files

        @staticmethod
        def list_files(bucket_name=None):
            # List files in a bucket
            if not bucket_name:
                bucket_name = 'discord-bot'
            s3_client = Ec2Functions.get_s3_client()
            try:
                response = s3_client.list_objects(Bucket=bucket_name)
                file_keys = []  # similar to file paths
                if 'Contents' in response:
                    for file in response['Contents']:
                        '''
                        file
                        [{'Key': 'test.txt', 'LastModified': datetime.datetime(2025, 1, 7, 7, 7, 8, tzinfo=tzutc()), 'ETag': '"5eb63bbbe01eeed093cb22bb8f5acdc3"', 'Size': 11, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'aananda.giri', 'ID': '953e9521edc879c84ab34754fcdba11d75ccd7abf4b9dbd9b1fab460fa780781'}}]
                        '''
                        file_keys.append(file['Key'])
                    return file_keys
                else:
                    print('No files in the bucket')
                    return []
            except Exception as e:
                print(f"Error listing files in the bucket '{bucket_name}': {e}")
                return []

        @staticmethod
        def delete_file(bucket_name, object_key):
            # Delete a file from S3
            s3_client = Ec2Functions.get_s3_client()
            try:
                response = s3_client.delete_object(Bucket=bucket_name, Key=object_key)
                print(f"File '{object_key}' deleted from bucket '{bucket_name}'")
            except Exception as e:
                print(f"Error deleting file '{object_key}' from bucket '{bucket_name}': {e}")

        @staticmethod
        def download_file(bucket_name, object_key, file_path):
            # Download a file from s3
            s3_client = Ec2Functions.get_s3_client()
            try:
                s3_client.download_file(bucket_name, object_key, file_path)
                print(f"File '{object_key}' downloaded to '{file_path}'")
            except exception as e:
                print(f'Error downloding file {object_key} from bucket {bucket_name}: {e}')

if __name__ == "__main__":
    # take filename from command line
    if len(sys.argv) >1:
        file_path = sys.argv[1]
    else:
        file_path = None


    if not os.path.exists(file_path):
        print(f'file {file_path} does not exist')
        # create test file
        with open('test.txt', 'w') as file:
            file.write('hello world')
        
        # upload test file
        # await using asyncio
        url = upload_file('test.txt', '1b-bucket', 'test.txt')
        print(url)
    else:
        print(f'file {file_path} exists')
        
        # Check file type
        if os.path.isdir(file_path):
            print(f'{file_path} is a directory')
            # Upload folder
            urls = Ec2Functions.upload_folder(file_path=file_path, bucket_name='1b-bucket')
            
            for url in urls:
                print(url)
        else:
            print(f'{file_path} is a file')
            url = Ec2Functions.upload_file(file_path=file_path, bucket_name='1b-bucket', object_key=file_path.split('/')[-1])
            
            print(url)
    Ec2Functions.list_files('1b-bucket')
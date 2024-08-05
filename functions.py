import os, json
from urllib.parse import urlparse
def get_file_name(url, domain_name = None):
    if not domain_name:
        # Get domain name from url
        parsed_url = urlparse(url)
        domain_name = parsed_url.netloc   # https://www.example.com
    
    file_name = domain_name + '.json'
    index = 0
    remove_file_if_empty(file_name)
    
    while os.path.exists(file_name):
        file_name = domain_name + f'_{index}.json'
        index += 1
    
    remove_file_if_empty(file_name)
    
    return file_name, domain_name

    # print(domain_name)
    # return domain_name.split('.')[1]  # example

def get_one_start_url(domain_name=None):
    if not domain_name:
        # get start_url from 
        with open("news_start_urls.json",'r') as file:
            urls = json.load(file)

        # one_url = urls.pop()
        return urls[0]

def remove_file_if_empty(file_path):
    """Checks if a file is empty and removes it if it is.

    Args:
        file_path (str): The path to the file to check.
    
    Returns:
        bool: True if the file was empty and removed, False otherwise.
    """
    if os.path.exists(file_path) and os.path.getsize(file_path) == 0:
        try:
            os.remove(file_path)
            print(f"Removed empty file: {file_path}")
            return True
        except OSError as e:
            print(f"Error removing file: {e}")
            return False
    else:
        print(f"File is not empty or does not exist: {file_path}")
        return False



def count_lines_in_file(file_path):
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
        return sum(1 for line in file)

def count_lines_in_directory(directory='./'):
    ignore_folders = ['__pycache__', '.git', 'project.egg-info', 'archive', 'migrations']
    extensions_to_count = ['.py']
    total_lines = 0
    
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # ignore folder
            if True not in [ignore_folder in file_path for ignore_folder in ignore_folders]:
                # only count for `extensions_to_count`
                if True in [file_path.endswith(extension) for extension in extensions_to_count]:
                    try:
                        lines = count_lines_in_file(file_path)
                        print(f"{file_path}: {lines} lines")
                        total_lines += lines
                    except Exception as e:
                        print(f"Could not read {file_path}: {e}")
    return total_lines


# count_lines_in_directory()  # 6393

import langid
import re
import os, json, csv
import pandas as pd

import pybloom_live
from pathlib import Path
from urllib.parse import urlparse

def merge_crawled_json_files():
  '''
        * Crawled data is saved in .json files.
        * There are multiple .json files.
        * this functionoi Combines them.
    '''
  data_files = set([
      file.split('_')[0].split('.json')[0] for file in os.listdir()
      if (file.endswith('.json')) and (
          file not in
          ['test.json', 'news_start_urls copy.json', 'news_start_urls.json'])
  ])
  merged_data = []
  for file in data_files:
    if remove_file_if_empty(file):
      # Removed empty file
      continue
    try:
      with open(file, 'r') as f:
        data = json.load(f)
    except:
      '''
               file is corrupt when scrapy is terminated while it is still crawling.
               while corrupt, file is terminated with: `{some_data},`
               adding : `""]` at the end of file to make it valid json file

            '''
      with open(new_file_name, 'a') as file:
        file.write("\"\"]")

      with open(new_file_name, 'r') as file:
        data = json.load(file)
    merged_data.extends(data)

def merge_same_named_json_files(delete_merged=False):
  '''
        * Crawled data is saved in .json files.
        * There are multiple .json files.
        * this functionoi Combines them.
    '''
  data_files = [
      file for file in os.listdir() if (file.endswith('.json')) and (
          file not in
          ['test.json', 'news_start_urls copy.json', 'news_start_urls.json']) and not file.endswith('_merged_.json')
  ]
  merged_data = []
  merged_file_name = '_merged_.json'
  for file in data_files:
    # merge only if it is less than 10 Mb
    if os.path.getsize(file) > 10000000:
      continue
    # merged_file_name = file.split('_')[0].split(
    #     '.json')[0] + '_merged_.json'
    # remove_file_if_empty(merged_file_name)
    print(f'\n\n merged_file_name: {merged_file_name}\n')
    if remove_file_if_empty(file):
      # Removed empty file
      continue
    try:
      with open(file, 'r') as f:
        data = json.load(f)
    except:
      '''
               file is corrupt when scrapy is terminated while it is still crawling.
               while corrupt, file is terminated with: `{some_data},`
               adding : `""]` at the end of file to make it valid json file

            '''
      with open(file, 'a') as f:
        f.write(",{}]")
      print(f'\n\n file: {file}\n')
      with open(file, 'r') as f:
        data = json.load(f)
    # load if file exists
    if os.path.exists(merged_file_name):
      try:
        with open(merged_file_name, 'r') as f:
          old_data = json.load(f)
        if not old_data:
          old_data = []
        old_data.extend(data)
      except:
        print(f'\n\n-------------- Not merged_file_name: {merged_file_name}\n')
        continue
    else:
      old_data = data
      if not old_data:
        old_data = []
    if old_data:
      with open(merged_file_name, 'w') as f:
        json.dump(old_data, f)
    if delete_merged:
      os.remove(file)

def compress_file(input_file_path="nepali_news_dataset.csv",
                  output_file_path="nepali_news_dataset.csv.gz"):
  '''
  '''
  with open(input_file_path, 'rb') as csv_file:
    with gzip.open(output_file_path, 'wb') as compressed_file:
      compressed_file.writelines(csv_file)


def save_nepali_paragraphs_to_csv(csv_file_name = "crawled_nepali_news_dataset.csv"):
  '''
    '''
  data_files = [
      file for file in os.listdir() if (file.endswith('.json')) and (
          file not in
          ['test.json', 'news_start_urls copy.json', 'news_start_urls.json'])
  ]
  for file in data_files:
    if remove_file_if_empty(file):
      continue
      # Removed empty file
    try:
      # load data from each file
      with open(file, 'r') as f:
        data = json.load(f)
    except:
      '''
               file is corrupt when scrapy is terminated while it is still crawling.
               while corrupt, file is terminated with: `{some_data},`
               adding : `""]` at the end of file to make it valid json file

      '''
      with open(file, 'a') as f:
        f.write(",{}]")
      with open(file, 'r') as f:
        data = json.load(f)
    nepali_paragraphs = []
    for d in data:
      if type(d) == dict:
        if 'paragraph' in d.keys():
          nepali_paragraphs.append(d)
          #   print(list(d.values()))
          #   # save dataset in a csv file
          #   with open(csv_file_name, 'a', newline='') as csv_file:
          #     # Create a CSV writer object
          #     csv_writer = csv.writer(csv_file)
          #     # Write the data to the CSV file
          #     csv_writer.writerows(list(d.values()))
          print(f'len_nepali_paragraphs:{len(nepali_paragraphs)} keys:{nepali_paragraphs[0].keys()}')
    # Open the CSV file in append mode
    with open(csv_file_name, 'a', newline='') as csv_file:
        # Create a CSV writer object
        csv_writer = csv.writer(csv_file)

        # Check if the CSV file is empty
        is_empty = os.path.getsize(csv_file_name) == 0
        
        # Extract unique column names from dictionary keys
        column_names = set(key for d in nepali_paragraphs for key in d.keys())
        
        # Write the header only if the CSV file is empty
        if is_empty:
            csv_writer.writerow(column_names)
        
        # Iterate through each dictionary and write rows to CSV
        for d in nepali_paragraphs:
            # Create a list of values for the row, using empty string for missing keys
            row_values = [str(d.get(column, '')) for column in column_names]
            csv_writer.writerow(row_values)
    # ----------------------------
    # Drop duplicate rows in csv
    # ----------------------------
    
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_name)
    # Drop duplicate rows
    df.drop_duplicates(inplace=True)
    # Write the cleaned DataFrame back to the CSV file
    df.to_csv(csv_file_name, index=False)

def remove_file_if_empty(file_path):
  """Checks if a file is empty and removes it if it is.

    Args:
        file_path (str): The path to the file to check.

    Returns:
        bool: True if the file was empty and removed, False otherwise.
    """
  if os.path.exists(file_path):
    if os.path.getsize(file_path) == 0:
      # File size is 0 bytes
      try:
        os.remove(file_path)
        print(f"Removed empty file: {file_path}")
        return True
      except OSError as e:
        print(f"Error removing file: {e}")
        return False
    else:
      try:
        if os.path.getsize(file_path) > 10000000:
            # Open only files less than 10 Mb
            return False
        with open(file_path, 'r') as f:
          data = json.load(f)
      except Exception as Ex:
        print(f'----------------------------------- Exception: {Ex} -----------------------------------\n  file_path: {file_path}\n ')
        with open(file_path, 'a') as f:
          f.write(",{}]")
        with open(file_path, 'r') as f:
          data = json.load(f)
      if not data:
        # File is empty
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


def get_resume_urls(domain_name_to_resume_from):
  '''
    -------------
     pseudocode:
    -------------

    domain_name_to_resume_from = ekantipur

    while there is file ['ekantipur.json', 'ekantipur_0.json', 'ekantipur_1.json', ...]:
        urls_to_visit.add(to_visited_urls)
        urls_visited.add(visited_urls)

    fro url in urls_to_visit:
        add url to news_urls if url is not present in urls_visited

    return list(news_urls)
    '''

  new_file_name = domain_name_to_resume_from + '.json'
  remove_file_if_empty(new_file_name)

  urls_to_visit = set()
  urls_visited = pybloom_live.ScalableBloomFilter(
      mode=pybloom_live.ScalableBloomFilter.LARGE_SET_GROWTH)
  news_start_ulrs = set()
  index = 0
  
  # load data from  merged file
  if os.path.exists(domain_name_to_resume_from + '_merged_.json'): 
    if not remove_file_if_empty(new_file_name):
        # File not empty
        with open(domain_name_to_resume_from + '_merged_.json','r') as f:
            data = json.load(f)
        if data:
            for each_data in data:
                if type(each_data) == dict and 'to_visit' in each_data.keys():
                    urls_to_visit.add(
                        each_data['to_visit'])  # each_data['to_visit'] is a of urls
                if type(each_data) == dict:
                    if 'visited' in each_data.keys():
                        urls_visited.add(
                            each_data['visited'])  # each_data['visited'] is a url

  while os.path.exists(new_file_name):
    if remove_file_if_empty(new_file_name):
      break
    try:
      with open(new_file_name, 'r') as file:
        data = json.load(file)
    except:
      '''
               file is corrupt when scrapy is terminated while it is still crawling.
               while corrupt, file is terminated with: `{some_data},`
               adding : `""]` at the end of file to make it valid json file

            '''
      with open(new_file_name, 'a') as file:
        file.write("\"\"]")

      with open(new_file_name, 'r') as file:
        data = json.load(file)
    for each_data in data:
      if type(each_data) == dict and 'to_visit' in each_data.keys():
        urls_to_visit.add(
            each_data['to_visit'])  # each_data['to_visit'] is a of urls

      if type(each_data) == dict:
        if 'visited' in each_data.keys():
          urls_visited.add(
              each_data['visited'])  # each_data['visited'] is a url

    new_file_name = domain_name_to_resume_from + f'_{index}.json'
    # remove file if file is empty
    remove_file_if_empty(new_file_name)

    index += 1

  print(f'\n\n new_file_name from function:{new_file_name}')
  for to_visit in urls_to_visit:
    if to_visit not in urls_visited:
      news_start_ulrs.add(to_visit)

  return list(news_start_ulrs), urls_visited


def is_nepali_language(text):
  lang, confidence = langid.classify(text)
  return lang == 'hi', confidence
  # is_nepali("बल्ल बुझायो एनसेलले खरिद-बिक्री विवरण")


def is_google_drive_link(link):
  # return id of drive link

  if link.startswith('https://drive.google.com/file/d/'):
    drive_id = link.split('https://drive.google.com/file/d/')[1].split(
        '/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/folders/'):
    drive_id = link.split('https://drive.google.com/drive/folders/')[1].split(
        '/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/open?id='):
    drive_id = link.split('https://drive.google.com/open?id=')[1].split(
        '/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/0/folders/'):
    drive_id = link.split('https://drive.google.com/drive/u/0/folders/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/1/folders/'):
    drive_id = link.split('https://drive.google.com/drive/u/1/folders/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/2/folders/'):
    drive_id = link.split('https://drive.google.com/drive/u/2/folders/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/3/folders/'):
    drive_id = link.split('https://drive.google.com/drive/u/3/folders/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/0/file/d'):
    drive_id = link.split('https://drive.google.com/drive/u/0/file/d/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/1/file/d/'):
    drive_id = link.split('https://drive.google.com/drive/u/1/file/d/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/2/file/d/'):
    drive_id = link.split('https://drive.google.com/drive/u/2/file/d/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/u/3/file/d/'):
    drive_id = link.split('https://drive.google.com/drive/u/3/file/d/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/drive/mobile/folders/'):
    drive_id = link.split('https://drive.google.com/drive/mobile/folders/'
                          )[1].split('/')[0].split('?')[0]
  elif link.startswith('https://drive.google.com/folderview?id='):
    drive_id = link.split('https://drive.google.com/folderview?id=')[1].split(
        '/')[0].split('?')[0]
  else:
    # return original link
    return False, link

  return True, drive_id


def is_same_domain(url1, url2):
  return urlparse(url1).netloc == urlparse(url2).netloc


def is_np_domain(url):
  # return true if it is one of first 1000 .np domain
  parsed_url = urlparse(url)
  base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

  # crawl it if .np domain and len(visited_urls_base) < 1000

  return base_url.endswith('.np') or base_url.endswith('.np/')


def should_we_crawl_it(new_url, visited_urls_base=None):
  # return true if it is one of first 1000 .np domain
  parsed_url = urlparse(new_url)
  base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"

  # crawl it if .np domain and len(visited_urls_base) < 50

  if base_url[-3:] == '.np' and (visited_urls_base == None
                                 or len(visited_urls_base) < 50):
    return base_url, True
  else:
    return base_url, False


# import re
def is_social_media_link(link):
  social_media_patterns = {
      'facebook': r"facebook\.com",
      'instagram': r"instagram\.com",
      'twitter': r"twitter\.com",
      'x': r"x\.com",
      'tiktok': r"tiktok\.com",
      'linkedin': r"linkedin\.com",
      'pinterest': r"pinterest\.com",
      'youtube': r"youtube\.com|youtu\.be",
      'reddit': r"reddit\.com",
      'snapchat': r"snapchat\.com",
      'quora': r"quora\.com",
      'tumblr': r"tumblr\.com",
      'flickr': r"flickr\.com",
      'medium': r"medium\.com",
      'vimeo': r"vimeo\.com",
      'vine': r"vine\.co",
      'periscope': r"periscope\.tv",
      'meetup': r"meetup\.com",
      'mix': r"mix\.com",
      'soundcloud': r"soundcloud\.com",
      'behance': r"behance\.net",
      'dribbble': r"dribbble\.com",
      'vk': r"vk\.com",
      'weibo': r"weibo\.com",
      'ok': r"ok\.ru",
      'deviantart': r"deviantart\.com",
      'slack': r"slack\.com",
      'telegram': r"t\.me|telegram\.me",
      'whatsapp': r"whatsapp\.com",
      'line': r"line\.me",
      'wechat': r"wechat\.com",
      'kik': r"kik\.me",
      'discord': r"discord\.gg",
      'skype': r"skype\.com",
      'twitch': r"twitch\.tv",
      'myspace': r"myspace\.com",
      'badoo': r"badoo\.com",
      'tagged': r"tagged\.com",
      'meetme': r"meetme\.com",
      'xing': r"xing\.com",
      'renren': r"renren\.com",
      'skyrock': r"skyrock\.com",
      'livejournal': r"livejournal\.com",
      'fotolog': r"fotolog\.com",
      'foursquare': r"foursquare\.com",
      'cyworld': r"cyworld\.com",
      'gaiaonline': r"gaiaonline\.com",
      'blackplanet': r"blackplanet\.com",
      'care2': r"care2\.com",
      'cafemom': r"cafemom\.com",
      'nextdoor': r"nextdoor\.com",
      'kiwibox': r"kiwibox\.com",
      'cellufun': r"cellufun\.com",
      'tinder': r"tinder\.com",
      'bumble': r"bumble\.com",
      'hinge': r"hinge\.co",
      'match': r"match\.com",
      'okcupid': r"okcupid\.com",
      'zoosk': r"zoosk\.com",
      'plentyoffish': r"pof\.com",
      'eharmony': r"eharmony\.com",
      'coffee_meets_bagel': r"coffeemeetsbagel\.com",
      'her': r"weareher\.com",
      'grindr': r"grindr\.com",
      'happn': r"happn\.com",
      'hily': r"hily\.com",
      'huggle': r"huggle\.com",
      'jdate': r"jdate\.com",
      'lovoo': r"lovoo\.com",
      'meetmindful': r"meetmindful\.com",
      'once': r"once\.com",
      'raya': r"raya\.app",
      'ship': r"getshipped\.com",
      'silversingles': r"silversingles\.com",
      'tastebuds': r"tastebuds\.fm",
      'the_league': r"theleague\.com",
      'tudder': r"tudder\.com",
      'twoo': r"twoo\.com",
  }

  for social_media, pattern in social_media_patterns.items():
    if (re.search(r'https://www\.' + pattern, link, flags=re.IGNORECASE)) or (
        (re.search(r'https://www\.m\.' + pattern, link, flags=re.IGNORECASE))):
      return True, social_media

  return False, None


# is_social_media_link('https://www.facebook.com/sth.s/ste') # (True, 'facebook')
# is_social_media_link('https://www.m.youtube.com/')# (True, 'youtube') # (True, 'youtube')
# is_social_media_link('https://www.google.com/search?q=youtube.com') # (False, None)


def is_document_link(link):
  # document extensions
  document_extensions = [
      '.pdf', '.odt', '.docx', '.doc', '.pst', '.ai', '.drw', '.dxf', '.eps',
      '.ps', '.svg', '.cdr', '.odg'
  ]
  # presentation extensions
  presentation_extensions = ['.ppt', '.pptx', '.odp', '.pps']
  # spreadsheet extensions
  spreadsheet_extensions = ['.xls', '.xlsx', '.ods']
  # archive extensions
  archive_extensions = [
      '.zip', '.rar', '.tar', '.gz', '.7z', '.7zip', '.bz2', '.tar.gz', '.xz'
  ]
  # video extensions
  video_extensions = [
      '.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.mng', '.mpg', '.qt',
      '.rm', '.swf', '.m4v', '.webm'
  ]
  # audio extensions
  audio_extensions = [
      '.mp3', '.wav', '.ogg', '.wma', '.ra', '.aac', '.mid', '.au', '.aiff',
      '.3gp', '.asf', '.asx', '.m4a'
  ]
  # image extensions
  image_extensions = [
      '.bmp', '.gif', '.jpg', '.jpeg', '.png', '.tif', '.tiff', '.ico',
      '.webp', '.mng', '.pct', '.psp'
  ]
  # other extensions
  other_extensions = ['.css', '.exe', '.bin', '.rss', '.dmg', '.iso', '.apk']

  extensions = {
      'document': document_extensions,
      'presentation': presentation_extensions,
      'spreadsheet': spreadsheet_extensions,
      'archive': archive_extensions,
      'video': video_extensions,
      'audio': audio_extensions,
      'image': image_extensions,
      'other': other_extensions
  }

  for doc_type, extension_list in extensions.items():
    for extension in extension_list:
      if link.lower().endswith(extension):
        return True, doc_type, (
            doc_type == 'other'
        )  # doc_type == 'image'  ignore image or other docs
  return False, None, True


# is_document_link('https://acem.edu.np/uploads/userfiles/files/old_question/8th%20Semester/BCTBEXBEL/Wind%20Energy%202.PDF')
# is_document, document_type = is_document_link('https://www.google.com/link.pdf')

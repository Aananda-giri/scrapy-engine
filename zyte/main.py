import shutil
from distutils.dir_util import copy_tree

# Copy scrapy engine
copy_tree('../scrapy_engine/', './scrapy_engine')

# copy mongo.py from server/ to './scrapy_engine/spiders'
shutil.copy('../server/mongo.py', './scrapy_engine/scrapy_engine/spiders/mongo.py')
shutil.copy('../server/.env', './.env')

'''
# Replace file: `scrapy_engine/scrapy_engine/spiders/worker_spider_v3.py`
from server.mongo import Mongo\n -> from .mongo import Mongo\n
'''

# Read file
with open('./scrapy_engine/scrapy_engine/spiders/worker_spider_v3.py', 'r') as file:
    lines = file.readlines()

# replace line
line_index = lines.index('from server.mongo import Mongo\n')
lines[line_index] = 'from .mongo import Mongo\n'

# re-write files
with open('./scrapy_engine/scrapy_engine/spiders/worker_spider_v3.py', 'w') as file:
    file.writelines(lines)

'''
# Replace file: `scrapy_engine/scrapy_engine/spiders/mongo.py`
# fill actual password in:
uri = f"mongodb+srv://jokeleopedia:{os.environ.get('mongo_password')}@scrapy-engine.5cqch4y.mongodb.net/?retryWrites=true&w=majority&appName=scrapy-engine"
'''
import os
from dotenv import load_dotenv
load_dotenv()

# Read file
with open('./scrapy_engine/scrapy_engine/spiders/mongo.py', 'r') as file:
    lines = file.readlines()

# replace line
for line in lines:
    if line.startswith('            uri = f"mongodb+srv://jokeleopedia:'):
        line_index = lines.index(line)
        lines[line_index] = line.replace("{os.environ.get('mongo_password')}", str(os.environ.get('mongo_password')))
        print('replaced mongo uri with actual password')
        break


# re-write files
with open('./scrapy_engine/scrapy_engine/spiders/mongo.py', 'w') as file:
    file.writelines(lines)


# # copy settings.py
# shutil.copy('./zyte_settings.py', './scrapy_engine/settings.py')

'''
# next steps
cd scrapy_hub
zyte logout; zyte login
shub deploy <project-id>
'''
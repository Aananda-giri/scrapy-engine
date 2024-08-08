import shutil
from distutils.dir_util import copy_tree

# Copy scrapy engine
copy_tree('../scrapy_engine/', './scrapy_engine')

# copy mongo.py from server/ to './scrapy_engine/spiders'
shutil.copy('../server/mongo.py', './scrapy_engine/scrapy_engine/spiders/mongo.py')
shutil.copy('../server/.env', './scrapy_engine/scrapy_engine/spiders/.env')

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
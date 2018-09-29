from algoliasearch import algoliasearch
import configparser, json, os

config = configparser.ConfigParser()
config.read('config.ini')

client = algoliasearch.Client(config['DEFAULT']['ApplicationID'], config['DEFAULT']['APIKey'])
index = client.init_index('cities')

files = [f for f in os.listdir('outputs') if '.json' in f]

for file in files:
    data = json.loads(open('outputs/%s' % file).read())
    res = index.add_objects(data)

from db import logger_db
from datetime import datetime
from configparser import ConfigParser
import matplotlib.pyplot as plt
import sys
import argparse
from dateutil.parser import parse
from dateutil.tz import gettz

config = ConfigParser()
config.read('logger.conf')
nodename = config['main']['Node_name']
db = logger_db(config)

parser = argparse.ArgumentParser(description='Graph some logs.')
parser.add_argument('-d', type=parse, nargs=2, help='start and end dates')
parser.add_argument( 'type', type=str, nargs=1, help='type')
args = parser.parse_args()
print(args)

data = db.get_data(args.type[0])
s_list = []
for feature_section_key in config['features']:
    feature_dict = {}
    feature_name = config['features'][feature_section_key]
    feature_dict['feature_name'] = feature_name
    feature_keys = config[feature_name]
    if 'type' in feature_keys:
        feature_type = config[feature_name]['type']
        feature_dict['feature_type'] = feature_type
    else:
        print('ERROR: NO TYPE SPECIFIED IN SECTION',feature_name)
    s_list.append(feature_dict)

plt.plot(data[1], data[0])
plt.show()
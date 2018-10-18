"__author__ = 'Samuel Kozuch'"
"__credits__ = 'Keboola 2018'"
"__project__ = 'ex-looker'"

"""
Python 3 environment 
"""

#import pip
#pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])


########################
####### NO PIVOT #######
########################

import sys
import os
import logging
import csv
import json
import pandas as pd
import pprint
from keboola import docker
from pylooker.client import LookerClient



### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)
sys.tracebacklimit = 0

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
client_id = params['client_id']
client_secret = params['#client_secret']
api_endpoint = params['api_endpoint']
look_id = params['look_id']
#data_table = cfg.get_parameters()["data_table"]

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


logging.info("Attempting to access API endoint %s" % api_endpoint)

lc = LookerClient(api_endpoint, client_id, client_secret)

for id in look_id:
    name = 'look_' + str(id)
    logging.info('Downloading data from look with ID %s' % id)
    look_data = lc.run_look(int(id))

    if (isinstance(look_data, dict) and \
        "message" in look_data.keys()):
        log = "Data could not be downloaded. Response for look {} "\
                "from server was: {}. Process exiting."
        logging.error(log.format(id, look_data['message']))
        logging.warn("Please make sure that look %s exists." % id)
        sys.exit(1)
    else:
        logging.info("Data for look %s was downloaded successfully." % id)
        path = DEFAULT_FILE_DESTINATION + name + '.csv'
        pd.DataFrame(look_data).to_csv(path, index=False)
        logging.info("%s saved to %s." % (name, path))

logging.info("Script finished.")
sys.exit(0)

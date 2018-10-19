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
import re
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
looker_objects = params['looker_objects']
#data_table = cfg.get_parameters()["data_table"]

#logging.debug("Fetched parameters are :" + str(params))

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def create_manifest(file_name, destination):
    """
    Function for manifest creation.
    """

    file = '/data/out/tables/' + str(file_name) + '.manifest'

    manifest_template = {
                         "destination": str(destination),
                         "incremental": False,
                         "primary_key": []
                        }

    manifest = manifest_template

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output %s manifest file produced." % file_name)
    except Exception as e:
        logging.error("Could not produce %s output file manifest." % file_name)
        logging.error(e)

def fetch_data(endpoint, id, secret, object_id):
    """
    Function fetching the data from query or looker via API.
    """

    logging.info("Attempting to access API endpoint %s" % endpoint)

    lc = LookerClient(endpoint, id, secret)

    
    try:
        look_data = lc.run_look(int(object_id))
    except:
        logging.error("Unable to download data. Please check whether API endpoint was inputted correctly.")
        sys.exit(1)

    if (isinstance(look_data, dict) and \
    "message" in look_data.keys()):
        log = "Data could not be downloaded. Response for look {} "\
        "from server was: {}. Process exiting."

        logging.error(log.format(id, look_data['message']))
        logging.warn("Please make sure that look %s exists." % id)
        sys.exit(1)
    elif isinstance(look_data, list):
        logging.info("Data for look %s was downloaded successfully." % object_id)
        return pd.DataFrame(look_data)
    else:
        logging.error("Unexpected type. Expected dict or list, got %s" % type(look_data))
        sys.exit(2)

def fullmatch_re(pattern, string):
    match = re.fullmatch(pattern, string)

    if match:
        return True
    else:
        return False

def main():
    for obj in looker_objects:
        id = obj['id']
        output = obj['output']

        bool = fullmatch_re(r'^(in|out)\.(c-)\w*\.[\w\-]*', output)
        
        if bool:
            destination = output
            logging.debug("The table with id {0} will be saved to {1}.".format(id, destination))
        elif bool == False and len(output) == 0:
            destination = "in.c-looker.looker_data_%s" % id
            logging.debug("The table with id {0} will be saved to {1}.".format(id, destination))
        else:
            logging.error("The name of the table contains unsupported chatacters. Please provide a valid name with bucket and table name.")
            sys.exit(1)

        file_name = 'looker_data_%s.csv' % id
        output_path = DEFAULT_FILE_DESTINATION + file_name

        look_data = fetch_data(api_endpoint, client_id, client_secret, id)
        look_data.to_csv(output_path, index=False)
        create_manifest(file_name, destination)

if __name__ == "__main__":
    main()

    logging.info("Script finished.")
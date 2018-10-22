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
import requests
import re
from keboola import docker



### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)
sys.tracebacklimit = 0

### Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
client_id = params['client_id']
client_secret = params['#client_secret']
api_endpoint = params['api_endpoint']
looker_objects = params['looker_objects']


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


def fetch_data(endpoint, id, secret, object_id, limit):
    """
    Function fetching the data from query or looker via API.
    """

    logging.info("Attempting to access API endpoint %s" % endpoint)

    params = {'client_id': id, 
              'client_secret': secret}

    logging.info("Logging in...")
    login = requests.post(api_endpoint + 'login', params=params)

    if login.status_code == 200:
        logging.info("Login to Looker was successfull.")
        token = login.json()['access_token']
    else:
        logging.error("Could not login to Looker. Please check, whether correct credentials and/or endpoint were inputted.")
        logging.error("Server response: %s" % login.reason)
        sys.exit(1)
    
    head = {'Authorization': 'token %s' % token}
    look_url = endpoint + 'looks/%s/run/json?limit=%s' % (object_id, str(limit))

    logging.info("Attempting to download data for look %s" % object_id)
    data = requests.get(look_url, headers=head)

    if data.status_code == 200:
        logging.info("Data was downloaded successfully.")
        return pd.io.json.json_normalize(data.json())
    else: 
        logging.error("Data could not be downloaded.")
        logging.error("Request returned: Error %s %s" % (data.status_code, data.reason))
        logging.warn("For more information, see: %s" % data.json()['documentation_url'])
        sys.exit(1)

def create_manifest(file_name, destination, primary_key, incremental):
    """
    Function for manifest creation.
    """

    file = '/data/out/tables/' + str(file_name) + '.manifest'

    manifest_template = {
                         "destination": str(destination),
                         "incremental": incremental,
                         "primary_key": primary_key
                        }

    manifest = manifest_template

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output %s manifest file produced." % file_name)
    except Exception as e:
        logging.error("Could not produce %s output file manifest." % file_name)
        logging.error(e)

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

        look_data = fetch_data(api_endpoint, client_id, client_secret, id, 5000)
        look_data.to_csv(output_path, index=False)
        create_manifest(file_name, destination)

if __name__ == "__main__":
    main()

    logging.info("Script finished.")
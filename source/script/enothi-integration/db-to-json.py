#!/usr/bin/env python
'''
from command line
------------------
./db-to-json.py --credential ./conf/postgres.yml --output ./out/output.json

from py files
------------------
'''

import argparse
import yaml
import psycopg2
import datetime
import time
import json

class Db2Json(object):

    def __init__(self, credential, output):
        self._start_time = int(round(time.time() * 1000))
        self._CREDENTIAL_YML = credential
        self._OUTPUT_JSON = output

    def run(self):
        self.set_up()
        self.read_data()
        self.generate_data()
        self.tear_down()

    def set_up(self):
        # load yml
        self._CREDENTIAL = yaml.load(open(self._CREDENTIAL_YML, 'r'))
        # connect to database
        self._conn = psycopg2.connect(host=self._CREDENTIAL["postgres"]["host"],
                                    database=self._CREDENTIAL["postgres"]["dbname"],
                                    user=self._CREDENTIAL["postgres"]["username"],
                                    password=self._CREDENTIAL["postgres"]["password"])

    def read_data(self):
        cur = self._conn.cursor()
        cur.execute('SELECT version()')

        self._data = {}
        self._data["db_version"] = cur.fetchone()

        cur.close()

    def generate_data(self):
        self._data["hello"] = "hi"

    def tear_down(self):
        # save data to the output json
        with open(self._OUTPUT_JSON, "w") as f:
            f.write(json.dumps(self._data, sort_keys=False, indent=4))

        self._end_time = int(round(time.time() * 1000))
        print("Script took {} seconds".format((self._end_time - self._start_time)/1000))

if __name__ == '__main__':
    # construct the argument parse and parse the arguments
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--credential", required=True, help="yml file where database credentials are given")
    ap.add_argument("-o", "--output", required=True, help="json file where output data will be saved")
    args = vars(ap.parse_args())

    processor = Db2Json(args["credential"], args["output"])
    processor.run()

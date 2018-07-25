#!/usr/bin/python3
# pylint: disable=line-too-long
"""
Module to read config
"""
import os
import sys
import json
import logging

class BlockStorageConfig(object):

    def __init__(self, configfile):
        """
        read config file if exists and return dict
        """
        self._config = None
        if homepath is None:
            if os.name == "nt":
                homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "config")
            else:
                homepath = os.path.join(os.path.expanduser("~"), "config")
        logging.debug("using config directory %s", homepath)
        if not os.path.isdir(homepath):
            print("please create directory {}".format(homepath))
            sys.exit(1)
        else:
            # first best use newer json format file
            configfile = os.path.join(homepath, "WebStorageWebApp.json")
            if os.path.isfile(configfile):
                with open(configfile, "rt") as infile:
                    self._config = json.load(infile)
                    # print(json.dumps(self._config, indent=4))
            else:
                print("please create configuration file %s" % configfile)
                sys.exit(2)

    @property
    def hashfung(self):
        return self._config["blockstorage"]

    @property
    def storage_dir(self):
        return self.blockstorage["storage_dir"]

    @property
    def apikeys(self):
        return self.client_config["apikeys"]

    def __str__(self):
        return json.dumps(self.client_config, indent=4)


if __name__ == "__main__":
    cc = ClientConfig()
    print(cc)


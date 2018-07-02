#!/usr/bin/python3
# pylint: disable=line-too-long
"""
Module to read config
"""
import os
import sys
import logging

def get_config():
    """
    read config file if exists and return dict
    """
    config = {}
    if os.name == "nt":
        homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
    else:
        homepath = os.path.join(os.path.expanduser("~"), ".webstorage")
    logging.debug("using config directory %s", homepath)
    if not os.path.isdir(homepath):
        print("please create directory {}".format(homepath))
        sys.exit(1)
    else:
        configfile = os.path.join(homepath, "WebStorageClient.ini")
        logging.debug("using configfile %s", configfile)
        if not os.path.isfile(configfile):
            print("please create configuration file %s" % configfile)
            sys.exit(2)
        else:
            with open(os.path.join(configfile), "rt") as infile:
                for line in infile:
                    key, value = line.strip().split("=")
                    config[key] = value
    return config


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    import json
    print(json.dumps(get_config(), indent=4))


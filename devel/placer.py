#!/usr/bin/python
import sys
import os
import time
import random
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import concurrent.futures
# own modules
from webstorage import ClientConfig as ClientConfig
from webstorage import BlockStorageClient as BlockStorageClient


def get_checksums(bs, filename, maxage=3600):
    if os.stat(filename).st_mtime + maxage > time.time:
        with open(filename, "rt") as infile:
            return json.load(infile)
    else:
        with open(filename, "wt") as outfile:
            checksums = bs.checksums
            json.dump(checksums, outfile)
            return checksums

if __name__ == "__main__":
    cc = ClientConfig()
    for config in cc.blockstorages:
        print(config)
    bs1_config = cc.blockstorages[0]
    bs1 = BlockStorageClient(url=bs1_config["url"], apikey=bs1_config["apikey"])
    print("found %d existing checksums in BlockStorage named %s" % (len(bs1.checksums), bs1_config["description"]))
    for checksum in get_checksums(bs1, "checksums.json"):
        print(checksums)

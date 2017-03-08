#!/usr/bin/python
import os
import sys
import time
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
# own modules
from webstorage import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    fs = FileStorageClient()
    if os.path.isfile(sys.argv[1]):
        # read checksum from file
        checksum = open(sys.argv[1], "rb").read().split("\n")[1]
        print fs.get(checksum)
    else:
        # read checksum from commandline
        for data in fs.read(sys.argv[1]):
            print data

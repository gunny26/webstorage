#!/usr/bin/python
import os
import sys
import time
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
# own modules
#from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    fs = FileStorageClient()
    if os.path.isfile(sys.argv[1]):
        checksum = open(sys.argv[1], "rb").read().split("\n")[1]
        print fs.get(checksum)
    else:
        data = fs.get(sys.argv[1])
        print data

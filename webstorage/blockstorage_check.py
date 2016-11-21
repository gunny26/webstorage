#!/usr/bin/python
import sys
import os
import time
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from WebStorageClient import BlockStorageClient as BlockStorageClient


if __name__ == "__main__":
    bs = BlockStorageClient()
    for checksum in bs.list():
        logging.info("checking %s", checksum)
        assert bs.exists(checksum)
        #md5 = hashlib.sha1()
        #data = bs.get(checksum)
        #md5.update(data)
        #assert md5.hexdigest() == checksum

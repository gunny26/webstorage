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
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 2 ^ 20
    bs = BlockStorageClient(None)
    for checksum in bs.ls():
        assert bs.exists(checksum)
        md5 = hashlib.md5()
        data = bs.get(checksum)
        md5.update(data)
        assert md5.hexdigest() == checksum

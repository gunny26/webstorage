#!/usr/bin/python
import sys
import os
import time
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient(None)
    for checksum in bs.ls():
        assert bs.exists(checksum)
        md5 = hashlib.md5()
        data = bs.get(checksum)
        md5.update(data)
        assert md5.hexdigest() == checksum

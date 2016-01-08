#!/usr/bin/python
import sys
import os
import time
import hashlib
import logging
logging.basicConfig(level=logging.INFO)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    fs = FileStorageClient(None)
    bs = BlockStorageClient(None)
    existing_blocks = set(bs.ls())
    used_blocks = set()
    for checksum in fs.ls():
        assert fs.exists(checksum)
        print checksum
        metadata = fs.get(checksum)
        for blockchecksum in metadata["blockchain"]:
            if not blockchecksum in existing_blocks:
                print "Block %s of file %s are missing" % (blockchecksum, checksum)
            else:
                used_blocks.add(blockchecksum)
        #md5 = hashlib.md5()
        #data = bs.get(checksum)
        #md5.update(data)
        #assert md5.hexdigest() == checksum
    unused_blocks = [blockchecksum for blockchecksum in existing_blocks if blockchecksum not in used_blocks]
    print "unused blocks %d" % len(unused_blocks)

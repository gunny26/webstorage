#!/usr/bin/python
import sys
import os
import time
import logging
logging.basicConfig(level=logging.INFO)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    fi = FileIndexClient(fs)
    targetname = sys.argv[1]
    if fi.exists(targetname):
        print "directory or file %s does already exist" % targetname
        sys.exit(1)
    fi.mkdir(targetname)

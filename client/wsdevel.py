#!/usr/bin/python
import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient(None)
    fs = FileStorageClient(None, bs, BLOCKSIZE)
    fi = FileIndexClient(None)
    # testing directory
    filename = "/"
    print fi.get(filename)
    print fi.isdir(filename)
    print fi.isfile(filename)
    # testing file
    filename = "/Der_Dicke_und_das_Warzenschwein_15.11.11_23-15_hr3_80_TVOON_DE.mpg.HQ.cut.mp4"
    print fi.get(filename)
    print fi.isdir(filename)
    print fi.isfile(filename)

#!/usr/bin/python
import sys
import os
import time
import logging
logging.basicConfig(level=logging.DEBUG)
import magic
# own modules
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
#from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    sourcename = sys.argv[1]
    m=magic.open(magic.MAGIC_MIME)
    m.load()
    print m.file(sourcename)
    try:
        targetname = sys.argv[2]
    except IndexError:
        print "usage {sourcefile} {targetfile or directory}"
        sys.exit(3)
    print "storing to %s" % targetname
    starttime = time.time()
    metadata = fs.put_fast(open(sourcename, "rb"))
    print metadata
    duration = time.time() - starttime
    print "stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration)

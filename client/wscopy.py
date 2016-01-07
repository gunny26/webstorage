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
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    fi = FileIndexClient(fs)
    sourcename = sys.argv[1]
    try:
        targetname = sys.argv[2]
    except IndexError:
        targetname = os.path.basename(sys.argv[1])
    if fi.isfile(targetname) is True:
        print "file %s already exists" % sys.argv[1]
        sys.exit(1)
    starttime = time.time()
    metadata = fs.put_fast(open(sourcename, "rb"))
    duration = time.time() - starttime
    print "stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration)
    fi.put(targetname, metadata["checksum"])
    print "Filename %s stored in FileIndex" % sourcename

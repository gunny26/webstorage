#!/usr/bin/python
import sys
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
    #if not fi.isdir("test"):
    #    fi.mkdir("test")
    #if fi.isfile(sys.argv[1]) is True:
    #    print "file %s already exists" % sys.argv[1]
    #    sys.exit(1)
    starttime = time.time()
    metadata = fs.put_fast(sys.stdin)
    duration = time.time() - starttime
    print "stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration)
    fi.put(sys.argv[1], metadata["checksum"])
    print "Filename %s stored in FileIndex" % sys.argv[1]

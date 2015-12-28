#!/usr/bin/python
import sys
import time
import logging
logging.basicConfig(level=logging.DEBUG)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient("http://odroid.op226/blockstorage")
    fs = FileStorageClient("http://odroid.op226/filestorage", bs, BLOCKSIZE)
    fh = open(sys.argv[1], "wb")
    # FileStorage Tests
    starttime = time.time()
    metadata = fs.put(sys.stdin)
    duration = time.time() - starttime
    print "stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration)
    fh.write(metadata["checksum"])
    fh.close()

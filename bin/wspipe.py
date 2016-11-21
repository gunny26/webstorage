#!/usr/bin/python
import sys
import time
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
# own modules
from WebStorageClient import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    fs = FileStorageClient()
    starttime = time.time()
    metadata = fs.put(sys.stdin)
    duration = time.time() - starttime
    sys.stdout.write("stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s\n" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration))

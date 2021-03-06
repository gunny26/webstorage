#!/usr/bin/python3
import sys
import time
import hashlib
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
# own modules
from webstorageClient import FileStorageClient


if __name__ == "__main__":
    fs = FileStorageClient()
    digest = hashlib.sha1()
    size = 0
    starttime = time.time()
    for data in fs.read(sys.argv[1]):
        sys.stdout.buffer.write(bytes(data))
        digest.update(data)
        size += len(data)
    duration = time.time() - starttime
    checksum = digest.hexdigest()
    if checksum != sys.argv[1]:
        sys.stderr.write("ERROR: checksum mismatch, the received is not the same as the requested\n")
    sys.stderr.write("stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s\n" % (checksum, size / 1024, duration, size / 1024 / duration))

#!/usr/bin/python
import sys
import os
import time
import random
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)
from webstorage import BlockStorageClient as BlockStorageClient


if __name__ == "__main__":
    bs1 = BlockStorageClient()
    bs2 = BlockStorageClient(url="http://neutrino.messner.click/blockstorage", apikey="65a7dfd9-3d41-4135-81ca-d845bc4b6676")
    print("found %d existing checksums in bs1" % len(bs1.checksums))
    print("found %d existing checksums in bs2" % len(bs2.checksums))
    for checksum in bs1.checksums:
        print("getting %s" % checksum)
        data = bs1.get(checksum)
        print("putting %s" % checksum)
        put_checksum, status = bs2.put(data)
        assert put_checksum == checksum

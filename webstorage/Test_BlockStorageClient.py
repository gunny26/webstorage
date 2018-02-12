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
from BlockStorageClient import BlockStorageClient as BlockStorageClient


if __name__ == "__main__":
    bs = BlockStorageClient()
    print("found %d existing checksums" % len(bs.checksums))
    print("checking 10 random blocks")
    for checksum in random.sample(bs.checksums, 10):
        logging.info("checking exists %s", checksum)
        assert bs.exists(checksum)
        md5 = hashlib.sha1()
        data = bs.get(checksum)
        logging.info("checking checksum of data with length %d", len(data))
        md5.update(data)
        assert md5.hexdigest() == checksum
        # put data back
        put_checksum, status = bs.put(data)
        assert checksum == put_checksum
    print("checking 10 random blocks with verify=True")
    for checksum in random.sample(bs.checksums, 10):
        logging.info("checking checksum %s", checksum)
        data = bs.get(checksum, verify=True)
        logging.info("got data with length %d", len(data))
    print("checking 10 random blocks which should not exist")
    for checksum in random.sample(bs.checksums, 10):
        logging.info("checking checksum %s+AA", checksum)
        assert not bs.exists(checksum + "AA")
        try:
            data = bs.get(checksum + "AA")
        except KeyError:
            pass

#!/usr/bin/python
import sys
import os
import time
import hashlib
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    fs = FileStorageClient(None)
    bs = BlockStorageClient(None)
    existing_blocks = set(bs.ls())
    logging.info("found %d blocks in BlockStorage", len(existing_blocks))
    used_blocks = set()
    existing_files = fs.ls()
    logging.info("found %d files in FileStorage", len(existing_files))
    percent = 0.0
    starttime = time.time()
    for counter, checksum in enumerate(existing_files):
        logging.debug("checking file with checksum %s", checksum)
        metadata = fs.get(checksum)
        for blockchecksum in metadata["blockchain"]:
            if not blockchecksum in existing_blocks:
                print "Block %s of file %s are missing" % (blockchecksum, checksum)
            else:
                used_blocks.add(blockchecksum)
        if (100.0 * counter/ len(existing_files)) > (percent + 1.0):
            percent = 100.0 * counter/ len(existing_files)
            logging.info("%0.2f %% done in %0.2f s (%d of %d file scanned)", percent, time.time()-starttime, counter, len(existing_files))
            starttime = time.time()
    logging.info("used blocks %d", len(used_blocks))
    unused_blocks = [blockchecksum for blockchecksum in existing_blocks if blockchecksum not in used_blocks]
    logging.info("unused blocks %d", len(unused_blocks))

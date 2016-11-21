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

# decode early, encode late

def walk(fi, directory):
    #assert isinstance(directory, unicode)
    # logging.info("descending into %s", directory)
    # remove trailing / at directory path
    if len(directory) > 1 and directory.endswith(u"/"):
        directory = directory[:-1]
    try:
        for filename in fi.listdir(directory):
            filename = unicode(filename)
            #print ":".join(["%x" % ord(char) for char in filename])
            abspath = directory + "/" + filename
            if directory == u"/":
                abspath = u"/" + filename
            if fi.isdir(abspath) is True:
                for result in walk(fi, abspath):
                    yield result
            elif fi.isfile(abspath) is True:
                yield abspath
            else:
                logging.error("this should be either file %s or directory %s", fi.isfile(filename), fi.isdir(filename))
    except StandardError as exc:
        logging.exception(exc)
        logging.error("Exception on walking in directory %s", directory)

if __name__ == "__main__":
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    fi = FileIndexClient(fs)
    existing_blocks = set(bs.ls())
    logging.info("existing blocks in BlockStorage: %d", len(existing_blocks))
    existing_file_checksums = set(fs.ls())
    logging.info("existing files in FileStorage: %d", len(existing_file_checksums))
    used_blocks = set()
    filechecksum_counter = {}
    used_checksums = set()
    for filename in walk(fi, u"/musik/ultrastar/songs/L-M"):
        checksum = fi.get(filename)
        # something like 4da1e9a09036f981f56ed1c5b3d00e8b
        try:
            assert isinstance(checksum, basestring)
            assert len(checksum) == len("4da1e9a09036f981f56ed1c5b3d00e8b")
        except AssertionError as exc:
            logging.error("%s : %s does not look like checksum", filename, checksum)
            raise exc
        used_checksums.add(checksum)
        if checksum not in filechecksum_counter:
            filechecksum_counter[checksum] = 1
        else:
            filechecksum_counter[checksum] += 1
        logging.info("File: %s - %s", checksum, filename)
        if not fs.exists(checksum):
            logging.error("filechecksum %s for %s does not exist", checksum, filename)
            continue
        metadata = fs.get(checksum)
        for blockchecksum in metadata["blockchain"]:
            if not blockchecksum in existing_blocks:
                logging.error("Block %s of file %s is missing", blockchecksum, checksum)
            else:
                used_blocks.add(blockchecksum)
        #md5 = hashlib.md5()
        #data = bs.get(checksum)
        #md5.update(data)
        #assert md5.hexdigest() == checksum
    logging.info("used blocks %d", len(used_blocks))
    unused_blocks = [blockchecksum for blockchecksum in existing_blocks if blockchecksum not in used_blocks]
    logging.info("unused blocks %d", len(unused_blocks))
    double_files = [checksum for checksum, count in filechecksum_counter.items() if count > 1]
    logging.info("found %s duplicate file checksums", len(double_files))
    unused_checksums = [checksum for checksum in existing_file_checksums if checksum not in used_checksums]
    logging.info("unused checksums in filestorage %s", len(unused_checksums))

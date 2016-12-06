#!/usr/bin/python3
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from webstorage import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    blockchecksums = {}
    fs = FileStorageClient(cache=True)
    print("checking all existing files in filestorage")
    files = fs.list()
    counter = 1
    numfiles = len(files)
    for filechecksum in files:
        metadata = fs.get(filechecksum)
        for blockchecksum in metadata["blockchain"]:
            if blockchecksum in blockchecksums:
                blockchecksums["blockchecksum"] += 1
            else:
                blockchecksums["blockchecksum"] = 1
        print("{}/{} file with checksum {} contains {} blocks".format(counter, numfiles, filechecksum, len(metadata["blockchain"])))
        counter += 1

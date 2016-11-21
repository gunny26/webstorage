#!/usr/bin/python3
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from WebStorageClient import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    blockchecksums = {}
    fs = FileStorageClient(cache=True)
    for filechecksum in fs.list():
        metadata = fs.get(filechecksum)
        for blockchecksum in metadata["blockchain"]:
            if blockchecksum in blockchecksums:
                blockchecksums["blockchecksum"] += 1
            else:
                blockchecksums["blockchecksum"] = 1
        print(filechecksum, len(metadata["blockchain"]))


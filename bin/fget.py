#!/usr/bin/python3
import sys
import hashlib
import json
import os
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.DEBUG)
# own modules
import webstorageClient.FileStorageClient


def main():
    # read information from fstor datastructure
    # and get according file data stream to sys.stdout
    if len(sys.argv) != 3:
        logging.error("usage: fget <checksum> <outputfile>")
        sys.exit(1)
    if os.path.isfile(sys.argv[2]):
        logging.error("file %s does already exist" % sys.argv[2])
        sys.exit(2)
    logging.info("getting file data stream of file with checksum %s", sys.argv[1])
    fs = webstorageClient.FileStorageClient(cache=False)
    with open(sys.argv[2], "wb") as outfile:
        # get file with checksum in sys.argv[1] as is
        digest = hashlib.sha1()
        for data in fs.read(sys.argv[1]):
            outfile.write(bytes(data))
            digest.update(data)
        checksum = digest.hexdigest()
        if checksum != sys.argv[1]:
            logging.error("ERROR: checksum mismatch, the received checksum %s is not the same as the requested checksum %s\n", checksum, sys.argv[1])
        else:
            logging.debug("checksum verification is successfull")

if __name__ == "__main__":
    main()

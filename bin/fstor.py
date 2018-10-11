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
    if len(sys.argv) != 2:
        logging.error("usage: fstor <filename>\n")
        sys.exit(1)
    if not os.path.isfile(sys.argv[1]):
        logging.error("file %s not found\n" % sys.argv[1])
        sys.exit(2)
    with open(sys.argv[1], "rt") as infile:
        # empty lines or lines starting with # should be ignored
        json_str = " ".join([line for line in infile.read().split("\n") if not line.startswith("#") or line == ""])
        metadata = json.loads(json_str)
        logging.info("getting file data stream of file with checksum %s", metadata["checksum"])
        fs = webstorageClient.FileStorageClient(cache=False)
        # get file with checksum in sys.argv[1] as is
        digest = hashlib.sha1()
        for data in fs.read(metadata["checksum"]):
            sys.stdout.buffer.write(bytes(data))
            digest.update(data)
        checksum = digest.hexdigest()
        if checksum != metadata["checksum"]:
            logging.error("ERROR: checksum mismatch, the received checksum %s is not the same as the requested checksum %s\n", checksum, metadata["checksum"])
        else:
            logging.debug("checksum verification is successfull")

if __name__ == "__main__":
    main()

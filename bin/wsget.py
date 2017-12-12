#!/usr/bin/python3
import os
import sys
import time
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
import argparse
# own modules
from webstorage import FileStorageClient as FileStorageClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="to retrieve file with checksum from filestorage backend. use search to find checksum")
    parser.add_argument("-c", "--checksum", help="checksum of file to retrieve", required=True)
    parser.add_argument("-o", "--output", help="full name to output retrieved file", required=True)
    parser.add_argument("-f", "--force", help="force overwrite of existing file, be carefull", action="store_true", default=False)
    # options = parser.parse_args("-c 3e3ffbf86c1d72fe4cced47038c1fef88c06d1f6 -o /tmp/test -f".split())
    options = parser.parse_args()
    fs = FileStorageClient()
    if os.path.isfile(options.output) and options.force is False:
        logging.error("file %s already exists, stopping", options.output)
    elif os.path.isfile(options.output) and options.force is True:
        logging.info("overwriting existing file %s", options.output)
        with open(options.output, "wb") as output:
            for data in fs.read(options.checksum):
                output.write(data)
    elif not os.path.isfile(options.output):
        with open(options.output, "wb") as output:
            for data in fs.read(options.checksum):
                output.write(data)

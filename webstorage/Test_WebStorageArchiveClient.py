#!/usr/bin/python3
import random
import hashlib
import json
import socket
import logging
logging.basicConfig(level=logging.DEBUG)
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)
from WebStorageArchiveClient import WebStorageArchiveClient as WebStorageArchiveClient


if __name__ == "__main__":
    wsac = WebStorageArchiveClient()
    print("checking all existing archives")
    print("getting all available backupsets for this host")
    print(json.dumps(wsac.get_backupsets(socket.gethostname()), indent=4))
    print("getting latest backupset name for this host")
    latest_backupset = wsac.get_latest_backupset(socket.gethostname())
    print(latest_backupset)
    print("getting latest backupset")
    print(wsac.get(latest_backupset)["hashmap"].keys())

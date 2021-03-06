#!/usr/bin/python
import sys
import os
import time
import random
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import concurrent.futures
# own modules
from webstorage import ClientConfig as ClientConfig
from webstorage import BlockStorageClient as BlockStorageClient


def copy(checksum):
    starttime = time.time()
    block = bs1.get(checksum)
    print("read %s size %s" % (checksum, len(block)))
    if block:
        bs2.put(block)
        print("checksum %s size %s duplicated in %0.2f s" % (checksum, len(block), (time.time() - starttime)))

if __name__ == "__main__":
    cc = ClientConfig()
    for config in cc.blockstorages:
        print(config)
    bs1_config = cc.blockstorages[0]
    bs1 = BlockStorageClient(url=bs1_config["url"], apikey=bs1_config["apikey"])
    bs2_config = cc.blockstorages[1]
    bs2 = BlockStorageClient(url=bs2_config["url"], apikey=bs2_config["apikey"])
    print("found %d existing checksums in BlockStorage named %s" % (len(bs1.checksums), bs1_config["description"]))
    print("found %d existing checksums in BlockStorage named %s" % (len(bs2.checksums), bs2_config["description"]))
    checksums = [checksum for checksum in bs1.checksums if checksum not in bs2.checksums]
    print("identified %s checksums to duplicate" % len(checksums))
<<<<<<< HEAD
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for checksum in checksums[:100]:
            futures.append(executor.submit(copy, checksum))
        for future in concurrent.futures.as_completed(futures):
            print(future.result())
=======
    for checksum in checksums[:100000]:
        copy(checksum)
>>>>>>> a8a3345a18548ba01c37feb813fa4183f37cf56b

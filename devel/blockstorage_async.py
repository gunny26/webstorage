#!/usr/bin/python
import sys
import os
import time
import hashlib
import json
import logging
logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("requests").setLevel(logging.WARNING)
# logging.getLogger("urllib3").setLevel(logging.WARNING)
# own modules
from webstorageClient import BlockStorageClient as BlockStorageClient

def verify():
    while True:
        checksum = q.get()
        if not checksum:
            break
        try:
            checksum_target, status_code = bs_target.put(bs.get(checksum))
            print("%s : synced %s - %s" % (threading.get_ident(), checksum, checksum_target))
            assert checksum_target == checksum
        except Exception as exc:
            print(exc)
        finally:
            q.task_done()

def status():
    interval = 5
    starttime = time.time()
    max_size = q.qsize()
    while not q.empty():
        size = q.qsize()
        done = max_size - size
        print("%d of %d left %0.2f checksums/s" % (max_size - size, max_size, done/(time.time() - starttime)))
        time.sleep(interval)


if __name__ == "__main__":
    bs_source = BlockStorageClient(url="http://wsa01.messner.click/blockstorage", apikey="4a90ae8a-9780-4737-b70d-7754e4cb1c9f")
    bs_target = BlockStorageClient(url="http://wsa02.messner.click/bs001", apikey="4a90ae8a-9780-4737-b70d-7754e4cb1c9f")
    checksums_source = sorted(bs_source.checksums)
    checksums_target = sorted(bs_target.checksums)
    while checksums_source:
        checksum = checksums_source.pop()
        print(checksum)
        if checksum != checksums_target[0]:
            checksum_target, status_code = bs_target.put(bs_source.get(checksum))
            print("synced %s - %s" % (checksum, checksum_target))
            checksums_target.pop()

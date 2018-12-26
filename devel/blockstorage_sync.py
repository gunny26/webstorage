#!/usr/bin/python
import sys
import os
import time
import hashlib
import json
import threading
import queue
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
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
    bs = BlockStorageClient()
    print(json.dumps(bs.info, indent=4))
    bs_target = BlockStorageClient(url="http://wsa02.messner.click/bs001", apikey="")
    print(json.dumps(bs_target.info, indent=4))
    num_worker_threads = 8
    # preparing list of checksums
    starttime = time.time()
    print("building list of differing blocks")
    checksums = [checksum for checksum in set(bs.checksums) if checksum not in set(bs_target.checksums)]
    print("done in %0.2f found %d blocks to sync", (time.time() - starttime, len(checksums)))
    starttime = time.time()
    print("preparing queue")
    q =queue.Queue() # put checksums in Queue
    for item in checksums:
        q.put(item)
    print("done in %0.2f" % (time.time() - starttime))
    # preparing threads
    status = threading.Thread(target=status)
    status.start()
    threads = []
    for i in range(num_worker_threads):
        thread = threading.Thread(target=verify)
        thread.start()
        threads.append(thread)
    # block until all tasks are done
    q.join()
    # stop workers by inserting None
    for i in range(num_worker_threads):
        q.put(None)
    for thread in threads:
        thread.join()
    status.join()

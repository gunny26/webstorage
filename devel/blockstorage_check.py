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
        try:
            if not checksum:
                break
            #print("%s : checking %s" % (threading.get_ident(), checksum))
            try:
                bs.get_verify(checksum)
            except AssertionError as exc:
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
    bs = BlockStorageClient(url="http://192.168.1.168/bs001", apikey="")
    print(json.dumps(bs.info, indent=4))
    num_worker_threads = 4
    starttime = time.time()
    q =queue.Queue() # put checksums in Queue
    for item in bs.checksums:
        q.put(item)
    print("loading list of %d checksums in %0.2f" % (len(bs.checksums), time.time() - starttime))
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

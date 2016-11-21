#!/usr/bin/python
import sys
import os
import time
import logging
logging.basicConfig(level=logging.INFO)
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    fi = FileIndexClient(fs)
    try:
        sourcedir = sys.argv[1]
        if sourcedir.endswith("/"):
            sourcedir = unicode(sourcedir[:-1])
        targetdir = sys.argv[2]
        if targetdir.endswith("/"):
            targetdir = unicode(targetdir[:-1])
    except IndexError:
        print "usage {sourcedirectory} {targetdirectory"
        sys.exit(3)
    if not os.path.isdir(sourcedir):
        print "Filesystem %s is not a directory" % sourcedir
        sys.exit(4)
    if fi.isdir(targetdir) is not True:
        print "WebStorage %s is not a directory" % targetdir
        sys.exit(5)
    for dirpath, dirnames, filenames in os.walk(sourcedir):
        for dirname in dirnames:
            abs_path = os.path.join(dirpath, dirname)
            target = abs_path.replace(sourcedir, targetdir)
            print "mkdir %s" %target
            fi.mkdir(target)
        for filename in filenames:
            abs_path = os.path.join(dirpath, filename)
            target = abs_path.replace(sourcedir, targetdir)
            print "copy %s to %s" % (abs_path, target)
            metadata = fs.put_fast(open(abs_path, "rb"))
            fi.put(target, metadata["checksum"])
    sys.exit(0)
    print "storing to %s" % targetname
    starttime = time.time()
    metadata = fs.put_fast(open(sourcename, "rb"))
    duration = time.time() - starttime
    print "stream stored with checksum %s, size %0.2f kb, duration %0.2f s, %0.2f kb/s" % (metadata["checksum"], metadata["size"] / 1024, duration, metadata["size"] / 1024 / duration)
    fi.put(targetname, metadata["checksum"])
    print "Filename %s stored in FileIndex" % sourcename

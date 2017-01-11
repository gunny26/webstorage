#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import os
import hashlib
import datetime
import time
import sys
import socket
import argparse
import stat
import re
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import json
import dbm
# own modules
from webstorage import WebStorageArchive as WebStorageArchive
from webstorage import FileStorageClient as FileStorageClient

def filemode(st_mode):
    """
    convert stat st_mode number to human readable string
    taken from https://stackoverflow.com/questions/17809386/how-to-convert-a-stat-output-to-a-unix-permissions-string
    """
    is_dir = 'd' if stat.S_ISDIR(st_mode) else '-'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = str(oct(st_mode)[-3:])
    return is_dir + ''.join(dic.get(x, x) for x in perm)

def sizeof_fmt(num, suffix='B'):
    """
    function to convert numerical size number into human readable number
    taken from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def ppls(absfile, filedata):
    """
    pritty print ls
    return long filename format, like ls -al does
    """
    st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
    datestring = datetime.datetime.fromtimestamp(int(st_mtime))
    return "%10s %s %s %10s %19s %s" % (filemode(st_mode), st_uid, st_gid, sizeof_fmt(st_size), datestring, absfile)

def main():
    myhostname = socket.gethostname()
    wsa = WebStorageArchive()
    backupsets = wsa.get_backupsets(myhostname)
    filecache = {}
    for backupset in backupsets:
        print(backupset)
        data = wsa.get(backupset)
        for absfile in data["filedata"].keys():
            if absfile not in filecache:
                print("new absfile {}".format(absfile))
                filecache[absfile] = [backupset, ]
            else:
                try:
                    if backupset not in filecache[absfile]:
                        filecache[absfile].append(backupset)
                except json.decoder.JSONDecodeError as exc:
                    print(filecache[absfile])
                    raise exc
        print(" done")
    print("Found %d filenames" % len(filecache.keys()))
    # write database to disk
    with dbm.open("cache", "n") as db:
        for key in filecache.keys():
            print("%10d %s" % (len(filecache[key]), key))
            db[key] = ";".join(filecache[key])
    diskcache = {}
    # read database from disk
    with dbm.open("cache", "r") as db:
        for key in db.keys():
            print(key,encode("utf-8"))
            diskcache[key] = db[key].split(";")
    # check if newly read diskcache is equal
    for key in diskcache.keys():
        print("%10d %10d %s" % (len(diskcache[key]), len(filecache[key]), key))
        assert len(diskcache[key]) == len(filecache[key])


if __name__ == "__main__":
    main()


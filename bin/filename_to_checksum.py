#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import os
import hashlib
import datetime
import dateutil.parser
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

def main(filename):
    myhostname = socket.gethostname()
    wsa = WebStorageArchive()
    backupsets = wsa.get_backupsets(myhostname)
    mem_cache = {}
    for backupset in backupsets:
        print(backupset)
        data = wsa.get(backupset)
        for absfile in data["filedata"].keys():
            if absfile not in mem_cache:
                print("new absfile {}".format(absfile))
                mem_cache[absfile] = [backupset, ]
            else:
                if backupset not in mem_cache[absfile]:
                    mem_cache[absfile].append(backupset)
        print(" done")
    print("Found %d filenames" % len(mem_cache.keys()))
    #json.dump(filecache, open("cache.json", "wt"))
    # write database to disk
    with dbm.open(filename, "n") as db:
        for key in mem_cache.keys():
            print("%10d %s" % (len(mem_cache[key]), key))
            db[bytes(key.encode("utf-8"))] = bytes(json.dumps(mem_cache[key]).encode("utf-8"))
    diskcache = {}
    # read database from disk
    with dbm.open(filename, "r") as db:
        for key in db.keys():
            print(key.decode("utf-8"))
            diskcache[key.decode("utf-8")] = json.loads(db[key].decode("utf-8"))
    # check if newly read diskcache is equal
    for key in diskcache.keys():
        print("%10d %10d %s" % (len(diskcache[key]), len(mem_cache[key]), key))
        assert len(diskcache[key]) == len(mem_cache[key])
    return

def update(filename):
    import sqlite3
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    cur.execute("create table if not exists filename_to_checksum (backupset, hostname, tag, dirname, basename, datestring datetime, checksum)")
    cur.execute("create table if not exists checksum_to_backupset (checksum, backupset)");
    myhostname = socket.gethostname()
    wsa = WebStorageArchive()
    backupsets = wsa.get_backupsets(myhostname)
    # like wse0000107_mesznera_2016-12-06T13:48:13.400565.wstar.gz
    for backupset in backupsets:
        hostname, tag, isoformat_ext = backupset.split("_")
        isoformat = isoformat_ext[:-9]
        datestring = dateutil.parser.parse(isoformat)
        print(hostname, tag, dateutil.parser.parse(isoformat))
        data = wsa.get(backupset)
        for absfile in data["filedata"].keys():
            checksum = data["filedata"][absfile]["checksum"]
            cur.execute("insert into filename_to_checksum values (?, ?, ?, ?, ?, ?, ?)", (backupset, hostname, tag, os.path.dirname(absfile), os.path.basename(absfile), isoformat, checksum))
            cur.execute("insert into checksum_to_backupset values (?, ?)", (checksum, backupset))
            # print(data["filedata"][absfile])
        conn.commit()
        print(" done")

if __name__ == "__main__":
    filename = "filename_to_checksum.db"
    #main(filename)
    update(filename)


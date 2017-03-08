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
import sqlite3
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import json
import dbm
# own modules
from webstorage import WebStorageArchive as WebStorageArchive
from webstorage import FileStorageClient as FileStorageClient

class NtoM(object):
    """
    build n : m dependent key value stores
    """

    def __init__(self, keyname1, keyname2):
        self.__keyname1 = keyname1
        self.__keyname2 = keyname2
        self.__filename = filename
        self.__data = {
            self.__keyname1 : {},
            self.__keyname2 : {}
            }
        self.__dirty = False # indicate if data is modified in memory

    def add(self, **kwds):
        key1 = kwds[self.__keyname1]
        key2 = kwds[self.__keyname2]
        if key1 in self.__data[self.__keyname1]:
            if key2 not in self.__data[self.__keyname1][key1]:
                self.__data[self.__keyname1][key1].add(key2)
            # ignore if value is already in list
        else:
            self.__data[self.__keyname1][key1] = set([key2, ])
        if key2 in self.__data[self.__keyname2]:
            if key1 not in self.__data[self.__keyname2][key2]:
                self.__data[self.__keyname2][key2].add(key1)
            # ignore if value is already in list
        else:
            self.__data[self.__keyname2][key2] = set([key1, ])
        self.__dirty = True

    def save(self, filename):
        """
        dump internal data to sqlite database
        """
        starttime = time.time()
        conn = sqlite3.connect(filename)
        cur = conn.cursor()
        # key 1
        tablename1 = "%s_to_%s" % (self.__keyname1, self.__keyname2)
        logging.debug("saving to %s", tablename1)
        cur.execute("drop table if exists %s" % tablename1)
        conn.commit()
        cur.execute("create table if not exists %s ('%s', '%s')" % (tablename1, self.__keyname1, self.__keyname2))
        for key, value in self.__data[self.__keyname1].items():
            cur.execute("insert into %s values (?, ?)" % tablename1, (key, json.dumps(list(value))))
        conn.commit()
        # key 2
        tablename2 = "%s_to_%s" % (self.__keyname2, self.__keyname1)
        logging.debug("saving to %s", tablename2)
        cur.execute("drop table if exists %s" % tablename2)
        conn.commit()
        cur.execute("create table if not exists %s ('%s', '%s')" % (tablename2, self.__keyname2, self.__keyname1))
        for key, value in self.__data[self.__keyname2].items():
            cur.execute("insert into %s values (?, ?)" % tablename2, (key, json.dumps(list(value))))
        conn.commit()
        logging.debug("save done in %0.2f s", time.time()-starttime)
        logging.debug("saved %d in %s", len(self.__data[self.__keyname1]), self.__keyname1)
        logging.debug("saved %d in %s", len(self.__data[self.__keyname2]), self.__keyname2)
        self.__dirty = False

    def load(self, filename):
        """
        dump internal data to sqlite database
        """
        starttime = time.time()
        conn = sqlite3.connect(filename)
        cur = conn.cursor()
        try:
            # key 1
            tablename1 = "%s_to_%s" % (self.__keyname1, self.__keyname2)
            for row in cur.execute("select * from %s" % tablename1).fetchall():
                self.__data[self.__keyname1][row[0]] = set(json.loads(row[1]))
            # key 2
            tablename2 = "%s_to_%s" % (self.__keyname2, self.__keyname1)
            for row in cur.execute("select * from %s" % tablename2).fetchall():
                self.__data[self.__keyname2][row[0]] = set(json.loads(row[1]))
            logging.debug("load done in %0.2f s", time.time()-starttime)
            logging.debug("loaded %d in %s", len(self.__data[self.__keyname1]), self.__keyname1)
            logging.debug("loaded %d in %s", len(self.__data[self.__keyname2]), self.__keyname2)
        except sqlite3.OperationalError as exc:
            logging.info("ignoring if table does not exist")


def update(filename):
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    cur.execute("create table if not exists backupsets_done (backupset)")
    myhostname = socket.gethostname()
    wsa = WebStorageArchive()
    backupsets = wsa.get_backupsets(myhostname)
    # like wse0000107_mesznera_2016-12-06T13:48:13.400565.wstar.gz
    filename_to_checksum = NtoM("absfile", "checksum")
    filename_to_checksum.load(filename)
    filename_to_backupset = NtoM("absfile", "backupset")
    filename_to_backupset.load(filename)
    backupsets_done = [row[0] for row in cur.execute("select backupset from backupsets_done").fetchall()]
    for backupset in backupsets:
        starttime = time.time()
        #if backupset in backupsets_done:
        #    print(" backupset %s already done" % backupset)
        #    continue
        hostname, tag, isoformat_ext = backupset.split("_")
        isoformat = isoformat_ext[:-9]
        datestring = dateutil.parser.parse(isoformat)
        print(hostname, tag, dateutil.parser.parse(isoformat))
        data = wsa.get(backupset)
        for absfile in data["filedata"].keys():
            checksum = data["filedata"][absfile]["checksum"]
            filename_to_checksum.add(absfile=absfile, checksum=checksum)
            filename_to_backupset.add(absfile=absfile, backupset=backupset)
            # print(data["filedata"][absfile])
        #cur.execute("insert into backupsets_done values (?)", (backupset,))
        #conn.commit()
        logging.info(" done in %0.2f s", time.time()-starttime)
    filename_to_checksum.save(filename)
    filename_to_backupset.save(filename)

if __name__ == "__main__":
    filename = "filename_to_checksum_dict.db"
    #main(filename)
    update(filename)


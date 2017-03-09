#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to search for files in stores webstorage archives
"""
import os
import time
import sys
import socket
import argparse
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import json
import sqlite3
import dateutil.parser
import mimetypes
# own modules
from webstorage import WebStorageArchive as WebStorageArchive

class NtoM(object):
    """
    build n : m dependent key value stores
    """

    def __init__(self, keyname1, keyname2):
        """
        initilize and name the two keys to store
        """
        self.__keyname1 = keyname1
        self.__keyname2 = keyname2
        self.__data = {
            self.__keyname1 : {},
            self.__keyname2 : {}
            }
        self.__dirty = False # indicate if data is modified in memory

    def add(self, **kwds):
        """
        add new key:value pair, automatically add also value:key pair
        """
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
    """
    update or create local webstorage index database
    """
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
    mime_to_absfile = NtoM("mime_type", "absfile")
    mime_to_absfile.load(filename)
    backupsets_done = [row[0] for row in cur.execute("select backupset from backupsets_done").fetchall()]
    for backupset in backupsets:
        starttime = time.time()
        if backupset in backupsets_done:
            print(" backupset %s already done" % backupset)
            continue
        hostname, tag, isoformat_ext = backupset.split("_")
        isoformat = isoformat_ext[:-9]
        datestring = dateutil.parser.parse(isoformat)
        print(hostname, tag, dateutil.parser.parse(isoformat))
        data = wsa.get(backupset)
        for absfile in data["filedata"].keys():
            mime_type, content_encoding = mimetypes.guess_type(absfile)
            if mime_type is not None:
                mime_to_absfile.add(absfile=absfile, mime_type=mime_type)
            checksum = data["filedata"][absfile]["checksum"]
            filename_to_checksum.add(absfile=absfile, checksum=checksum)
            filename_to_backupset.add(absfile=absfile, backupset=backupset)
        cur.execute("insert into backupsets_done values (?)", (backupset,))
        conn.commit()
        logging.info(" done in %0.2f s", time.time()-starttime)
    filename_to_checksum.save(filename)
    filename_to_backupset.save(filename)
    mime_to_absfile.save(filename)


def search_name(filename, pattern, exact):
    """
    search for some pattern in database, exact or like
    """
    this_pattern = pattern
    if exact is False:
        this_pattern = "%%%s%%" % pattern
    logging.debug("searching for pattern %s", this_pattern)
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    data = cur.execute("select absfile, checksum from absfile_to_checksum where absfile like ?", (this_pattern,)).fetchall()
    for row in data:
        absfile = row[0]
        logging.info(absfile)
        for checksum in json.loads(row[1]):
            logging.info("\twith checksum %s", checksum)
            backupsets = json.loads(cur.execute("select backupset from absfile_to_backupset where absfile=?", (absfile, )).fetchall()[0][0])
            for backupset in backupsets:
                logging.info("\t\tfound in %s", backupset)
    logging.info("found %d occurances", len(data))

def search_checksum(filename, pattern):
    """
    search for some pattern in database, exact or like
    """
    this_pattern = pattern
    logging.debug("searching for pattern %s", this_pattern)
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    data = cur.execute("select checksum, absfile from checksum_to_absfile where checksum=?", (this_pattern,)).fetchall()
    for row in data:
        checksum = row[0]
        logging.info(checksum)
        for absfile in json.loads(row[1]):
            logging.info("\t%s", absfile)
            backupsets = json.loads(cur.execute("select backupset from absfile_to_backupset where absfile=?", (absfile, )).fetchall()[0][0])
            for backupset in backupsets:
                logging.info("\t\tfound in %s", backupset)
    logging.info("found %d occurances", len(data))

def search_mime_type(filename, pattern):
    """
    search for some pattern in database, exact or like
    """
    this_pattern = pattern
    logging.debug("searching for pattern %s", this_pattern)
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    data = cur.execute("select mime_type, absfile from mime_type_to_absfile where mime_type=?", (this_pattern,)).fetchall()
    for row in data:
        logging.info(row[0])
        for absfile in json.loads(row[1]):
            logging.info("\t%s", absfile)
    logging.info("found %d occurances", len(data))


def main():
    """
    parse commandline and to something useful
    """
    parser = argparse.ArgumentParser(description='search for files and checksum in local index database')
    parser.add_argument("--update", action="store_true", default=False, help="create or update local index database", required=False)
    parser.add_argument("-c", "--checksum", help="search for checksum", required=False)
    parser.add_argument("-n", '--name', help="search for name", required=False)
    parser.add_argument("-m", '--mime-type', help="search by mime-type", required=False)
    parser.add_argument("-b", '--backupset', help="search for backupset", required=False)
    parser.add_argument("-x", '--exact', action="store_true", default=False, help="dont use wildcard search, use the provided argument exactly", required=False)
    parser.add_argument('-d', '--database', default="~/.webstorage/searchindex.db", help="sqlite3 database to use", required=False)
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR", required=False)
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG", required=False)
    args = parser.parse_args()
    database = os.path.expanduser(args.database) # expand user directory in path
    if os.path.isfile(database):
        logging.info("using database %s", database)
    else:
        logging.info("first use, creating database %s, you should run --update first", database)
    if args.update is True:
        update(database)
    if args.name is not None:
        search_name(database, args.name, args.exact)
    elif args.checksum is not None:
        search_checksum(database, args.checksum)
    elif args.mime_type is not None:
        search_mime_type(database, args.mime_type)


if __name__ == "__main__":
    main()

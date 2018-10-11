#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to search for files in stores webstorage archives
"""
import os
import time
import datetime
import sys
import socket
import argparse
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import json
import dbm
import mimetypes
# own modules
from webstorage import WebStorageArchiveClient as WebStorageArchiveClient

def main():
    """
    update or create local webstorage index database
    """
    print(dbm.whichdb("test.dbm"))
    with dbm.open("test.dbm", "cf") as db:
        db.reorganize()
        checksum = db.firstkey()
        while checksum is not None:
            print(checksum, len(json.loads(db[checksum])))
            checksum = db.nextkey(checksum)
        print("checksum 771be3eaed13f0f88889a78ec9f6df76f34f2161")
        for backupset in json.loads(db["771be3eaed13f0f88889a78ec9f6df76f34f2161"]):
            print("\t%s" % backupset)

def main1():
    """
    update or create local webstorage index database
    """
    print(dbm.whichdb("test.dbm"))
    with dbm.open("test.dbm", "cf") as db:
        myhostname = socket.gethostname()
        wsa = WebStorageArchiveClient()
        backupsets = wsa.get_backupsets(myhostname)
        for backupset in backupsets:
            # dict struct like
            # {
            #  'date': '2016-10-05',
            #  'time': '23:00:00',
            #  'datetime': '2016-10-05T23:00:00.000000',
            #  'size': 303771,
            #  'tag': 'privat',
            #  'basename': 'wse0000107_privat_2016-10-05T23:00:00.000000.wstar.gz'
            # }
            print(backupset)
            with dbm.open("backupset_done.dbm", "c") as db_backupsets:
                if backupset["basename"] in db_backupsets:
                    print("already done")
                    continue
            starttime = time.time()
            hostname, tag, isoformat_ext = backupset["basename"].split("_")
            print(hostname, tag, backupset["date"], backupset["time"])
            data = wsa.get(backupset["basename"])
            for absfile in data["filedata"].keys():
                mime_type, content_encoding = mimetypes.guess_type(absfile)
                # if mime_type is not None:
                #     mime_to_absfile.add(absfile=absfile, mime_type=mime_type)
                checksum = data["filedata"][absfile]["checksum"]
                if checksum not in db:
                    print("%s first appeared in %s" % (checksum, backupset["basename"]))
                    db[checksum] = json.dumps([backupset["basename"], ])
                else:
                    old = json.loads(db[checksum])
                    if backupset["basename"] not in old: # update only if not in list
                        print("%s appending backupset %s" % (checksum, backupset["basename"]))
                        old.append(backupset["basename"])
                        db[checksum] = json.dumps(old)
            logging.info(" done in %0.2f s", time.time()-starttime)
            with dbm.open("backupset_done.dbm", "c") as db_backupsets:
                if backupset["basename"] not in db_backupsets:
                    db_backupsets[backupset["basename"]] = datetime.datetime.now().isoformat()
            db.reorganize()

def main1():
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

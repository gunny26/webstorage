#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to search for files in stores webstorage archives
"""
import os
import io
import time
import sys
import socket
import argparse
import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("BlockStorageClient").setLevel(logging.INFO)
logging.getLogger("FileStorageClient").setLevel(logging.INFO)
import json
import sqlite3
import dateutil.parser
import hashlib
import mimetypes
from PIL import Image
import PIL.ExifTags
import PIL.IptcImagePlugin
# own modules
from webstorage import WebStorageArchive as WebStorageArchive
from webstorage import FileStorageClient as FileStorageClient


class Associator(object):

    def __init__(self):
        self.__data = {}
        self.__whitelist = ("Make", "Model", "ExifImageHeight", "ExifImageWidth", "Date", "Year")

    def add(self, key, v_dict):
        for v_key, v_value in v_dict.items():
            if v_key not in self.__whitelist:
                continue
            if v_key in self.__data.keys():
                if v_value in self.__data[v_key].keys():
                    if key not in self.__data[v_key][v_value]:
                        self.__data[v_key][v_value].append(key)
                    else:
                        print("key already exists")
                else:
                    self.__data[v_key][v_value] = [key, ]
            else:
                self.__data[v_key] = {
                        v_value : [key, ]
                }

    def keys(self):
        return self.__data.keys()

    def __getitem__(self, v_key):
        return self.__data[v_key]

    def save(self, filename):
        json.dump(self.__data, open(filename, "w"), indent=4)

    def load(self, filename):
        self.__data = json.load(open(filename, "r"))


class ImageDb(object):

    def __init__(self, key1, key2):
        self.__key1 = key1
        self.__key2 = key2
        self.__data = {
            self.__key1 : {},
            self.__key2 : {}
        }

    def add(self, value1, value2):
        if value1 not in self.__data[self.__key1].keys():
            self.__data[self.__key1][value1] = []
            if value2 not in self.__data[self.__key1][value1]:
                self.__data[self.__key1][value1].append(value2)
        if value2 not in self.__data[self.__key2].keys():
            self.__data[self.__key2][value2] = []
            if value1 not in self.__data[self.__key2][value2]:
                self.__data[self.__key2][value2].append(value1)

    def save(self, filename):
        json.dump(self.__data, open(filename, "w"), indent=4)

    def load(self, filename):
        self.__data = json.load(open(filename, "r"))

    def exists(self, key, value):
        assert key in (self.__key1, self.__key2)
        return value in self.__data[key].keys()


def search(filename, pattern):
    """
    search for some pattern in database, exact or like
    """
    fs = FileStorageClient()
    imagedb = ImageDb("md5", "sha256")
    if os.path.isfile("imagedb_test.json"):
        imagedb.load("imagedb_test.json")
    assi = Associator()
    if os.path.isfile("assi_test.json"):
        assi.load("assi_test.json")
    conn = sqlite3.connect(filename)
    cur = conn.cursor()
    sql_string = """
    select
        absfile_to_checksum.checksum,
        absfile_to_checksum.absfile,
        absfile_to_mime_type.mime_type
    from
        absfile_to_checksum,
        absfile_to_mime_type
    where
        absfile_to_checksum.absfile = absfile_to_mime_type.absfile"""
    data = cur.execute(sql_string).fetchall()
    counter = 0
    for row in data:
        checksum = eval(row[0])[0]
        absfile = row[1]
        #if absfile != "/home/mesznera/Bilder/2017/06/09/IMG_20170609_143842386_BURST009.jpg":
        #    continue
        mime_type = eval(row[2])[0]
        if mime_type == "image/jpeg":
            print(checksum, absfile, mime_type)
            if imagedb.exists("md5", checksum):
                print("data already available, skipping this image")
                continue
            image_io = io.BytesIO()
            f_sha256 = hashlib.sha256()
            f_sha256.update(image_io.read())
            startts = time.time()
            for data in fs.read(checksum):
                image_io.write(data)
            try:
                image = Image.open(image_io)
            except OSError as exc:
                print(exc)
                continue
            if image is not None:
                print("\timage load done in %0.6fs" %(time.time() - startts))
                if min(image.size) < 1024:
                    print("\tSkipping, this image seems to be to small")
                    continue
                i_sha256 = hashlib.sha256()
                i_sha256.update(image.tobytes())
                # get exif data if available
                exif_data = image._getexif()
                exif = {}
                if exif_data is not None:
                    exif = {PIL.ExifTags.TAGS[k]: v for k, v in exif_data.items() if k in PIL.ExifTags.TAGS}
                    if "GPSInfo" in exif.keys():
                        gpsinfo = {}
                        for key in exif['GPSInfo'].keys():
                            decode = PIL.ExifTags.GPSTAGS.get(key,key)
                            gpsinfo[decode] = exif['GPSInfo'][key]
                        exif.update(gpsinfo)
                # add ipct tags, if available
                ipct_data = PIL.IptcImagePlugin.getiptcinfo(image)
                if ipct_data is not None:
                    print(ipct_data)
                    if (2, 25) in ipct_data:
                        exif["Keywords"] = ipct_data[(2, 25)]
                #for key in sorted(exif.keys()):
                #    if key in ("MakerNote", "GPSInfo"):
                #        continue
                #    print("\t%s : %s" % (key, exif[key]))
                if "DateTimeOriginal" in exif.keys():
                    # type str looks like 2014:08:10 14:39:13
                    exif["Date"] = exif["DateTimeOriginal"].split(" ")[0].replace(":", "-")
                    exif["Year"] = exif["DateTimeOriginal"].split(":")[0]
                assi.add(i_sha256.hexdigest(), exif)
                imagedb.add(checksum, i_sha256.hexdigest())
                counter += 1
        if counter == 200:
            break
    assi.save("assi_test.json")
    imagedb.save("imagedb_test.json")
    for v_key in assi.keys():
        print(v_key)
        for value in assi[v_key].keys():
            print("\t%s :%d" % (value, len(assi[v_key][value])))

def main():
    """
    parse commandline and to something useful
    """
    parser = argparse.ArgumentParser(description='search for files and checksum in local index database')
    parser.add_argument("--update", action="store_true", default=False, help="create or update local index database", required=False)
    parser.add_argument("-c", "--checksum", help="search for checksum", required=False)
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
    search(database, "image/jpeg")


if __name__ == "__main__":
    main()

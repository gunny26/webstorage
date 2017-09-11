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
        self.__whitelist = ("DateTimeOriginal", "Make", "Model", "ExifImageHeight", "ExifImageWidth")

    def add(self, key, v_dict):
        for v_key, v_value in v_dict.items():
            if v_key not in self.__whitelist:
                continue
            if v_key in self.__data.keys():
                if v_value in self.__data[v_key].keys():
                    self.__data[v_key][v_value].append(key)
                else:
                    self.__data[v_key][v_value] = [key, ]
            else:
                self.__data[v_key] = {
                        v_value : [key, ]
                }

    def __str__(self):
        return str(self.__data)

def search(filename, pattern):
    """
    search for some pattern in database, exact or like
    """
    fs = FileStorageClient()
    assi = Associator()
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
            image_io = io.BytesIO()
            for data in fs.read(checksum):
                image_io.write(data)
            try:
                image = Image.open(image_io)
            except OSError as exc:
                print(exc)
                continue
            f_sha256 = hashlib.sha256()
            f_sha256.update(image_io.read())
            print("File checksum : %s" % f_sha256.hexdigest())
            if image is not None:
                print(image)
                i_sha256 = hashlib.sha256()
                i_sha256.update(image.tobytes())
                print("Pixel checksum : %s" % i_sha256.hexdigest())
                exif_data = image._getexif()
                exif = {}
                if exif_data is not None:
                    exif = {PIL.ExifTags.TAGS[k]: v for k, v in exif_data.items() if k in PIL.ExifTags.TAGS}
                    # exif = convert_exif_to_dict(exif_data)
                    if "GPSInfo" in exif.keys():
                        gpsinfo = {}
                        for key in exif['GPSInfo'].keys():
                            decode = PIL.ExifTags.GPSTAGS.get(key,key)
                            gpsinfo[decode] = exif['GPSInfo'][key]
                        exif.update(gpsinfo)
                ipct_data = PIL.IptcImagePlugin.getiptcinfo(image)
                if ipct_data is not None:
                    print(ipct_data)
                    if (2, 25) in ipct_data:
                        exif["Keywords"] = ipct_data[(2, 25)]
                for key in sorted(exif.keys()):
                    if key in ("MakerNote", "GPSInfo"):
                        continue
                    print("\t%s : %s" % (key, exif[key]))
                assi.add(i_sha256.hexdigest(), exif)
                counter += 1
                print(counter)
        if counter == 20:
            break
    print(json.dumps(assi, indent=4))


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

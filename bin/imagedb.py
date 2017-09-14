#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to search for files in stores webstorage archives
"""
import os
import io
import time
import datetime
import sys
import socket
import argparse
import requests
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

    def __init__(self, filename, autosave=100):
        self.__data = {}
        self.__whitelist = ("Make", "Model", "ExifImageHeight",
            "ExifImageWidth", "Date", "Year", "Month", "DayOfMonth",
            "Weekday", "Keywords","Orientation", "ColorHue", "ColorSaturation", "ColorValue",
            "HasGPSInfo", "TimeOfDay", "HasExifInfo", "HasIpctInfo")
        self.__filename = filename
        self.__autosave = autosave
        self.__autosave_counter = autosave

    def add(self, key, v_dict):
        for v_key, v_value in v_dict.items():
            if isinstance(v_value, list):
                for entry in v_value:
                    self.add(key, {v_key : entry})
            else:
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
                # periodically save data to disc
                self.__autosave_counter -= 1
                if self.__autosave == 0:
                    self.save()

    def keys(self):
        return self.__data.keys()

    def __getitem__(self, v_key):
        return self.__data[v_key]

    def save(self):
        json.dump(self.__data, open(self.__filename, "w"), indent=4)
        self.__autosave_counter = self.__autosave

    def load(self):
        self.__data = json.load(open(self.__filename, "r"))


class PersistentNtoM(object):

    def __init__(self, key1, key2, filename, autosave=100):
        self.__key1 = key1
        self.__key2 = key2
        self.__data = {
            self.__key1 : {},
            self.__key2 : {}
        }
        self.__filename = filename
        self.__autosave = autosave
        self.__autosave_counter = autosave

    def __len__(self):
        return max(len(self.__data[self.__key1]), len(self.__data[self.__key2]))

    def add(self, value1, value2):
        if value1 not in self.__data[self.__key1].keys():
            self.__data[self.__key1][value1] = []
            if value2 not in self.__data[self.__key1][value1]:
                self.__data[self.__key1][value1].append(value2)
        if value2 not in self.__data[self.__key2].keys():
            self.__data[self.__key2][value2] = []
            if value1 not in self.__data[self.__key2][value2]:
                self.__data[self.__key2][value2].append(value1)
        self.__autosave_counter -= 1
        if self.__autosave == 0:
            self.save()

    def save(self):
        json.dump(self.__data, open(self.__filename, "w"), indent=4)
        self.__autosave_counter = self.__autosave

    def load(self):
        self.__data = json.load(open(self.__filename, "r"))

    def exists(self, key, value):
        assert key in (self.__key1, self.__key2)
        return value in self.__data[key].keys()


class PersistentList(object):

    def __init__(self, filename, autosave=10):
        self.__filename = filename
        self.__autosave = autosave
        self.__autosave_counter = autosave
        self.__data = []

    def append(self, value):
        self.__data.append(value)
        self.__autosave_counter -= 1
        if self.__autosave_counter == 0:
            self.save()

    def save(self):
        json.dump(self.__data, open(self.__filename, "w"))
        self.__autosave_counter = self.__autosave

    def load(self):
        self.__data = json.load(open(self.__filename, "r"))

    def __getitem__(self, index):
        return self.__data[index]

    def __len__(self):
        return len(self.__data)


class PersistentDict(object):

    def __init__(self, filename, autosave=10):
        self.__filename = filename
        self.__autosave = autosave
        self.__autosave_counter = autosave
        self.__data = {}

    def __setitem__(self, key, value):
        if key not in self.__data.keys():
            self.__data[key] = [value, ]
        else:
            self.__data[key].append(value)
        self.__autosave_counter -= 1
        if self.__autosave_counter == 0:
            self.save()

    def save(self):
        json.dump(self.__data, open(self.__filename, "w"))
        self.__autosave_counter = self.__autosave

    def load(self):
        self.__data = json.load(open(self.__filename, "r"))

    def __getitem__(self, index):
        return self.__data[index]


def search(filename, pattern):
    """
    search for some pattern in database, exact or like
    """
    fs = FileStorageClient()
    imagedb = PersistentNtoM("md5", "sha256", "imagedb_test.json")
    skipped = PersistentList("skipped_test.json")
    gpsdb = PersistentNtoM("gps", "sha256", "gpsdb_test.json")
    if os.path.isfile("skipped_test.json"):
        skipped.load()
    if os.path.isfile("imagedb_test.json"):
        imagedb.load()
    assi = Associator("assi_test.json")
    if os.path.isfile("assi_test.json"):
        assi.load()
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
        mime_type = eval(row[2])[0]
        # start processing
        if checksum in skipped:
            continue
        if mime_type == "image/jpeg":
            print(checksum, absfile, mime_type)
            if imagedb.exists("md5", checksum):
                print("\tdata already available, skipping this image")
                continue
            image_io = io.BytesIO()
            f_sha256 = hashlib.sha256()
            f_sha256.update(image_io.read())
            startts = time.time()
            try:
                for data in fs.read(checksum):
                    image_io.write(data)
            except Exception as exc:
                print("Exception %s while getting data for file with this checksum" % exc)
                continue
            try:
                image = Image.open(image_io)
            except OSError as exc:
                print("Exception %s while opening the image" % exc)
                skipped.append(checksum)
                continue
            if image is not None:
                print("\timage load done in %0.6fs" %(time.time() - startts))
                startts = time.time()
                if min(image.size) < 1024:
                    print("\tSkipping, this image seems to be to small")
                    skipped.append(checksum)
                    continue
                i_sha256 = hashlib.sha256()
                i_sha256.update(image.tobytes())
                # get exif data if available
                if not isinstance(image, PIL.JpegImagePlugin.JpegImageFile):
                    print("\tSeems not to be an jpeg file, skipping")
                    skipped.append(checksum)
                    continue
                meta = {
                    "ExifImageHeight" : image.height,
                    "ExifImageWidth" : image.width
                    }
                # EXIF Data
                exif_data = image._getexif()
                if exif_data is not None:
                    meta["HasEfixInfo"] = True
                    meta.update({PIL.ExifTags.TAGS[k]: v for k, v in exif_data.items() if k in PIL.ExifTags.TAGS})
                    if "GPSInfo" in meta.keys():
                        gpsinfo = {}
                        for key in meta["GPSInfo"].keys():
                            decode = PIL.ExifTags.GPSTAGS.get(key, key)
                            gpsinfo[decode] = meta["GPSInfo"][key]
                        meta.update(gpsinfo)
                        if "GPSLatitude" in meta:
                            def dms2dd(dms, direction=None):
                                degrees, minutes, seconds = dms
                                return float(degrees) + float(minutes) / 60 + float(seconds) / (60 * 60);
                                #if direction == 'E' or direction == 'N':
                                #    dd *= -1
                            meta["Latitude"] = dms2dd((float(value)/divisor for value, divisor in meta["GPSLatitude"]))
                            meta["Longitude"] = dms2dd((float(value)/divisor for value, divisor in meta["GPSLongitude"]))
                        meta["HasGPSInfo"] = True
                    else:
                        meta["HasGPSInfo"] = False
                else:
                    meta["HasEfixInfo"] = False
                # orientation
                if image.height > image.width:
                    meta["Orientation"] = "portrait"
                elif image.height < image.width:
                    meta["Orientation"] = "landscape"
                else:
                    meta["Orientation"] = "none"
                # add ipct tags, if available
                ipct_data = PIL.IptcImagePlugin.getiptcinfo(image)
                if ipct_data is not None:
                    meta["HasIpctInfo"] = True
                    if (2, 25) in ipct_data.keys():
                        if isinstance(ipct_data[(2, 25)], list):
                            meta["Keywords"] = [entry.decode("utf-8") for entry in ipct_data[(2, 25)]]
                        else:
                            meta["Keywords"] = ipct_data[(2, 25)].decode("utf-8")
                        # print(exif["Keywords"])
                else:
                    meta["HasIpctInfo"] = False
                # date and time
                if "DateTimeOriginal" in meta.keys():
                    # type str looks like 2014:08:10 14:39:13
                    timeofday = meta["DateTimeOriginal"].split(" ")[1]
                    hour = int(timeofday.split(":")[0])
                    if hour < 6:
                        meta["TimeOfDay"] = "Night"
                    elif 6 <= hour < 10:
                        meta["TimeOfDay"] = "Morning"
                    elif 10 <= hour < 14:
                        meta["TimeOfDay"] = "Midday"
                    elif 14 <= hour < 18:
                        meta["TimeOfDay"] = "Afternoon"
                    elif 18 <= hour < 22:
                        meta["TimeOfDay"] = "Evening"
                    elif 22 <= hour:
                        meta["TimeOfDay"] = "Night"
                    meta["Date"] = meta["DateTimeOriginal"].split(" ")[0].replace(":", "-")
                    meta["Year"] = int(meta["Date"].split("-")[0])
                    meta["Month"] = int(meta["Date"].split("-")[1])
                    meta["DayOfMonth"] = int(meta["Date"].split("-")[2])
                    meta["Weekday"] = datetime.date(meta["Year"], meta["Month"], meta["DayOfMonth"]).weekday()
                image_hsv = image.resize((1,1)).convert("HSV")
                # print(image_hsv)
                meta["ColorHue"], meta["ColorSaturation"], meta["ColorValue"] = image_hsv.getpixel((0,0))
                # print("\tmedium color of image is %s" % str(image_hsv.getpixel((0,0))))
                # print("\tduration to resize image %0.6fs" % (time.time() - startts))
                # print final exif information
                #for key in sorted(exif.keys()):
                #    if key in ("MakerNote", "GPSInfo"):
                #        continue
                #    print("\t%s : %s" % (key, exif[key]))
                assi.add(i_sha256.hexdigest(), meta)
                imagedb.add(checksum, i_sha256.hexdigest())
                if "Latitude" in meta.keys():
                    gpsdb.add(str((meta["Latitude"], meta["Longitude"])), i_sha256.hexdigest())
                print("\tgetting metadata and storing done in %0.6fs" % (time.time() - startts))
                counter += 1
                #if counter == 200:
                #    break
    assi.save()
    imagedb.save()
    gpsdb.save()
    skipped.save()
    for v_key in assi.keys():
        print(v_key)
        for value in assi[v_key].keys():
            print("\t%s :%d" % (value, len(assi[v_key][value])))
    print("image checksum to filestorage checksums %s" % len(imagedb))
    print("image checksum to gps %s" % len(gpsdb))
    print("skipped checksums %s" % len(skipped))

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
    search(database, "image/jpeg")


if __name__ == "__main__":
    main()

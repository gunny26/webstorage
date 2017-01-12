#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import os
import sys
import re
import json
import hashlib
import logging
import requests
import base64


CONFIG = {}
HOMEPATH = os.path.expanduser("~/.webstorage")
if not os.path.isdir(HOMEPATH):
    print("first create INI file in directory {}".format(HOMEPATH))
    sys.exit(1)
else:
    for line in open(os.path.join(HOMEPATH, "WebStorageClient.ini"), "r"):
        key, value = line.strip().split("=")
        CONFIG[key] = value


class HTTPError(Exception):
    """indicates general exception"""
    pass


class HTTP404(Exception):
    """indicates not found"""
    pass


class WebStorageArchive(object):
    """
    store and retrieve S3 Data, specific for WebStorageArchives
    """

    def __init__(self):
        """
        bucket <str> S3 bucket name
        path <str> Path
        """
        self.__url = CONFIG["URL_WEBSTORAGE_ARCHIVE"]
        self.__session = requests.Session()
        self.__headers = {
            "x-auth-token" : CONFIG["APIKEY_WEBSTORAGE_ARCHIVE"]
        }

    def get_backupsets(self, hostname):
        """
        return data of available backupsets for this specific hostname
        """
        url = self.__url + "/"
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            result = {}
            rex = re.compile(r"^(.+)_(.+)_(.+)\.wstar\.gz$")
            for basename, value in res.json().items():
                size = value["size"]
                match = rex.match(basename)
                if match is not None:
                    thishostname = match.group(1)
                    tag = match.group(2)
                    timestamp = match.group(3)
                    # 2016-10-25T20:23:17.782902
                    thisdate, thistime = timestamp.split("T")
                    thistime = thistime.split(".")[0]
                    if hostname == thishostname:
                        result[basename] = {
                            "date": thisdate,
                            "time" : thistime,
                            "size" : size,
                            "tag" : tag,
                            "basename" : basename
                        }
            return result
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get_latest_backupset(self, hostname):
        """
        get the latest backupset stored on s3

        hostname <str>
        """
        backupsets = self.get_backupsets(hostname)
        if len(backupsets) > 0:
            latest = sorted(backupsets)[-1]
            filename = backupsets[latest]["basename"]
            logging.info("latest backupset found %s", filename)
            return filename
        logging.error("no backupsets found")

    def get(self, filename):
        """
        return filename
        """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        url = "/".join((self.__url, filename64.decode("utf-8")))
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def put(self, data, filename):
        """
        return filename
        """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        url = "/".join((self.__url, filename64.decode("utf-8")))
        logging.debug("PUT %s", url)
        res = self.__session.put(url, headers=self.__headers, data=json.dumps(data))
        if res.status_code == 200:
            return res
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import os
import sys
import re
import json
import logging
import base64
import requests
# own modules
from Config import get_config


class WebStorageArchiveClient(object):
    """
    store and retrieve Data, specific for WebStorageArchives
    """

    __version = "1.1"

    def __init__(self):
        """ __init__ """
        self.__logger = logging.getLogger(self.__class__.__name__)
        config = get_config()
        self.__url = config["URL_WEBSTORAGE_ARCHIVE"]
        self.__session = requests.Session()
        self.__headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self.__version),
            "x-auth-token" : config["APIKEY_WEBSTORAGE_ARCHIVE"],
            "x-apikey" : config["APIKEY_WEBSTORAGE_ARCHIVE"]
        }
        # if HTTPS_PROXY is set in config file use this information
        if "HTTPS_PROXY" in config:
            self.__proxies = {"https": config["HTTPS_PROXY"]}
            self.__logger.debug("using HTTPS_PROXY %s", self.__proxies)
        else:
            self.__proxies = {}

    def __request(self, method, path="", data=None):
        """
        single point of request
        """
        res = self.__session.request(method, "/".join((self.__url, path)), data=data, headers=self.__headers, proxies=self.__proxies)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)

    def __get_json(self, path=""):
        """
        single point of json requests
        """
        res = self.__request("get", path)
        # hack to be compatible with older requests versions
        try:
            return res.json()
        except TypeError:
            return res.json

    def get_backupsets(self, hostname):
        """
        return data of available backupsets for this specific hostname
        """
        result = {}
        rex = re.compile(r"^(.+)_(.+)_(.+)\.wstar\.gz$")
        for basename, value in self.__get_json().items():
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

    def get_latest_backupset(self, hostname):
        """
        get the latest backupset stored

        hostname <str>
        """
        backupsets = self.get_backupsets(hostname)
        if backupsets:
            latest = sorted(backupsets)[-1]
            filename = backupsets[latest]["basename"]
            self.__logger.info("latest backupset found %s", filename)
            return filename
        self.__logger.error("no backupsets found")

    def get(self, filename):
        """
        return filename
        """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self.__get_json(filename64.decode("utf-8"))

    def put(self, data, filename):
        """
        return filename
        """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self.__request("put", filename64.decode("utf-8"), data=json.dumps(data))

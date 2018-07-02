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
from webstorage.Config import get_config
from webstorage.WebStorageClient import WebStorageClient


class WebStorageArchiveClient(WebStorageClient):
    """
    store and retrieve Data, specific for WebStorageArchives
    """

    def __init__(self):
        """ __init__ """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._config = get_config()
        self._url = self._config["URL_WEBSTORAGE_ARCHIVE"]
        self._apikey = self._config["APIKEY_WEBSTORAGE_ARCHIVE"]
        super().__init__()

    def get_backupsets(self, hostname):
        """
        return data of available backupsets for this specific hostname
        """
        result = {}
        rex = re.compile(r"^(.+)_(.+)_(.+)\.wstar\.gz$")
        for basename, value in self._get_json().items():
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
            self._logger.info("latest backupset found %s", filename)
            return filename
        self._logger.error("no backupsets found")

    def get(self, filename):
        """ get archive """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self._get_json(filename64.decode("utf-8"))

    def put(self, data, filename):
        """ put archive """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self._request("put", filename64.decode("utf-8"), data=json.dumps(data))

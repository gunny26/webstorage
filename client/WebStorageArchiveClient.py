#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import re
import json
import logging
import base64
# own modules
from webstorageClient.ClientConfig import ClientConfig
from webstorageClient.WebStorageClient import WebStorageClient


class WebStorageArchiveClient(WebStorageClient):
    """
    store and retrieve Data, specific for WebStorageArchives
    """

    def __init__(self, url=None, apikey=None):
        """ __init__ """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client_config = ClientConfig()
        if not url:
            self._url = self._client_config.archive_url
        else:
            self._url = url
        if not apikey:
            self._apikey = self._client_config.archive_apikey
        else:
            self._apikey = apikey
        super().__init__()

    def get_backupsets(self, hostname=None):
        """
        get all available backupsets
        works like directory listing of *.wstar.gz
        returns data sorted by datetime of filename

        returns:
        <list>
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
                if hostname and hostname != thishostname: # filter only backupsets for this hostname
                    continue
                result[basename] = {
                    "date": thisdate,
                    "time" : thistime,
                    "datetime" : timestamp,
                    "size" : size,
                    "tag" : tag,
                    "basename" : basename
                }
        # sort by datetime
        return sorted(result.values(), key=lambda a: a["datetime"])

    def get_latest_backupset(self, hostname=None):
        """
        get the latest backupset stored shorthand function to get_backupsets

        parmeters:
        hostname <str>

        returns:
        <str>
        """
        try:
            return self.get_backupsets(hostname)[-1]["basename"]
        except IndexError:
            pass

    def get(self, filename):
        """ get specific archive """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self._get_json(filename64.decode("utf-8"))

    def put(self, data, filename):
        """ put archive """
        filename64 = base64.b64encode(filename.encode("utf-8"))
        return self._request("put", filename64.decode("utf-8"), data=json.dumps(data))

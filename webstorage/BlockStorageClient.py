#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import logging
# own modules
from webstorage.Config import get_config
from webstorage.WebStorageClient import WebStorageClient

class BlockStorageClient(WebStorageClient):
    """stores chunks of data into BlockStorage"""

    def __init__(self, cache=True):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._config = get_config()
        self._url = self._config["URL_BLOCKSTORAGE"]
        self._apikey = self._config["APIKEY_BLOCKSTORAGE"]
        super().__init__()
        # get info from backend
        self._cache = cache # cache blockdigests or not
        self._info = self._get_json("info")
        self._checksums = set()

    @property
    def blocksize(self):
        return int(self._info["blocksize"])

    @property
    def checksums(self):
        if (self._cache == True) and (not self._checksums):
            self._logger.info("getting existing checksums")
            self._checksums = set(self._get_json())
        return self._checksums

    def put(self, data, use_cache=False):
        """put some arbitrary data into storage"""
        assert len(data) <= self.blocksize
        checksum = self._blockdigest(data)
        if use_cache and checksum in self.checksums:
            self._logger.info("202 - skip this block, checksum is in list of stored checksums")
            return (checksum, 202)
        else:
            res = self._request("put", checksum, data=data)
            if res.status_code == 201:
                self._logger.info("201 - block rewritten")
            if res.text != checksum:
                raise AssertionError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
            self._checksums.add(checksum) # add to local cache
            return res.text, res.status_code

    def get(self, checksum):
        """get data defined by hexdigest from storage"""
        res = self._request("get", checksum)
        return res.content

    def get_verify(self, checksum):
        """get data defined by hexdigest from storage"""
        res = self._request("get", checksum)
        data = res.content
        if checksum != self._blockdigest(data):
            raise AssertionError("Checksum mismatch %s requested, %s get" % (checksum, self._blockdigest(data)))
        return data

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the backend is queried
        """
        try:
            if checksum in self._checksums or self._request("options", checksum).status_code == 200:
                return True
        except KeyError:
            pass
        return False

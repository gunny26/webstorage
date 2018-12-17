#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import requests
import logging
# own modules
from webstorageClient.ClientConfig import ClientConfig
from webstorageClient.WebStorageClient import WebStorageClient

class BlockStoragError(Exception):
    pass

class BlockStorageClient(WebStorageClient):
    """stores chunks of data into BlockStorage"""

    def __init__(self, url=None, apikey=None, cache=True):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client_config = ClientConfig()
        if url is None:
            self._url = self._client_config.blockstorage_url
        else:
            self._url = url
        if apikey is None:
            self._apikey = self._client_config.blockstorage_apikey
        else:
            self._apikey = apikey
        super().__init__()
        # get info from backend
        self._cache = cache # cache blockdigests or not
        self._info = self._get_json("info")
        self._cachefile = "%s_%s.bin" % (self._info["id"], self._info["blockchain_epoch"])
        self._cachefile = os.path.join(self._client_config.homepath, self._cachefile)
        self._checksums = set()

    @property
    def blocksize(self):
        return int(self._info["blocksize"])

    @property
    def info(self):
        return self._info

    @property
    def checksums(self):
        if (self._cache == True) and (not self._checksums):
            self._logger.info("getting existing checksums")
            self._checksums = set(self._get_json())
        return self._checksums

    def _get_chunked(self, method, data=None):
        """
        call url and received chunked content to yield
        """
        url = "/".join((self._url, method))
        if data is not None:
            self._logger.info("adding search parameters : %s", data)
        self._logger.info("calling %s", url)
        res = requests.get(url, params=data, headers=self._headers, proxies=self._proxies, stream=True)
        logging.debug("got Status code : %s", res.status_code)
        if res.status_code == 200:
            return res
        elif res.status_code == 404:
            raise KeyError("HTTP 404 received")
        else:
            raise Exception("got status %d for call to %s" % (res.status_code, url))

    def get_info(self):
        """
        get some information about backend
        """
        self._info = self._get_json("info")
        return self._info

    def get_epoch(self, epoch):
        """
        return blockchain data at epoch
        """
        return self._get_json("epoch/%d" % epoch)

    def get_journal(self, epoch):
        """
        return blockchain journal starting at epoch
        """
        return self._get_json("journal/%d" % epoch)

    def get_checksums(self, epoch):
        """
        write binary blob of checksums to cachefile
        """
        res = self._get_chunked("checksums/%d" % epoch)
        with open(self._cachefile, "wb") as outfile:
            for chunk in res:
                outfile.write(chunk)

    def put(self, data, use_cache=False):
        """put some arbitrary data into storage"""
        if len(data) > self.blocksize: # assure maximum length
            raise BlockStorageError("length of providede data (%s) is above maximum blocksize of %s" % (len(data), self.blocksize))
        checksum = self._blockdigest(data)
        if use_cache and checksum in self.checksums:
            self._logger.debug("202 - skip this block, checksum is in list of stored checksums")
            return (checksum, 202)
        else:
            res = self._request("put", checksum, data=data)
            if res.status_code == 201:
                self._logger.debug("201 - block rewritten")
            if res.text != checksum:
                raise BlockStorageError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
            self._checksums.add(checksum) # add to local cache
            return res.text, res.status_code

    def get(self, checksum, verify=False):
        """
        get data defined by hexdigest from storage
        if verify - recheck checksum locally
        """
        res = self._request("get", checksum)
        data = res.content
        if verify:
            if checksum != self._blockdigest(data):
                raise BlockStorageError("Checksum mismatch %s requested, %s get" % (checksum, self._blockdigest(data)))
        return data

    def get_verify(self, checksum):
        return self.get(checksum, True)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried
        """
        if checksum in self._checksums: # check local cache first
            return True
        if self._exists(checksum):
            self._checksums.add(checksum)
            return True
        return False

    def meta(self):
        """
        receive meta information of any checksum stored in bockstorage
        """
        res = self._get_chunked("meta")
        for chunk in res.iter_lines(decode_unicode=True):
            yield json.loads(chunk)


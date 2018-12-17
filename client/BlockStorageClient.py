#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import array
import logging
# non std
import requests
# own modules
from webstorageClient.ClientConfig import ClientConfig
from webstorageClient.WebStorageClient import WebStorageClient

class BlockStorageError(Exception):
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
        # search for cachefiles, and load local data
        self._cachefile = None
        for filename in os.listdir(self._client_config.homepath):
            if filename.startswith(self._info["id"]):
                cache_epoch = int(filename.split("_")[1].split(".")[0])
                self._logger.info("found checksum cache file until epoch %d", cache_epoch)
                if cache_epoch > self._info["blockchain_epoch"]: # something wrong
                    self._logger.error("cachefile %s is invalid", filename)
                    os.unlink(os.path.join(self._client_config.homepath, filename))
                elif cache_epoch != self._info["blockchain_epoch"]: # not the latest set
                    self._logger.info("local cache should be updated %d epochs", (self._info["blockchain_epoch"] - cache_epoch))
                else: # ok this could be used
                    self._cachefile = os.path.join(self._client_config.homepath, filename)
        if self._cachefile is None: # generate new cachefile
            self._cachefile = "%s_%s.bin" % (self._info["id"], self._info["blockchain_epoch"])
            self._cachefile = os.path.join(self._client_config.homepath, self._cachefile)
            self.get_checksums(2)
        # load stored data
        self._checksums = []
        self._logger.info("using cachefile %s", self._cachefile)
        data = array.array("B")
        with open(self._cachefile, "rb") as infile:
            data.fromfile(infile, os.stat(self._cachefile).st_size)
            for index in range(0, len(data), 20):
                checksum = "".join(["%02x" % item for item in data[index:index+20]])
                self._checksums.append(checksum)

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

    def get_checksums(self, epoch, filename=None):
        """
        write binary blob of checksums to cachefile
        """
        res = self._get_chunked("checksums/%d" % epoch)
        self._logger.info("writing %s", self._cachefile)
        if filename is None:
            filename = self._cachefile
        with open(filename, "wb") as outfile:
            for chunk in res:
                outfile.write(chunk)

    def put(self, data, use_cache=False):
        """put some arbitrary data into storage"""
        if len(data) > self.blocksize: # assure maximum length
            raise BlockStorageError("length of providede data (%s) is above maximum blocksize of %s" % (len(data), self.blocksize))
        checksum = self._blockdigest(data)
        if use_cache and checksum in self.checksums:
            self._logger.debug("202 - skip this block, checksum is in list of stored checksums")
            return checksum, 202
        else:
            res = self._request("put", checksum, data=data)
            if res.status_code == 201:
                self._logger.debug("201 - block rewritten")
            if res.text != checksum:
                raise BlockStorageError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
            self._checksums.append(checksum) # add to local cache
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
        return self._exists(checksum)

    def meta(self):
        """
        receive meta information of any checksum stored in bockstorage
        """
        res = self._get_chunked("meta")
        for chunk in res.iter_lines(decode_unicode=True):
            yield json.loads(chunk)


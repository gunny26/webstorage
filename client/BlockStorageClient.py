#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import logging
# own modules
from webstorageClient.ClientConfig import ClientConfig
from webstorageClient.WebStorageClient import WebStorageClient

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
            print("adding search parameters : %s", data)
        print("calling %s" % url)
        res = requests.get(url, params=data, headers=self.__headers, proxies=self.__proxies, stream=True)
        logging.debug("got Status code : %s", res.status_code)
        if res.status_code == 200:
            return res
        elif res.status_code == 404:
            raise KeyError("HTTP 404 received")
        else:
            raise Exception("got status %d for call to %s" % (res.status_code, url))

    def put(self, data, use_cache=False):
        """put some arbitrary data into storage"""
        assert len(data) <= self.blocksize # assure maximum length
        checksum = self._blockdigest(data)
        if use_cache and checksum in self.checksums:
            self._logger.debug("202 - skip this block, checksum is in list of stored checksums")
            return (checksum, 202)
        else:
            res = self._request("put", checksum, data=data)
            if res.status_code == 201:
                self._logger.debug("201 - block rewritten")
            if res.text != checksum:
                raise AssertionError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
            self._checksums.add(checksum) # add to local cache
            return res.text, res.status_code

    def get(self, checksum):
        """
        get data defined by hexdigest from storage
        """
        res = self._request("get", checksum)
        return res.content

    def get_verify(self, checksum):
        """
        get data defined by hexdigest from storage and verify received block to check integrity
        """
        res = self._request("get", checksum)
        data = res.content
        if checksum != self._blockdigest(data):
            raise AssertionError("Checksum mismatch %s requested, %s get" % (checksum, self._blockdigest(data)))
        return data

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


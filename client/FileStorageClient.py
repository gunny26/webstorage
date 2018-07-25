#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage WebApp
"""
import json
import logging
# own modules
from webstorage.ClientConfig import ClientConfig
from webstorage.BlockStorageClient import BlockStorageClient
from webstorage.WebStorageClient import WebStorageClient


class FileStorageClient(WebStorageClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    the recipe to reassemble will be stored in FileStorage
    """

    __version = "1.1"

    def __init__(self, url=None, apikey=None, cache=True):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client_config = ClientConfig()
        if not url:
            self._url = self._client_config.filestorage_url
        else:
            self._url = url
        if not apikey:
            self._apikey = self._client_config.filestorage_apikey
        else:
            self._apikey = apikey
        super().__init__()
        self._bs = BlockStorageClient(cache=cache)
        self._info = self._get_json("info") # TODO: use it
        self._cache = cache
        self._checksums = set()

    @property
    def blockstorage(self):
        return self._bs # TODO: is this necessary

    @property
    def checksums(self):
        if self._cache is True and not self._checksums:
            self._logger.info("getting existing checksums from backend")
            self._checksums = set(self._get_json())
        return self._checksums

    def put(self, fh, mime_type="application/octet-stream"):
        """
        save data of fileobject in Blockstorage

        data is read in blocks
        every block will be checksummed and tested if exists against
        BlockStorage
          if not existing, put it into BlockStorage
        the whole file is also checksummed and tested against FileStorage
          if not existing, put it into FileStorage
        """
        metadata = {
            "blockchain" : [],
            "size" : 0,
            "checksum" : None,
            "mime_type" : mime_type,
            "filehash_exists" : False, # indicate if the filehash already
            "blockhash_exists" : 0, # how many blocks existed already
        }
        filehash = self.hashfunc()
        # Put blocks in Blockstorage
        data = fh.read(self._bs.blocksize)
        while data:
            metadata["size"] += len(data)
            filehash.update(data) # running filehash until end
            checksum, status = self._bs.put(data, use_cache=True)
            self._logger.debug("PUT blockcount: %d, checksum: %s, status: %s", len(metadata["blockchain"]), checksum, status)
            # 202 - skipped, block in cache, 201 - rewritten, block existed
            if status in (201, 202):
                metadata["blockhash_exists"] += 1
            metadata["blockchain"].append(checksum)
            data = fh.read(self._bs.blocksize)
        self._logger.debug("put %d blocks in BlockStorage, %d existed already", len(metadata["blockchain"]), metadata["blockhash_exists"])
        # put file composition into filestorage
        filedigest = filehash.hexdigest()
        metadata["checksum"] = filedigest
        if self.exists(filedigest) is not True: # check if filehash is already stored
            self._logger.debug("storing recipe for filechecksum: %s", filedigest)
            res = self._request("put", filedigest, data=json.dumps(metadata))
            if res.status_code == 201: # could only be true at some rare race conditions
                self._logger.debug("recipe for checksum %s exists already", filedigest)
                metadata["filehash_exists"] = True
            return metadata
        self._logger.debug("filehash %s already stored", filedigest)
        metadata["filehash_exists"] = True
        return metadata

    def read(self, checksum):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almost all times less than self.blocksize
        """
        for block in self._get_json(checksum)["blockchain"]:
            yield self._bs.get(block)

    def delete(self, checksum):
        """
        delete blockchain defined by hexdigest
        the underlying data in BlockStorage will not be deleted
        """
        self._request("delete", checksum)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        return self._get_json(checksum)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried
        """
        if checksum in self._checksums:
            return True
        if self._exists(checksum):
            self._checksums.add(checksum)
            return True
        return False

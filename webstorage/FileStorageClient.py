#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage WebApp
"""
import os
import sys
import json
import hashlib
import logging
import requests
# own modules
from BlockStorageClient import BlockStorageClient as BlockStorageClient


CONFIG = {}
HOMEPATH = os.path.expanduser("~/.webstorage")
if not os.path.isdir(HOMEPATH):
    print("first create INI file in directory {}".format(HOMEPATH))
    sys.exit(1)
else:
    with open(os.path.join(HOMEPATH, "WebStorageClient.ini"), "rt") as infile:
        for line in infile:
            key, value = line.strip().split("=")
            CONFIG[key] = value


class FileStorageClient(object):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    the recipe to reassemble will be stored in FileStorage
    """

    __version = "1.1"

    def __init__(self, cache=True):
        """__init__"""
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__url = CONFIG["URL_FILESTORAGE"]
        self.__bs = BlockStorageClient(cache)
        self.__session = requests.Session()
        self.__headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self.__version),
            "x-auth-token" : CONFIG["APIKEY_FILESTORAGE"],
            "x-apikey" : CONFIG["APIKEY_FILESTORAGE"]
        }
        # get info from backend
        info = self.__get_json("info")
        if info["hashfunc"] != "sha1":
            raise Exception("only sha1 hashfunc implemented yet")
        self.__hashfunc = hashlib.sha1
        # build local checksum set
        self.__checksums = set()
        if cache is True:
            self.__checksums = set(self.__get_json())

    @property
    def blockstorage(self):
        return self.__bs

    @property
    def hashfunc(self):
        return self.__hashfunc

    @property
    def checksums(self):
        return self.__checksums

    def __request(self, method, path="", data=None):
        """
        single point of request
        """
        res = self.__session.request(method, "/".join((self.__url, path)), data=data, headers=self.__headers)
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

    def __blockdigest(self, data):
        """
        single point of digesting return hexdigest of data
        """
        digest = self.__hashfunc()
        digest.update(data)
        return digest.hexdigest()

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
        filehash = self.__hashfunc()
        # Put blocks in Blockstorage
        data = fh.read(self.__bs.blocksize)
        while data:
            metadata["size"] += len(data)
            filehash.update(data)
            #TODO: prevent double digesting
            blockdigest = self.__blockdigest(data)
            if not self.__bs.exists(blockdigest):
                checksum, status = self.__bs.put(data)
                assert checksum == blockdigest
                self.__logger.debug("PUT blockcount: %d, checksum: %s, status: %s", len(metadata["blockchain"]), checksum, status)
            else:
                metadata["blockhash_exists"] += 1
            metadata["blockchain"].append(blockdigest)
            data = fh.read(self.__bs.blocksize)
        self.__logger.debug("put %d blocks in BlockStorage, %d existed already", len(metadata["blockchain"]), metadata["blockhash_exists"])
        # File Checksum
        filedigest = filehash.hexdigest()
        metadata["checksum"] = filedigest
        if self.exists(filedigest) is not True: # check if filehash is already stored
            self.__logger.debug("storing recipe for filechecksum: %s", filedigest)
            res = self.__request("put", filedigest, data=json.dumps(metadata))
            if res.status_code == 201: # could only be true at some rare race conditions
                self.__logger.debug("recipe for checksum %s exists already", filedigest)
                metadata["filehash_exists"] = True
            return metadata
        self.__logger.debug("filehash %s already stored", filedigest)
        metadata["filehash_exists"] = True
        return metadata

    def read(self, checksum):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almoust all times less than self.blocksize
        """
        for block in self.__get_json(checksum)["blockchain"]:
            yield self.__bs.get(block)

    def delete(self, checksum):
        """
        delete blockchain defined by hexdigest
        the unerlying data in BlockStorage will not be deleted
        """
        self.__request("delete", checksum)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        return self.__get_json(checksum)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried
        """
        if checksum in self.__checksums:
            return True
        if self.__request("options", checksum).status_code == 200:
            self.__checksums.add(checksum)
            return True
        return False

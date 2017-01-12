#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
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
    for line in open(os.path.join(HOMEPATH, "WebStorageClient.ini"), "r"):
        key, value = line.strip().split("=")
        CONFIG[key] = value


class HTTPError(Exception):
    """indicates general exception"""
    pass


class HTTP404(Exception):
    """indicates not found"""
    pass



class FileStorageClient(object):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    the recipe to reassemble will be stored in FileStorage
    """

    def __init__(self, cache=False):
        """__init__"""
        self.__url = CONFIG["URL_FILESTORAGE"]
        self.__bs = BlockStorageClient(cache)
        self.__blocksize = self.__bs.blocksize
        self.__session = None
        self.__hashfunc = None
        self.__cache = cache
        self.__cache_checksums = set()
        self.__headers = {
            "x-auth-token" : CONFIG["APIKEY_FILESTORAGE"]
        }
        self.__info()

    @property
    def blockstorage(self):
        return self.__bs

    @property
    def hashfunc(self):
        return self.__hashfunc

    def __info(self):
        """get info from backend and create cache"""
        self.__session = requests.Session()
        # get info from backend
        res = self.__session.get(self.__get_url("info"), headers=self.__headers)
        # hack to be compatible with older requests versions
        try:
            data = res.json()
        except TypeError:
            data = res.json
        if data["hashfunc"] == "sha1":
            self.__hashfunc = hashlib.sha1
        else:
            raise Exception("only hashfunc sha1 implemented yet")
        # checksum cache
        if self.__cache is True:
            logging.info("Getting list of stored checksums from FileStorageBackend, this could take some time")
            self.__init_cache_checksums()

    def __get_url(self, arg=None):
        """return compound url"""
        if arg is None:
            return self.__url + "/"
        return "%s/%s" % (self.__url, arg)

    def put_fast(self, fh, mime_type="application/octet-stream"):
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
        # Block Checksums
        data = fh.read(self.__bs.blocksize)
        while data:
            metadata["size"] += len(data)
            filehash.update(data)
            blockhash = self.__bs.hashfunc()
            blockhash.update(data)
            if not self.__bs.exists(blockhash.hexdigest()):
                checksum, status = self.__bs.put(data)
                assert checksum == blockhash.hexdigest()
                logging.debug("PUT blockcount: %d, checksum: %s, status: %s", len(metadata["blockchain"]), checksum, status)
            else:
                metadata["blockhash_exists"] += 1
            metadata["blockchain"].append(blockhash.hexdigest())
            data = fh.read(self.__bs.blocksize)
        logging.debug("put %d blocks in BlockStorage, %d existed already", len(metadata["blockchain"]), metadata["blockhash_exists"])
        # File Checksum
        metadata["checksum"] = filehash.hexdigest()
        if self.exists(filehash.hexdigest()) is not True: # check if filehash is already stored
            logging.debug("storing recipe for filechecksum: %s", metadata["checksum"])
            res = self.__session.put(self.__get_url(metadata["checksum"]), data=json.dumps(metadata), headers=self.__headers)
            if res.status_code in (200, 201):
                if res.status_code == 201: # could only be true at some rare race conditions
                    logging.debug("recipe for checksum %s exists already", metadata["checksum"])
                    metadata["filehash_exists"] = True
                return metadata
            raise HTTPError("webapplication returned status %s" % res.status_code)
        else:
            logging.debug("filehash %s already stored", filehash.hexdigest())
            metadata["filehash_exists"] = True
            return metadata

    def read(self, checksum):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almoust all times less than self.blocksize
        """
        url = self.__get_url(checksum)
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            metadata = res.json()
            for block in metadata["blockchain"]:
                yield self.__bs.get(block)
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def delete(self, checksum):
        """
        delete blockchain defined by hexdigest
        the unerlying data in BlockStorage will not be deleted
        """
        url = self.__get_url(checksum)
        logging.debug("DELETE %s", url)
        res = self.__session.delete(url, headers=self.__headers)
        if res.status_code != 200:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        url = self.__get_url(checksum)
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def list(self):
        """
        return list of hexdigests stored in FileStorage
        """
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def exists_nocache(self, checksum):
        """
        check if file defined by checksum is already stored
        """
        url = self.__get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = self.__session.options(url, headers=self.__headers)
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        raise HTTP404("webapplication returned status %s" % res.status_code)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried
        """
        if checksum in self.__cache_checksums:
            return True
        else:
            if self.exists_nocache(checksum):
                self.__cache_checksums.add(checksum)
                return True
            else:
                return False

    def __init_cache_checksums(self):
        """create caches"""
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            # hack to work also on earlier versions of python 3
            try:
                self.__cache_checksums = set(res.json)
            except TypeError:
                self.__cache_checksums = set(res.json())
        else:
            logging.error("Failure to get stored checksum from FileStorage Backend, status %s", res.status_code)



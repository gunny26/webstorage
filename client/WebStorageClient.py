#!/usr/bin/python3
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import json
import hashlib
import logging
import requests

CONFIG = {}
for line in open("WebStorageClient.ini", "r"):
    key, value = line.strip().split("=")
    CONFIG[key] = value


class HTTPError(Exception):
    pass


class HTTP404(Exception):
    pass


class BlockStorageClient(object):
    """stores chunks of data into BlockStorage"""

    def __init__(self, cache=False):
        self.__url = CONFIG["URL_BLOCKSTORAGE"]
        self.__blocksize = None
        self.__hashfunc = None
        self.__session = None
        self.__cache = cache
        self.__cache_checksums = set()
        self.__info()

    def __info(self):
        # initialize
        self.__session = requests.Session()
        # get info from backend
        res = self.__session.get(self.__get_url("info"))
        data = res.json()
        self.__blocksize = int(data["blocksize"])
        if data["hashfunc"] == "sha1":
            self.__hashfunc = hashlib.sha1
        else:
            raise StandardError("only sha1 hashfunc implemented yet")
        # checksum cache
        if self.__cache is True:
            logging.info("Getting list of stored checksums from BlockStorageBackend, this could take some time")
            self.__init_cache_checksums()

    def __get_url(self, arg=None):
        if arg is None:
            return self.__url + "/"
        return "%s/%s" % (self.__url, arg)

    @property
    def blocksize(self):
        return self.__blocksize

    @property
    def hashfunc(self):
        return self.__hashfunc

    def put(self, data):
        """put some arbitrary data into storage"""
        assert len(data) <= self.blocksize
        digest = self.__hashfunc()
        digest.update(data)
        checksum = digest.hexdigest()
        url = self.__get_url(checksum)
        logging.debug("PUT %s", url)
        res = self.__session.put(self.__get_url(checksum), data=data)
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("block existed, but rewritten")
            assert res.text == checksum
            return res.text, res.status_code
        raise HTTPError("call to %s delivered status %s" % (self.__get_url(), res.status_code))

    def get(self, checksum):
        """get data defined by hexdigest from storage"""
        url = self.__get_url(checksum)
        logging.debug("GET %s", url)
        res = self.__session.get(self.__get_url(checksum))
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)
        return res.content

    def delete(self, checksum):
        """delete data defined by hexdigest from storage"""
        url = self.__get_url(checksum)
        logging.debug("DELETE %s", url)
        res = self.__session.delete(url)
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)

    def list(self):
        """return all availabel data defined by hexdigest as list of hexdigests"""
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url)
        if res.status_code == 200:
            return res.json()
        raise HTTP404("webapplication delivered status %s" % res.status_code)

    def exists_nocache(self, checksum):
        """check if data defined by hexdigest exists"""
        url = self.__get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = self.__session.options(url)
        if res.status_code == 200:
            return True
        return False

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
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url)
        if res.status_code == 200:
            # hack to work also on earlier versions of python 3
            try:
                self.__cache_checksums = set(res.json)
            except TypeError:
                self.__cache_checksums = set(res.json())
        else:
            logging.error("Failure to get stored checksum from FileStorage Backend, status %s", res.status_code)


class FileStorageClient(object):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    the recipe to reassemble will be stored in FileStorage
    """

    def __init__(self, cache=False):
        self.__url = CONFIG["URL_FILESTORAGE"]
        self.__bs = BlockStorageClient(cache)
        self.__blocksize = self.__bs.blocksize
        self.__session = None
        self.__hashfunc = None
        self.__cache = cache
        self.__cache_checksums = set()
        self.__info()

    def __info(self):
        self.__session = requests.Session()
        # get info from backend
        res = self.__session.get(self.__get_url("info"))
        data = res.json()
        if data["hashfunc"] == "sha1":
            self.__hashfunc = hashlib.sha1
        else:
            raise StandardError("only hashfunc sha1 implemented yet")
        # checksum cache
        if self.__cache is True:
            logging.info("Getting list of stored checksums from FileStorageBackend, this could take some time")
            self.__init_cache_checksums()

    def __get_url(self, arg=None):
        if arg is None:
            return self.__url + "/"
        return "%s/%s" % (self.__url, arg)

#    def put(self, fh, mime_type="text/html"):
#        """
#        save data of fileobject in Blockstorage
#        data is chunked in self.blocksize pieces and sent to BlockStorage
#
#        the data is anyway transfered to BlockStorage, no matter if
#        this data is already stored
#        """
#        def read_block():
#            return fh.read(self.__bs.blocksize)
#        metadata = {
#            "blockchain" : [],
#            "size" : 0,
#            "checksum" : None,
#            "mime_type" : mime_type
#        }
#        filehash = self.__hashfunc()
#        for data in iter(read_block, ""):
#            filehash.update(data)
#            metadata["size"] += len(data)
#            checksum, status = self.__bs.put(data)
#            metadata["blockchain"].append(checksum)
#        metadata["checksum"] = filehash.hexdigest()
#        res = self.__session.put(self.__get_url(metadata["checksum"]), data=json.dumps(metadata))
#        if res.status_code in (200, 201):
#            if res.status_code == 201:
#                logging.info("file for this checksum already existed")
#            return metadata
#        raise HTTP404("webapplication returned status %s" % res.status_code)

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
            res = self.__session.put(self.__get_url(metadata["checksum"]), data=json.dumps(metadata))
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
        res = self.__session.get(url)
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
        res = self.__session.delete(url)
        if res.status_code != 200:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        url = self.__get_url(checksum)
        logging.debug("GET %s", url)
        res = self.__session.get(url)
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
        res = self.__session.get(url)
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
        res = self.__session.options(url)
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
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url)
        if res.status_code == 200:
            # hack to work also on earlier versions of python 3
            try:
                self.__cache_checksums = set(res.json)
            except TypeError:
                self.__cache_checksums = set(res.json())
        else:
            logging.error("Failure to get stored checksum from FileStorage Backend, status %s", res.status_code)

#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import os
import sys
import hashlib
import logging
import requests


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


class BlockStorageClient(object):
    """stores chunks of data into BlockStorage"""

    def __init__(self, cache=False):
        """__init__"""
        self.__url = CONFIG["URL_BLOCKSTORAGE"]
        self.__blocksize = None
        self.__hashfunc = None
        self.__session = None
        self.__cache = cache
        self.__cache_checksums = set()
        self.__headers = {
            "x-auth-token" : CONFIG["APIKEY_BLOCKSTORAGE"]
        }
        self.__info()

    def __info(self):
        """get info from backend, and initialize caches"""
        # initialize
        self.__session = requests.Session()
        # get info from backend
        res = self.__session.get(self.__get_url("info"), headers=self.__headers)
        # hack to be compatible with older requests versions
        try:
            data = res.json()
        except TypeError:
            data = res.json
        self.__blocksize = int(data["blocksize"])
        if data["hashfunc"] == "sha1":
            self.__hashfunc = hashlib.sha1
        else:
            raise Exception("only sha1 hashfunc implemented yet")
        # checksum cache
        if self.__cache is True:
            logging.info("Getting list of stored checksums from BlockStorageBackend, this could take some time")
            self.__init_cache_checksums()

    def __get_url(self, arg=None):
        """return compound url"""
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
        res = self.__session.put(self.__get_url(checksum), data=data, headers=self.__headers)
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
        res = self.__session.get(self.__get_url(checksum), headers=self.__headers)
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)
        return res.content

    def delete(self, checksum):
        """delete data defined by hexdigest from storage"""
        url = self.__get_url(checksum)
        logging.debug("DELETE %s", url)
        res = self.__session.delete(url, headers=self.__headers)
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)

    def list(self):
        """return all availabel data defined by hexdigest as list of hexdigests"""
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            return res.json()
        raise HTTP404("webapplication delivered status %s" % res.status_code)

    def exists_nocache(self, checksum):
        """check if data defined by hexdigest exists"""
        url = self.__get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = self.__session.options(url, headers=self.__headers)
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
        """initialize cache"""
        url = self.__get_url()
        logging.debug("GET %s", url)
        res = self.__session.get(url, headers=self.__headers)
        if res.status_code == 200:
            # hack to work also on earlier versions of python 3
            try:
                self.__cache_checksums = set(res.json())
            except TypeError:
                self.__cache_checksums = set(res.json)
        else:
            logging.error("Failure to get stored checksum from FileStorage Backend, status %s", res.status_code)


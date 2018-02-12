#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import sys
import hashlib
import logging
import requests

CONFIG = {}
HOMEPATH = os.path.expanduser("~/.webstorage")
if not os.path.isdir(HOMEPATH):
    print("please create directory {}".format(HOMEPATH))
    sys.exit(1)
else:
    with open(os.path.join(HOMEPATH, "WebStorageClient.ini"), "rt") as infile:
        for line in infile:
            key, value = line.strip().split("=")
            CONFIG[key] = value


class BlockStorageClient(object):
    """stores chunks of data into BlockStorage"""

    __version = "1.1"

    def __init__(self, cache=True):
        """__init__"""
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__url = CONFIG["URL_BLOCKSTORAGE"]
        self.__session = requests.Session()
        self.__headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self.__version),
            "x-auth-token" : CONFIG["APIKEY_BLOCKSTORAGE"],
            "x-apikey" : CONFIG["APIKEY_BLOCKSTORAGE"]
        }
        # get info from backend
        info = self.__get_json("info")
        self.__blocksize = int(info["blocksize"])
        if info["hashfunc"] != "sha1":
            raise Exception("only sha1 hashfunc implemented yet")
        self.__hashfunc = hashlib.sha1
        # build local checksum set
        self.__checksums = set()
        if cache is True:
            self.__checksums = set(self.__get_json())

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

    @property
    def blocksize(self):
        return self.__blocksize

    @property
    def hashfunc(self):
        return self.__hashfunc

    @property
    def checksums(self):
        return self.__checksums

    def put(self, data):
        """put some arbitrary data into storage"""
        assert len(data) <= self.blocksize
        checksum = self.__blockdigest(data)
        res = self.__request("put", checksum, data=data)
        if res.status_code == 201:
            self.__logger.info("block rewritten")
        if res.text != checksum:
            raise AssertionError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
        self.__checksums.add(checksum) # add to local cache
        return res.text, res.status_code

    def get(self, checksum, verify=False):
        """get data defined by hexdigest from storage"""
        res = self.__request("get", checksum)
        data = res.content
        if verify is True and checksum != self.__blockdigest(data):
            raise AssertionError("Checksum mismatch %s requested, %s get" % (checksum, self.__blockdigest(data)))
        return data

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the backend is queried
        """
        if checksum in self.__checksums:
            return True
        # check also Backend to be sure
        try:
            if self.__request("options", checksum).status_code == 200:
                return True
        except KeyError:
            pass
        return False

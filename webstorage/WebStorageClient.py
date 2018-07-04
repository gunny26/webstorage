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
# own modules
from webstorage.Config import get_config


class WebStorageClient(object):
    """basic super clas for WebStorage Client Classes"""

    _version = "2.0"

    def __init__(self):
        """__init__"""
        self._session = requests.Session()
        self._headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self._version),
            "x-auth-token" : self._config["APIKEY_BLOCKSTORAGE"],
            "x-apikey" : self._config["APIKEY_BLOCKSTORAGE"]
        }
        self.hashfunc = hashlib.sha1

    def _request(self, method, path="", data=None):
        """
        single point of request
        """
        # if HTTPS_PROXY is set in config file use this information
        proxies = {}
        if "HTTPS_PROXY" in self._config:
            proxies = {"https": self._config["HTTPS_PROXY"]}
        url = "/".join((self._url, path))
        res = self._session.request(method, url, data=data, headers=self._headers, proxies=proxies)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)

    def _get_json(self, path=""):
        """
        single point of json requests
        """
        res = self._request("get", path)
        # hack to be compatible with older requests versions
        try:
            return res.json()
        except TypeError:
            return res.json

    def _blockdigest(self, data):
        """
        single point of digesting return hexdigest of data
        """
        digest = self.hashfunc()
        digest.update(data)
        return digest.hexdigest()

    def _exists(self, path):
        """
        OPTIONS call to path, if status_code == 200 return True
        otherwise False
        """
        try:
            if self._request("options", path).status_code == 200:
                return True
        except KeyError: # 404 if not found
            return False

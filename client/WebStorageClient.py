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
from webstorageClient.ClientConfig import ClientConfig


class WebStorageClient(object):
    """basic super class for WebStorage Client Classes"""

    _version = "2.0"

    def __init__(self):
        """__init__"""
        self._session = requests.Session()
        self._headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self._version),
            "x-auth-token" : self._apikey,
            "x-apikey" : self._apikey,
            "connection" : "keep-alive",
        }
        self._proxies = {}
        if self._client_config.https_proxy:
            self._proxies["https"] = self._client_config.https_proxy
        if self._client_config.http_proxy:
            self._proxies["http"] = self._client_config.http_proxy
        self.hashfunc = hashlib.sha1

    def _request(self, method, path="", data=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        with self._session.request(method, url, data=data, headers=self._headers, proxies=self._proxies, timeout=180) as res:
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

    def _get_chunked(self, method, data=None):
        """
        call url and received chunked content to yield
        """
        url = "/".join((self._url, method))
        if data is not None:
            self._logger.info("adding search parameters : %s", data)
        self._logger.info("calling %s", url)
        res = requests.get(url, params=data, headers=self._headers, proxies=self._proxies, stream=True)
        self._logger.info("received %s", res.status_code)
        if res.status_code == 200:
            return res
        elif res.status_code == 404:
            raise KeyError("HTTP 404 received")
        else:
            raise Exception("got status %d for call to %s" % (res.status_code, url))

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

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
        if self._client_config is None:
            raise AttributeError("_client_config must be set")
        if self._url is None:
            raise AttributeError("_url must be set")
        if self._client_config.requests_verify is False:
            logging.info("TLS Certificate verification will be disabled")
            import urllib3
            urllib3.disable_warnings()
        self._session = requests.Session()
        self._session.verify = self._client_config.requests_verify
        self._session.proxies = self._client_config.proxies
        self._headers = {
            "user-agent": "%s-%s" % (self.__class__.__name__, self._version),
            "x-apikey" : self._client_config.apikey,
            "connection" : "keep-alive",
        }
        self._session.headers.update(self._headers)
        self._session.timeout = 180
        self.hashfunc = hashlib.sha1

    def _delete(self, path):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        res = self._session.delete(url)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)

    def _get(self, path, params=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        res = self._session.get(url, params=params)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)

    def _put(self, path, data=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        res = self._session.get(url, data=data)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)

    def _post(self, path, data=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        res = self._session.post(url, data=data)
        if 199 < res.status_code < 300:
            return res
        elif 399 < res.status_code < 500:
            raise KeyError("HTTP_STATUS %s received" % res.status_code)
        elif 499 < res.status_code < 600:
            raise IOError("HTTP_STATUS %s received" % res.status_code)


    def _get_json(self, path, params=None):
        """
        single point of json requests
        """
        res = self._get(path, params=params)
        # hack to be compatible with older requests versions
        try:
            return res.json()
        except TypeError:
            return res.json

    def _get_chunked(self, path, params=None):
        """
        call url and received chunked content to yield
        """
        url = "/".join((self._url, path))
        if data is not None:
            self._logger.info("adding search parameters : %s", data)
        self._logger.info("calling %s", url)
        # this does not work with self._session, i dont know why
        res = self._session.get(url, params=data, stream=True)
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
            if self._session.request("options", path).status_code == 200:
                return True
        except KeyError: # 404 if not found
            return False

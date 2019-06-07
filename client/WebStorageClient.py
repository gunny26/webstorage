#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import sys
import hashlib
import logging
import json
# non std
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

    def _call(self, *args, **kwds):
        """
        most basic method to make a http call
        """
        method = args[0] # first is http method
        r_args = args[1:] # then path or other things
        # do the renew 30 seconds before exp
        #if self._id_token and self._id_token["exp"] <= (int(time.time()) + 30):
        #    self.logger.info("token will expire <= 30 seconds")
        #    if self._discovery:
        #        self.logger.info("getting new token")
        #        self.__enter__()
        if method.upper() == "GET":
            res = self._session.get(*r_args, **kwds)
        elif method.upper() == "PUT":
            res = self._session.put(*r_args, **kwds)
        elif method.upper() == "POST":
            res = self._session.post(*r_args, **kwds)
        elif method.upper() == "DELETE":
            res = self._session.delete(*r_args, **kwds)
        elif method.upper() == "OPTIONS":
            res = self._session.options(*r_args, **kwds)
        else:
            raise NotImplementedError("HTTP Method %s is not implemented" % method)
        if res.status_code < 500: # everything below 500 is acceptable
            if res.status_code == 200:
                return res
            if res.status_code == 401:
                raise IOError("unauthorized to access %s" % r_args[0])
            if res.status_code in (301, 302): # redirects
                self._logger.debug(json.dumps(dict(res.headers), indent=4))
                self._logger.debug(res.text)
                return res
            if res.status_code == 404:
                raise KeyError(res.reason)
        # you schould not get there
        self._logger.debug(json.dumps(dict(res.headers), indent=4))
        self._logger.info("Status Code: %s", res.status_code)
        self._logger.error(res.text)
        raise IOError(res.reason)

    def _delete(self, path):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        return self._call("DELETE", url)

    def _get(self, path, params=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        return self._call("GET", url, params=params)

    def _put(self, path, data=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        return self._call("PUT", url, data=data)

    def _post(self, path, data=None):
        """
        single point of request
        """
        url = "/".join((self._url, path))
        return self._call("POST", url, data=data)

    def _get_json(self, path, params=None):
        """
        single point of json requests
        """
        url = "/".join((self._url, path))
        return self._call("GET", url, params=params).json()

    def _get_chunked(self, path, params=None):
        """
        call url and received chunked content to yield
        """
        url = "/".join((self._url, path))
        return self._call("GET", url, params=params, stream=True)

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
        url = "/".join((self._url, path))
        try:
            if self._call("OPTIONS", url).status_code == 200:
                return True
        except KeyError: # 404 if not found
            return False

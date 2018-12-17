#!/usr/bin/python3
# pylint: disable=line-too-long
"""
Module to read config
"""
import os
import sys
import json
import logging

class ClientConfig(object):

    def __init__(self):
        """
        read config file if exists and return dict
        """
        self.client_config = None
        if os.name == "nt":
            self._homepath = os.path.join(os.path.expanduser("~"), "AppData", "Local", "webstorage")
        else:
            self._homepath = os.path.join(os.path.expanduser("~"), ".webstorage")
        logging.debug("using config directory %s", self._homepath)
        if not os.path.isdir(self._homepath):
            print("please create directory {}".format(self._homepath))
            sys.exit(1)
        else:
            # first best use newer json format file
            configfile = os.path.join(self._homepath, "WebStorageClient.json")
            if os.path.isfile(configfile):
                with open(configfile, "rt") as infile:
                    self.client_config = json.load(infile)
                    # print(json.dumps(self.client_config, indent=4))
            else:
                print("please create configuration file %s" % configfile)
                sys.exit(2)

    @property
    def homepath(self):
        return self._homepath

    @property
    def blockstorages(self):
        return self.client_config["blockstorages"]

    @property
    def blockstorage(self):
        return [blockstorage for blockstorage in self.client_config["blockstorages"] if blockstorage["default"] == True][0]

    @property
    def blockstorage_url(self):
        return self.blockstorage["url"]

    @property
    def blockstorage_apikey(self):
        return self.blockstorage["apikey"]

    @property
    def filestorage(self):
        return [filestorage for filestorage in self.client_config["filestorages"] if filestorage["default"] == True][0]

    @property
    def filestorage_url(self):
        return self.filestorage["url"]

    @property
    def filestorage_apikey(self):
        return self.filestorage["apikey"]

    @property
    def archive(self):
        return [archive for archive in self.client_config["archives"] if archive["default"] == True][0]

    @property
    def archive_url(self):
        return self.archive["url"]

    @property
    def archive_apikey(self):
        return self.archive["apikey"]

    @property
    def https_proxy(self):
        try:
            return self.client_config["proxies"]["https"]
        except KeyError:
            return

    @property
    def http_proxy(self):
        try:
            return self.client_config["proxies"]["http"]
        except KeyError:
            return

    def __str__(self):
        return json.dumps(self.client_config, indent=4)


if __name__ == "__main__":
    cc = ClientConfig()
    print(cc)
    print(cc.blockstorage_url)
    print(cc.blockstorage_apikey)
    print(cc.filestorage_url)
    print(cc.filestorage_apikey)
    print(cc.archive_url)
    print(cc.archive_apikey)


#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import hashlib
import json

urls = ("/(.*)", "FileIndex",
        )

# add wsgi functionality
CONFIG = {}
for line in open("fileindex.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = CONFIG["STORAGE_DIR"]


class FileIndex(object):
    """stores filename to checksum index in normal filesystem"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, subpath):
        return os.path.join(STORAGE_DIR, "/".join(args))

    def GET(self, args):
        """
	    get block stored in blockstorage directory with hash
        """
        filename = self.__get_filename(args)
        if os.path.isfile(filename):
            web.header('Content-Type', 'application/octet-stream')
            return json.dumps(open(filename, "rb").read())
        else:
            logging.error("File %s does not exist", filename)
            web.notfound()

    def EXISTS(self, md5):
        """
        check if block with digest exists
        """
        if os.path.isfile(self.__get_filename(md5)):
            web.ctx.status = '200 Exists'
        else:
            web.notfound()

    def POST(self, args):
        """
        put data into storage
        """
        #logging.debug("POST called")
        filename = self.__get_filename(args)
        checksum = json.loads(web.data())
        if checksum is not None:
            if not os.path.isfile(filename):
                try:
                    json.dump(checksum, open(filename, "wb"))
                    web.ctx.status = '201 metadata stored'
                except TypeError:
                    os.unlink(filename)
        else:
            return "Hello World"
    PUT = POST

    def DELETE(self, args):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage
        """
        filename = self.__get_filename(args)
        if os.path.isfile(filename):
            os.unlink(filename)
            web.ctx.status = '201 metadata deleted'
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

#!/usr/bin/python

import web
import os
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
import hashlib
import json

urls = ("/(.*)", "FileIndex",
        )

# add wsgi functionality
CONFIG = {}
for line in open("/var/www/webstorage/webapps/fileindex/fileindex.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = CONFIG["STORAGE_DIR"]


class FileIndex(object):
    """stores filename to checksum index in normal filesystem"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, args):
        return os.path.join(STORAGE_DIR, args)

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

    def EXISTS(self, args):
        """
        check if block with digest exists
        """
	logging.info("EXISTS called with %s", args)
        filename = self.__get_filename(args)
        if os.path.isfile(filename):
            logging.info("found file %s", filename)
            web.ctx.status = '200 file exists'
        elif os.path.isdir(filename):
            logging.info("found directory %s", filename)
            web.ctx.status = '201 directory exists'
        else:
            web.notfound()

    def POST(self, args):
        """
        put data into storage
        """
        #logging.debug("POST called")
        filename = self.__get_filename(args)
        if len(web.data()) != 0:
            checksum = json.loads(web.data())
            if not os.path.isfile(filename):
                try:
                    json.dump(checksum, open(filename, "wb"))
                    web.ctx.status = '201 metadata stored'
                except TypeError:
                    os.unlink(filename)
            else:
                 try:
                    json.dump(checksum, open(filename, "wb"))
                    web.ctx.status = '202 metadata overwritten'
                    logging.info("file %s overwritten", filename)
                 except TypeError:
                    pass
        else:
            logging.info("creating directory %s", filename)
            os.mkdir(filename)
            web.ctx.status = '203 directory created'
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

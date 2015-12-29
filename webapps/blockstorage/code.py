#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import hashlib
import json

urls = ("/(.*)", "BlockStorage",
        )

# add wsgi functionality
CONFIG = {}
for line in open("filestorage.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = CONFIG["STORAGE_DIR"]


class BlockStorage(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, checksum):
        return os.path.join(STORAGE_DIR, "%s.bin" % checksum)

    def GET(self, md5):
        """
	    get block stored in blockstorage directory with hash
        """
        logging.debug("GET called, md5=%s", md5)
        if os.path.isfile(self.__get_filename(md5)):
            web.header('Content-Type', 'application/octet-stream')
            return open(self.__get_filename(md5), "rb").read()
        else:
            logging.error("File %s does not exist", self.__get_filename(md5))
            web.notfound()

    def EXISTS(self, md5):
        """
        check if block with digest exists
        """
        logging.debug("EXISTS called, md5=%s", md5)
        if os.path.exists(self.__get_filename(md5)):
            web.ctx.status = '200 Exists'
        else:
            web.notfound()

    def POST(self, args):
        """
        put data into storage
        """
        #logging.debug("POST called")
        data = web.data()
        if len(data) > 0:
            digest = hashlib.md5()
            digest.update(data)
            md5 = digest.hexdigest()
            if not os.path.isfile(self.__get_filename(md5)):
                fo = open(self.__get_filename(md5), "wb")
                fo.write(data)
                fo.close()
                web.ctx.status = '201 block stored'
                logging.info("block %s stored", self.__get_filename(md5))
                return json.dumps(digest.hexdigest())
            else:
                web.ctx.status = '202 block existed'
                return json.dumps(digest.hexdigest())
        else:
            return "Hello World"
    PUT = POST

    def DELETE(self, md5):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage
        """
        logging.debug("DELETE called, md5=%s", md5)
        if os.path.exists(self.__get_filename(md5)):
            os.unlink(self.__get_filename(md5))
            web.ctx.status = '201 block deleted'
        else:
            web.ctx.status = '202 block not found or already deleted'


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

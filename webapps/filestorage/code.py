#!/usr/bin/python

import web
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import hashlib
import json

urls = ("/(.*)", "FileStorage",
        )

# add wsgi functionality
CONFIG = {}
for line in open("/var/www/webstorage/webapps/filestorage/filestorage.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = CONFIG["STORAGE_DIR"]
#storage_dir = "/media/webstorage/filestorage"


class FileStorage(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, checksum):
        return os.path.join(STORAGE_DIR, "%s.json" % checksum)

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
        if os.path.isfile(self.__get_filename(md5)):
            web.ctx.status = '200 Exists'
        else:
            web.notfound()

    def POST(self, md5):
        """
        put data into storage
        """
        #logging.debug("POST called")
        metadata = json.loads(web.data())
        assert metadata["checksum"] == md5
        if metadata is not None:
            if not os.path.isfile(self.__get_filename(md5)):
                try:
                    json.dump(metadata, open(self.__get_filename(md5), "wb"))
                    web.ctx.status = '201 metadata stored'
                except TypeError:
                    os.unlink(self.__get_filename(md5))
            else:
                existing = json.load(open(self.__get_filename(md5), "rb"))
                try:
                    assert existing == metadata
                    logging.error("Metadata for %s already stored", md5)
                    web.ctx.status = '202 metadata existed'
                except AssertionError as exc:
                    logging.exception(exc)
                    logging.error("existing: %s", existing)
                    logging.error("new: %s", metadata)
        else:
            return "Hello World"
    PUT = POST

    def DELETE(self, md5):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage
        """
        logging.debug("DELETE called, md5=%s", md5)
        if os.path.isfile(self.__get_filename(md5)):
            os.unlink(self.__get_filename(md5))
            web.ctx.status = '201 metadata deleted'
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

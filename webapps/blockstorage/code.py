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
for line in open("/var/www/webstorage/webapps/blockstorage/blockstorage.ini", "rb"):
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

    def GET(self, args):
        params = args.split("/")
        method = None
        method_args = None
        if len(params) == 1:
            method = params[0]
            method_args = ()
        else:
            method = params[0]
            method_args = params[1:]
        logging.info("calling method %s", method)
        data = web.data()
        if method == "get":
            return self.get(method_args, data)
        elif method == "exists":
            return self.exists(method_args, data)
        elif method == "stats":
            return self.stats(method_args, data)
        elif method == "put":
            return self.put(method_args, data)
        elif method == "ls":
            return self.ls(method_args, data)
        elif method == "delete":
            return self.delete(method_args, data)
        else:
            return "unknown method %s called" % method

    def get(self, args, data):
        """
	get block stored in blockstorage directory with hash
        """
        md5 = args[0]
        if os.path.isfile(self.__get_filename(md5)):
            web.header('Content-Type', 'application/octet-stream')
            return open(self.__get_filename(md5), "rb").read()
        else:
            logging.error("File %s does not exist", self.__get_filename(md5))
            web.notfound()

    def ls(self, args, data):
        """
        returns list of existing block checksums
        """
        return json.dumps([filename[:-4] for filename in os.listdir(STORAGE_DIR)])

    def exists(self, args, data):
        """
        check if block with digest exists
        """
        md5 = args[0]
        if os.path.exists(self.__get_filename(md5)):
            web.ctx.status = '200 exists'
        else:
            web.ctx.status = '201 does not exists'

    def put(self, args, data):
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
                return json.dumps(digest.hexdigest())
            else:
                web.ctx.status = '202 block exists'
                logging.info("block %s already exists", self.__get_filename(md5))
                return json.dumps(digest.hexdigest())
        else:
            return "Hello World"

    def delete(self, args, data):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage
        """
        md5 = args[0]
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

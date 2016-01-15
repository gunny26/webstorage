#!/usr/bin/python

import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import hashlib
import json

urls = ("/(.*)", "BlockStorage",
        )

# add wsgi functionality
CONFIG = {}
for line in open("/var/www/webstorage/webapps/blockstorage.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = CONFIG["STORAGE_DIR"]


def calllogger(func):
     """
     decorator
     """
     def inner(*args, **kwds):
         starttime = time.time()
         call_str = "%s(%s, %s)" % (func.__name__, args[1:], kwds)
         logging.debug("calling %s", call_str)
         try:
             ret_val = func(*args, **kwds)
             logging.debug("duration of call %s : %s", call_str, (time.time() - starttime))
             return ret_val
         except StandardError as exc:
             logging.exception(exc)
             logging.error("call to %s caused StandardError", call_str)
             web.internalerror()
     # set inner function __name__ and __doc__ to original ones
     inner.__name__ = func.__name__
     inner.__doc__ = func.__doc__
     return inner


class BlockStorage(object):
    """
    Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier
    """

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, checksum):
        """
        build and return absolute filpath

        params:
        checksum <basestring>

        ret:
        <basestring>
        """
        return os.path.join(STORAGE_DIR, "%s.bin" % checksum)

    @calllogger
    def GET(self, args):
        """
        GET multiplexer, uses first part of URL path to get the desired method,
        than calles given method with remaining URL Paths

        params:
        args <list> called by web.py
        """
        params = args.split("/")
        method = None
        method_args = None
        if len(params) == 1:
            method = params[0]
            method_args = ()
        else:
            method = params[0]
            method_args = params[1:]
        logging.debug("calling method %s", method)
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
        logging.info("call for non-exsiting method detected")
        web.notfound()

    def get(self, args, data):
        """
	get block stored in blockstorage directory with hash
        
        md5 checksum is the remaining part of URI

        GOOD : HTTP 200
        BAD  : HTTP 404 
        UGLY : catched by decorator
        """
        md5 = args[0]
        if os.path.isfile(self.__get_filename(md5)):
            # set to octet stream, binary data
            web.header('Content-Type', 'application/octet-stream')
            return open(self.__get_filename(md5), "rb").read()
        else:
            logging.error("File %s does not exist", self.__get_filename(md5))
            web.notfound()

    def ls(self, args, data):
        """
        returns list of existing block checksums

        GOOD : HTTP 200 and json encoded list checksums
        BAD  : this should not be possible
        UGLY : catched by decorator
        """
        return json.dumps([filename[:-4] for filename in os.listdir(STORAGE_DIR)])

    def exists(self, args, data):
        """
        check if block with digest exists

        GOOD : HTTP 200
        BAD  : HTTP 404 for not found, not existing
        UGLY : catched by decorator
        """
        md5 = args[0]
        if not os.path.exists(self.__get_filename(md5)):
            web.notfound()

    def put(self, args, data):
        """
        put data into storage
 
        GOOD : HTTP 200 - stored, HTTP 201 - existed already but stored
        BAD  : HTTP 500 - no data
        UGLY : catched by decorator
        """
        data = web.data()
        if len(data) > 0:
            digest = hashlib.md5()
            digest.update(data)
            md5 = digest.hexdigest()
            if not os.path.isfile(self.__get_filename(md5)):
                fo = open(self.__get_filename(md5), "wb")
                fo.write(data)
                fo.close()
            else:
                web.ctx.status = '201 block rewritten'
                logging.info("block %s already exists", self.__get_filename(md5))
            return json.dumps(digest.hexdigest())
        else:
            web.ctx.status = '501 no data to store'

    def delete(self, args, data):
        """
        delete block with md5checksum given
        the block should only be deleted if not used anymore in any FileStorage

        GOOD : HTTP 200 - block deleted
        BAD  : HTTP 404 - not found if block does not exist
        UGLY : catched by decorator
        """
        md5 = args[0]
        if os.path.exists(self.__get_filename(md5)):
            os.unlink(self.__get_filename(md5))
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

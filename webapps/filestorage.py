#!/usr/bin/python

import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
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


class FileStorage(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, checksum):
        return os.path.join(STORAGE_DIR, "%s.json" % checksum)

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
        elif method == "put":
            return self.put(method_args, data)
        elif method == "ls":
            return self.ls(method_args, data)
        elif method == "delete":
            return self.delete(method_args, data)
        logging.info("detected unknown method call %s", method)
        web.notfound()

    def get(self, args, data):
        """
	get block stored in blockstorage directory with hash

        GOOD : 200 : get metadata stored in file, json formatted
        BAD  : 404 : not found
        UGLY : decorator
        """
        md5 = args[0]
        if os.path.isfile(self.__get_filename(md5)):
            web.header('Content-Type', 'application/json')
            return open(self.__get_filename(md5), "rb").read()
        else:
            logging.error("File %s does not exist", self.__get_filename(md5))
            web.notfound()

    def ls(self, args, data):
        """
	get block stored in blockstorage directory with hash

        GOOD : 200 returns json encoded directory listing
        BAD  : this should not be possible
        UGLY : decortator
        """
        web.header('Content-Type', 'application/json')
        return json.dumps([filename[:-5] for filename in os.listdir(STORAGE_DIR)])

    def exists(self, args, data):
        """
        check if block with digest exists

        GOOD : 200 if file exists
        BAD  : 404 not found
        UGLY : decorator
        """
        md5 = args[0]
        if not os.path.isfile(self.__get_filename(md5)):
            web.notfound()

    def put(self, args, data):
        """
        put data into storag

        GOOD : 200 storing metadata in file
               201 if file already existed
        BAD  : 404 if file not found
        UGLY : decorator or if no data is given
        """
        md5 = args[0]
        metadata = json.loads(web.data())
	try:
            assert metadata["checksum"] == md5
	except AssertionError as exc:
            logging.error("metadata: %s, md5: %s", metadata, md5)
            raise exc
	except TypeError as exc:
            logging.error("metadata: %s, md5: %s", metadata, md5)
            raise exc
        if metadata is not None:
            if not os.path.isfile(self.__get_filename(md5)):
                try:
                    json.dump(metadata, open(self.__get_filename(md5), "wb"))
                except TypeError:
                    os.unlink(self.__get_filename(md5))
            else:
                existing = json.load(open(self.__get_filename(md5), "rb"))
                try:
                    assert existing == metadata
                    logging.error("Metadata for %s already stored", md5)
                    web.ctx.status = '201 metadata existed'
                except AssertionError as exc:
                    logging.exception(exc)
                    logging.error("existing: %s", existing)
                    logging.error("new: %s", metadata)
        else:
            web.notfound()

    def delete(self, args, data):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage

        GOOD : 200 file deletes
        BAD  : 404 file not found
        UGLY : decorator
        """
        md5 = args[0]
        logging.debug("DELETE called, md5=%s", md5)
        if os.path.isfile(self.__get_filename(md5)):
            os.unlink(self.__get_filename(md5))
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

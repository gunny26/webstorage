#!/usr/bin/python

import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import hashlib
import json
import base64

urls = ("/(.*)", "FileIndex",
        )

# add wsgi functionality
CONFIG = {}
for line in open("/var/www/webstorage/webapps/fileindex/fileindex.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value
STORAGE_DIR = unicode(CONFIG["STORAGE_DIR"])


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
             return "call to %s caused StandardError" % call_str
     # set inner function __name__ and __doc__ to original ones
     inner.__name__ = func.__name__
     inner.__doc__ = func.__doc__
     return inner


class FileIndex(object):
    """stores filename to checksum index in normal filesystem"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __b64_get_filename(self, args):
        logging.info("%s <%s>", args[0], type(args[0]))
        decoded = base64.b64decode(str(args[0])).decode("utf-8")
        subname = decoded
        if decoded.startswith(u"/"):
            subname = decoded[1:]
        logging.debug("joining %s and %s", STORAGE_DIR, subname) 
        filename = os.path.join(STORAGE_DIR, subname)
        return filename

    def __get_filename(self, basename):
        logging.error(":".join(["%x" % ord(char) for char in basename]))
        logging.error(basename)
        if basename.startswith(u"/"):
            basename = basename[1:]
        filename = os.path.join(STORAGE_DIR, basename)
        return filename

    @staticmethod
    def __ret_json(text):
        #web.header('Content-Type', 'application/octet-stream')
        web.header('Content-Type','application/json; charset=utf-8', unique=True)  
        return json.dumps(text, encoding="utf-8")

    def GET(self, args):
        web.header('Content-Type','application/json; charset=utf-8', unique=True)  
        params = args.split("/")
        method = None
        method_args = None
        if len(params) == 1:
            method = params[0]
            method_args = ()
        else:
            method = params[0]
            method_args = params[1:]
        # usually this if json formatted
        data = web.data()
        if method == "get":
            return self.get(method_args, data)
        elif method == "exists":
            return self.exists(method_args, data)
        elif method == "isfile":
            return self.isfile(method_args, data)
        elif method == "isdir":
            return self.isdir(method_args, data)
        elif method == "stats":
            return self.stats(method_args, data)
        elif method == "put":
            return self.put(method_args, data)
        elif method == "mkdir":
            return self.mkdir(method_args, data)
        elif method == "listdir":
            return self.listdir(method_args, data)
        elif method == "delete":
            return self.delete(method_args, data)
        else:
            return "unknown method %s called" % method

    def get(self, args, data):
        """
	get block stored in blockstorage directory with hash

        if arguments end with / a directory listing will be served,
        otherwise the content of the specific file will be returned
        """
        filename = self.__get_filename(unicode(data, "utf-8"))
        if os.path.isfile(filename.encode("utf-8")):
            return self.__ret_json(open(filename.encode("utf-8"), "rb").read())
        else:
            logging.error("File %s does not exist", filename)
            web.notfound()

    def listdir(self, args, data):
        """
	get block stored in blockstorage directory with hash

        if arguments end with / a directory listing will be served,
        otherwise the content of the specific file will be returned
        """
        filename = self.__get_filename(unicode(data, "utf-8"))
        if os.path.isdir(filename.encode("utf-8")):
            return self.__ret_json(os.listdir(filename.encode("utf-8")))
        else:
            logging.error("File %s does not exist", filename)
            web.notfound()

    @calllogger
    def exists(self, args, data):
        """
        check if block with digest exists
        """
        filename = self.__get_filename(data.decode("utf-8"))
        if os.path.exists(filename.encode("utf-8")):
            web.ctx.status = '200 file or directory exists'
        else:
            web.notfound()

    @calllogger
    def isfile(self, args, data):
        """
        check if block with digest exists
        """
        filename = self.__get_filename(unicode(data, "utf-8"))
        if os.path.isfile(filename.encode("utf-8")):
            web.ctx.status = '200 file exists'
        else:
            web.notfound()

    @calllogger
    def isdir(self, args, data):
        """
        check if block with digest exists
        """
        filename = self.__get_filename(unicode(data, "utf-8"))
        if os.path.isdir(filename.encode("utf-8")):
            web.ctx.status = '200 file exists'
        else:
            web.notfound()

    def stats(self, args, data):
        """
        return file stats of stored file
        """
        filename = self.__get_filename(unicode(data, "utf-8"))
        stat = os.stat(filename.encode("utf-8"))
        ret_data = {
            "st_atime" : stat.st_atime,
            "st_mtime" : stat.st_mtime,
            "st_ctime" : stat.st_ctime,
        }
        return self.__ret_json(ret_data)

    def put(self, args, data):
        """
        store checksum in file named

        URL: /filename
        data: checksum (output of hexdigest())

        if no checksum is given, a directory will be created
        """
        mydata = json.loads(data)
        filename = self.__get_filename(mydata["name"].decode("utf-8"))
        assert len(data) > 0
        checksum = mydata["checksum"]
        if not os.path.isfile(filename):
            try:
                open(filename, "wb").write(checksum)
                web.ctx.status = '201 checksum stored'
            except IOError as exc:
                logging.exception(exc)
                os.unlink(filename)
        else:
            open(filename, "wb").write(checksum)
            web.ctx.status = '202 checksum overwritten'
            logging.info("file %s overwritten", filename)

    def mkdir(self, args, data):
        """
        store checksum in file named

        URL: /filename
        data: checksum (output of hexdigest())

        if no checksum is given, a directory will be created
        """
        filename = self.__get_filename(data.decode("utf-8"))
        logging.info("creating directory %s", filename)
        if not os.path.exists(filename):
            os.mkdir(filename)
        else:
            web.ctx.status = "405 file or direcory already exists"

    def delete(self, args, data):
        """
        delete block with md5checksum given

        the block should only be deleted if not used anymore in any FileStorage
        """
        filename = self.__get_filename(data.decode("utf-8"))
        if os.path.isfile(filename):
            os.unlink(filename)
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

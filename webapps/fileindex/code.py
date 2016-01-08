#!/usr/bin/python

import web
import os
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
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
STORAGE_DIR = CONFIG["STORAGE_DIR"]


class FileIndex(object):
    """stores filename to checksum index in normal filesystem"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

    def __get_filename(self, args):
        decoded = base64.b64decode(args[0])
        subname = decoded
        if decoded.startswith("/"):
            subname = decoded[1:]
        logging.debug("joining %s and %s", STORAGE_DIR, subname) 
        filename = os.path.join(STORAGE_DIR, subname)
        logging.debug("__getfilename returns %s", filename)
        return filename

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
        filename = self.__get_filename(args)
        if os.path.isfile(filename):
            web.header('Content-Type', 'application/octet-stream')
            return open(filename, "rb").read()
        elif os.path.isdir(filename):
            return json.dumps(os.listdir(filename))
        else:
            logging.error("File %s does not exist", filename)
            web.notfound()

    def listdir(self, args, data):
        """
	get block stored in blockstorage directory with hash

        if arguments end with / a directory listing will be served,
        otherwise the content of the specific file will be returned
        """
        filename = self.__get_filename(args)
        if os.path.isdir(filename):
            return json.dumps(os.listdir(filename))
        else:
            logging.error("File %s does not exist", filename)
            web.notfound()

    def exists(self, args, data):
        """
        check if block with digest exists
        """
        filename = self.__get_filename(args)
        if os.path.isfile(filename):
            logging.info("found file %s", filename)
            web.ctx.status = '200 file exists'
        elif os.path.isdir(filename):
            logging.info("found directory %s", filename)
            web.ctx.status = '201 directory exists'
        else:
            web.notfound()

    def stats(self, args, data):
        """
        return file stats of stored file
        """
        filename = self.__get_filename(args)
        stat = os.stat(filename)
        ret_data = {
            "st_atime" : stat.st_atime,
            "st_mtime" : stat.st_mtime,
            "st_ctime" : stat.st_ctime,
        }
        return json.dumps(ret_data)

    def put(self, args, data):
        """
        store checksum in file named

        URL: /filename
        data: checksum (output of hexdigest())

        if no checksum is given, a directory will be created
        """
        filename = self.__get_filename(args)
        assert len(data) > 0
        checksum = web.data()
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
        filename = self.__get_filename(args)
        logging.info("creating directory %s", filename)
        os.mkdir(filename)
        web.ctx.status = '203 directory created'

    def delete(self, args, data):
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

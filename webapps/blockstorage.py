#!/usr/bin/python

import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT)
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

    def __get_rfc_filename(self, checksum):
        """
        build and return absolute filpath

        params:
        checksum <basestring>

        ret:
        <basestring>
        """
        return os.path.join(STORAGE_DIR, "%s.rfc" % checksum)

    def __get_rfc(self, checksum):
        """get reference counter"""
        return int(open(self.__get_rfc_filename(checksum), "rb").read())

    def __set_rfc(self, checksum, value):
        """set reference counter to given value"""
        open(self.__get_rfc_filename(checksum), "wb").write(str(value))

    def __inc_rfc(self, checksum):
        """increment reference counter by 1"""
        self.__set_rfc(checksum, self.__get_rfc(checksum) + 1)

    def __dec_rfc(self, checksum):
        """decrement reference counter by 1"""
        self.__set_rfc(checksum, self.__get_rfc(checksum) - 1)

    @calllogger
    def GET(self, path):
        """
        get some data or return available blocks in storage

        if called with argument in URI, try to find the specified block,
        if no argument is given return a list of available blockchecksums
        """
        if len(path) == 0:
            # ls behaviour if no path is given
            web.header("Content-Type", "application/json")
            return json.dumps([filename[:-4] for filename in os.listdir(STORAGE_DIR)])
        else:
            args = path.split("/")
            # get data behaviour
            md5 = args[0]
            if os.path.isfile(self.__get_filename(md5)):
                # set to octet stream, binary data
                web.header('Content-Type', 'application/octet-stream')
                return open(self.__get_filename(md5), "rb").read()
            else:
                logging.error("File %s does not exist", self.__get_filename(md5))
                web.notfound()

    @calllogger
    def PUT(self, path):
        """
        PUT some new data to Blockstorage
        should always be called without arguments
        data in http data segment

        returns 200 if this was write
        returns 201 if this was update

        returns checksum of stored data
        """
        data = web.data()
        if len(data) > 0:
            digest = hashlib.md5()
            digest.update(data)
            md5 = digest.hexdigest()
            if not os.path.isfile(self.__get_filename(md5)):
                filename = self.__get_filename(md5)
                fo = open(filename, "wb")
                fo.write(data)
                fo.close()
                self.__set_rfc(md5, 1)
            else:
                web.ctx.status = '201 block rewritten'
                logging.info("block %s already exists", self.__get_filename(md5))
                filename = self.__get_filename(md5)
                self.__inc_rfc(md5)
            return digest.hexdigest()
        else:
            web.ctx.status = '501 no data to store'

    @calllogger
    def OPTIONS(self, path):
        """
        get some information of this checksum, but no data
        used as exists equivalent

        returns 200 if this checksum exists
        returns refcounter in data segment

        either raise 404
        """
        md5 = path.split("/")[0]
        if not os.path.exists(self.__get_filename(md5)):
            web.notfound()
        else:
            return self.__get_rfc(md5)

    @calllogger
    def DELETE(self, path):
        """
        delete some block with checksum, but only decrease
        refcounter until refcounter reaches 0, then delete data

        returns 200 if this checksum exists
        returns refcounter in data segment left
        """
        md5 = path.split("/")[0]
        if os.path.exists(self.__get_filename(md5)):
            rfc = self.__get_rfc(checksum) - 1
            if rfc > 0:
                self.__set_rfc(checksum, rfc)
                web.ctx.status = '201 refcounter decreased to %d' % rfc
                return rfc
            else:
                filename = self.__get_filename(md5)
                os.unlink(self.__get_filename(md5))
                os.unlink(self.__get_rfc_filename(md5))
                web.ctx.status = '200 block deleted'
        else:
            web.notfound()


if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

#!/usr/bin/python
"""
Blockstorage Web Application

Backend to store chunks of Blocks to disk, and retrieve thru RestFUL API
"""
import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import hashlib
import json

urls = (
    "/info", "BlockStorageInfo",
    "/(.*)", "BlockStorage",
)

# add wsgi functionality
CONFIG = {}
def load_config():
    configfile = os.path.expanduser("~/blockstorage.ini")
    global CONFIG
    if os.path.isfile(configfile):
        for line in open(configfile, "rb"):
            key, value = line.strip().split("=")
            CONFIG[key] = value
    else:
        logging.error("configfile %s does not exist", configfile)

APIKEYS = {}
def load_apikeys():
    """
    load APIKEYS from stored json file in user home directory
    """
    apikeysfile = os.path.expanduser("~/blockstorage_apikeys.json")
    if os.path.isfile(apikeysfile):
        global APIKEYS
        logging.error("loading APIKEYS from %s", apikeysfile)
        APIKEYS = json.load(open(apikeysfile))
        logging.error(APIKEYS)
    else:
        logging.error("no API-KEYS File found, create file %s", apikeysfile)


def authenticator(func):
    """
    decorator for authentication
    """
    def inner(*args, **kwds):
        call_str = "%s(%s, %s)" % (func.__name__, args[1:], kwds)
        logging.debug("calling %s", call_str)
        try:
            if web.ctx.env.get("HTTP_X_AUTH_TOKEN") is not None:
                if web.ctx.env.get("HTTP_X_AUTH_TOKEN") not in APIKEYS:
                    logging.error("X-AUTH-TOKEN %s not in allowed APIKEYS", web.ctx.env.get("HTTP_X_AUTH_TOKEN"))
                    web.ctx.status = '401 Unauthorized'
                else:
                    # authorization OK
                    logging.debug("successfully autorized with api-key %s", web.ctx.env.get("HTTP_X_AUTH_TOKEN")) 
                    ret_val = func(*args, **kwds)
                    return ret_val
            else:
                logging.error("X-AUTH-TOKEN HTTP Header missing")
                web.ctx.status = '401 Unauthorized'
            return
        except StandardError as exc:
            logging.exception(exc)
            logging.error("call to %s caused StandardError", call_str)
            web.internalerror()
    # set inner function __name__ and __doc__ to original ones
    inner.__name__ = func.__name__
    inner.__doc__ = func.__doc__
    return inner

def calllogger(func):
    """
    decorator for logging call arguments and duration
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


class BlockStorageInfo(object):
    """
    return inforamtions about BlockStorage
    """

    @authenticator
    def GET(self):
        """
        get some statistical data from blockstorage
        """
        web.header("Content-Type", "application/json")
        return json.dumps({
            "blocksize" : int(CONFIG["MAXSIZE"]),
            "blocks" : len(os.listdir(CONFIG["STORAGE_DIR"])),
            "st_mtime" : os.stat(CONFIG["STORAGE_DIR"]).st_mtime,
            "hashfunc" : CONFIG["HASHFUNC"],
            })


class BlockStorage(object):
    """
    Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier
    """

    def __init__(self):
        """__init__"""
        if not os.path.exists(CONFIG["STORAGE_DIR"]):
            logging.error("creating directory %s", CONFIG["STORAGE_DIR"])
            os.mkdir(CONFIG["STORAGE_DIR"])
        if CONFIG["HASHFUNC"] == "sha1":
            self.hashfunc = hashlib.sha1
        else:
            raise StandardError("only sha1 hashfunction implemented yet")

    def __get_filename(self, checksum):
        """
        build and return absolute filpath

        params:
        checksum <basestring>

        ret:
        <basestring>
        """
        return os.path.join(CONFIG["STORAGE_DIR"], "%s.bin" % checksum)

    @authenticator
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
            return json.dumps([filename[:-4] for filename in os.listdir(CONFIG["STORAGE_DIR"])])
        else:
            args = path.split("/")
            # get data behaviour
            checksum = args[0]
            if os.path.isfile(self.__get_filename(checksum)):
                # set to octet stream, binary data
                web.header('Content-Type', 'application/octet-stream')
                return open(self.__get_filename(checksum), "rb").read()
            else:
                logging.error("File %s does not exist", self.__get_filename(checksum))
                web.notfound()

    @authenticator
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
        if len(data) > CONFIG["MAXSIZE"]:
            web.ctx.status = '501 data too long'
            return
        if len(data) > 0:
            digest = self.hashfunc()
            digest.update(data)
            checksum = digest.hexdigest()
            if not os.path.isfile(self.__get_filename(checksum)):
                filename = self.__get_filename(checksum)
                fo = open(filename, "wb")
                fo.write(data)
                fo.close()
            else:
                web.ctx.status = '201 exists, not written'
                logging.info("block %s already exists", self.__get_filename(checksum))
            return checksum
        else:
            web.ctx.status = '501 no data to store'

    @authenticator
    @calllogger
    def OPTIONS(self, path):
        """
        get some information of this checksum, but no data
        used as exists equivalent

        returns 200 if this checksum exists
        returns refcounter in data segment

        either raise 404
        """
        checksum = path.split("/")[0]
        if not os.path.exists(self.__get_filename(checksum)):
            web.notfound()

    @authenticator
    @calllogger
    def NODELETE(self, path):
        """
        delete some block with checksum, but only decrease
        refcounter until refcounter reaches 0, then delete data

        returns 200 if this checksum exists
        returns refcounter in data segment left
        """
        checksum = path.split("/")[0]
        if os.path.exists(self.__get_filename(checksum)):
            filename = self.__get_filename(checksum)
            os.unlink(self.__get_filename(checksum))
            web.ctx.status = '200 block deleted'
        else:
            web.notfound()


if __name__ == "__main__":
    load_config()
    load_apikeys()
    app = web.application(urls, globals())
    app.run()
else:
    load_config()
    load_apikeys()
    application = web.application(urls, globals()).wsgifunc()

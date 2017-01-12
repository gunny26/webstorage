#!/usr/bin/python
"""
Webapp to store webstorage archives
"""
import web
import os
import time
import base64
import gzip
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import json

urls = (
    "/(.*)", "WebStorageArchive",
)

# add wsgi functionality
CONFIG = {}
def load_config():
    """
    load some configuration parameters from file
    """
    configfile = os.path.expanduser("~/webstoragearchive.ini")
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
    apikeysfile = os.path.expanduser("~/filestorage_apikeys.json")
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
                    logging.debug("successfully authorized with APIKEY %s", web.ctx.env.get("HTTP_X_AUTH_TOKEN")) 
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


class WebStorageArchive(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(CONFIG["STORAGE_DIR"]):
            logging.error("creating directory %s", CONFIG["STORAGE_DIR"])
            os.mkdir(CONFIG["STORAGE_DIR"])

    def __get_filename(self, checksum):
        return os.path.join(CONFIG["STORAGE_DIR"], "%s.json" % checksum)

    @authenticator
    @calllogger
    def GET(self, args):
        """
        get either content of named file, or directory listing
        """
        if len(args) == 0 or args == "":
            # return all files for this names hostname
            path = os.path.join(CONFIG["STORAGE_DIR"])
            ret_data = {}
            for filename in os.listdir(path):
                absfile = os.path.join(path, filename)
                ret_data[filename] = {
                    "size": os.stat(absfile).st_size
                }
            return json.dumps(ret_data)
        else:
            # return named filename
            logging.debug("received b64encoded filename %s", args)
            filename = base64.b64decode(args)
            logging.debug("decoded filename %s", filename)
            path = os.path.join(CONFIG["STORAGE_DIR"], filename)
            web.header('Content-Type', 'application/json')
            return gzip.open(path, "rb").read()

    @authenticator
    @calllogger
    def PUT(self, args):
        """
        same json information to file
        """
        data = web.data()
        if len(args) == 0 or args == "":
            # should be b64 encoded filename
            raise web.internalerror()
        else:
            logging.debug("received b64encoded filename %s", args)
            filename = base64.b64decode(args)
            logging.debug("decoded filename %s", filename)
            path = os.path.join(CONFIG["STORAGE_DIR"], filename)
            if os.path.isfile(path):
                logging.info("file %s exists already, not allowed", filename)
                raise web.notauthorized()
            else:
                gzip.open(path, "wb").write(data)

    @authenticator
    @calllogger
    def NODELETE(self, args):
        """
        delete nemd file
        """
        checksum = args.split("/")[0]
        assert len(checksum) == self.maxlength
        logging.debug("DELETE called, checksum=%s", checksum)
        if os.path.isfile(self.__get_filename(checksum)):
            os.unlink(self.__get_filename(checksum))
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

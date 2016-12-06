#!/usr/bin/python
"""
Webapp to store recipes for files chunked in blocks and stored in BlockStorage
"""
import web
import os
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import json

urls = (
    "/info", "FileStorageInfo",
    "/(.*)", "FileStorage",
)

# add wsgi functionality
CONFIG = {}
def load_config():
    """
    load some configuration parameters from file
    """
    configfile = os.path.expanduser("~/filestorage.ini")
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


class FileStorageInfo(object):
    """
    return inforamtions about FileStorage
    """

    @authenticator
    def GET(self):
        """
        get some statistical data from FileStorage
        """
        web.header("Content-Type", "application/json")
        return json.dumps({
            "files" : len(os.listdir(CONFIG["STORAGE_DIR"])),
            "st_mtime" : os.stat(CONFIG["STORAGE_DIR"]).st_mtime,
            "hashfunc" : CONFIG["HASHFUNC"],
            })


class FileStorage(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(CONFIG["STORAGE_DIR"]):
            logging.error("creating directory %s", CONFIG["STORAGE_DIR"])
            os.mkdir(STORAGE_DIR)
        if CONFIG["HASHFUNC"] == "sha1":
            self.maxlength = 40 # lenght of sha1 checksum
        else:
            raise StandardError("only sha1 checksums are implemented yet")

    def __get_filename(self, checksum):
        return os.path.join(CONFIG["STORAGE_DIR"], "%s.json" % checksum)

    @authenticator
    @calllogger
    def GET(self, args):
        """
        get block stored in blockstorage directory with hash

        GOOD : 200 : get metadata stored in file, json formatted
        BAD  : 404 : not found
        UGLY : decorator
        """
        if len(args) == 0:
            # no checksum given, do ls style
            web.header('Content-Type', 'application/json')
            return json.dumps([filename[:-5] for filename in os.listdir(CONFIG["STORAGE_DIR"])])
        else:
            checksum = args.split("/")[0]
            assert len(checksum) == self.maxlength
            if os.path.isfile(self.__get_filename(checksum)):
                web.header('Content-Type', 'application/json')
                return open(self.__get_filename(checksum), "rb").read()
            else:
                logging.error("File %s does not exist", self.__get_filename(checksum))
                web.notfound()

    @authenticator
    @calllogger
    def OPTIONS(self, args):
        """
        check if recipre with given checksum exists

        GOOD : 200 if file exists
        BAD  : 404 not found
        UGLY : decorator
        """
        checksum = args.split("/")[0]
        assert len(checksum) == self.maxlength
        if not os.path.isfile(self.__get_filename(checksum)):
            web.notfound()

    @authenticator
    @calllogger
    def PUT(self, args):
        """
        INSERT if not existing, compare if exists but do not overwrite
        put some arbitraty recipe in Store
        recipe is used to reassemble a file from its stored chunkes in BlockStorage

        the name of the recipe is the sha1 checksum of the reassembled file
        put data into storag

        GOOD : 200 storing metadata in file
               201 if file already existed
        BAD  : 404 if file not found
        UGLY : decorator or if no data is given
        """
        params = args.split("/")
        checksum = params[0]
        assert len(checksum) == self.maxlength
        logging.error("PUT recipe for file with checksum %s", checksum)
        metadata = json.loads(web.data())
        try:
            assert metadata["checksum"] == checksum
        except AssertionError as exc:
            logging.error("metadata: %s, checksum: %s", metadata, checksum)
            raise exc
        except TypeError as exc:
            logging.error("metadata: %s, checksum: %s", metadata, checksum)
            raise exc
        if metadata is not None:
            if not os.path.isfile(self.__get_filename(checksum)):
                try:
                    json.dump(metadata, open(self.__get_filename(checksum), "wb"))
                except TypeError:
                    os.unlink(self.__get_filename(checksum))
            else:
                # there is already a file, check if this is the same
                existing = json.load(open(self.__get_filename(checksum), "rb"))
                try:
                    assert existing == metadata
                    logging.debug("Metadata for %s already stored", checksum)
                    web.ctx.status = '201 metadata existed'
                except AssertionError as exc:
                    logging.exception(exc)
                    logging.error("existing: %s", existing)
                    logging.error("new: %s", metadata)
        else:
            web.notfound()

    @authenticator
    @calllogger
    def POST(self, args):
        """
        INSERT and overwrite existing data

        put some arbitraty recipe in Store
        recipe is used to reassemble a file from its stored chunkes in BlockStorage

        the name of the recipe is the sha1 checksum of the reassembled file
        put data into storag

        GOOD : 200 storing metadata in file
               201 if file already existed
        BAD  : 404 if file not found
        UGLY : decorator or if no data is given
        """
        params = args.split("/")
        checksum = params[0]
        assert len(checksum) == self.maxlength
        logging.error("PUT recipe for file with checksum %s", checksum)
        metadata = json.loads(web.data())
        try:
            assert metadata["checksum"] == checksum
        except AssertionError as exc:
            logging.error("metadata: %s, checksum: %s", metadata, checksum)
            raise exc
        except TypeError as exc:
            logging.error("metadata: %s, checksum: %s", metadata, checksum)
            raise exc
        if metadata is not None:
            try:
                json.dump(metadata, open(self.__get_filename(checksum), "wb"))
            except TypeError:
                os.unlink(self.__get_filename(checksum))
        else:
            web.notfound()

    @authenticator
    @calllogger
    def NODELETE(self, args):
        """
        delete block with checksum given

        the block should only be deleted if not used anymore in any FileStorage

        GOOD : 200 file deletes
        BAD  : 404 file not found
        UGLY : decorator
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

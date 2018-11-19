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
FORMAT = 'WebStorageArchveWebApp.%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import json
# own modules
from webstorageServer.Decorators import authenticator, calllogger

urls = (
    "/(.*)", "WebStorageArchive",
)

# load global config and read initial checksums list
#print(web.ctx)
#module_filename = web.ctx.environment.get("SCRIPT_NAME")
#logging.info("module_filename %s", module_filename)
config_filename = "/var/www/WebStorageArchiveWebApp.json" # omit .py extention
logging.info("config_filename %s", config_filename)
with open(config_filename, "rt") as infile:
    global CONFIG
    CONFIG = json.load(infile)
_storage_dir = CONFIG["storage_dir"]
if not os.path.exists(_storage_dir):
    logging.error("creating directory %s", _storage_dir)
    os.mkdir(_storage_dir)


class WebStorageArchive(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""

    def __init__(self):
        """__init__"""
        if not os.path.exists(CONFIG["storage_dir"]):
            logging.error("creating directory %s", CONFIG["storage_dir"])
            os.mkdir(CONFIG["storage_dir"])

    def __get_filename(self, checksum):
        return os.path.join(CONFIG["storage_dir"], "%s.json" % checksum)

    @authenticator(CONFIG)
    @calllogger
    def GET(self, args):
        """
        get either content of named file, or directory listing
        """
        if len(args) == 0 or args == "":
            # return all files for this names hostname
            path = os.path.join(CONFIG["storage_dir"], web.ctx.env.get("HTTP_X_AUTH_TOKEN"))
            ret_data = {}
            logging.info("directory listing for client subdir %s", path)
            for filename in os.listdir(path):
                absfile = os.path.join(path, filename)
                ret_data[filename] = {
                    "size": os.stat(absfile).st_size
                }
            return json.dumps(ret_data)
        else:
            # return named filename
            logging.debug("received b64encoded filename %s", args)
            filename = base64.b64decode(args).decode("utf-8")
            logging.debug("decoded filename %s", filename)
            path = os.path.join(CONFIG["storage_dir"], web.ctx.env.get("HTTP_X_AUTH_TOKEN"), filename)
            web.header('Content-Type', 'application/json')
            return gzip.open(path, "rb").read()

    @authenticator(CONFIG)
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
            filename = base64.b64decode(args).decode("utf-8")
            logging.debug("decoded filename %s", filename)
            path = os.path.join(CONFIG["storage_dir"], web.ctx.env.get("HTTP_X_AUTH_TOKEN"), filename)
            if os.path.isfile(path):
                logging.info("file %s exists already, not allowed", filename)
                raise web.notauthorized()
            else:
                gzip.open(path, "wb").write(data)

if __name__ == "__main__":
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

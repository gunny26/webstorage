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
# own modules
from webstorageServer.Decorators import authenticator, calllogger

urls = (
    "/info", "BlockStorageInfo",
    "/(.*)", "BlockStorage",
)

with open("/var/www/BlockStorage_001.json", "rt") as infile:
    CONFIG = json.load(infile)


class BlockStorageInfo(object):
    """
    return inforamtions about BlockStorage
    """

    @authenticator(CONFIG)
    def GET(self):
        """
        get some statistical data from blockstorage
        """
        web.header("Content-Type", "application/json")
        statvfs = os.statvfs(CONFIG["storage_dir"])
        b_free = statvfs.f_bfree * statvfs.f_bsize / CONFIG["blocksize"]
        i_free = statvfs.f_ffree
        free = int(min(b_free, i_free))
        b_size = statvfs.f_blocks * statvfs.f_bsize / CONFIG["blocksize"]
        i_size = statvfs.f_files
        size = int(min(b_size, i_size))
        return json.dumps({
            "id" : CONFIG["id"],
            "blocksize" : int(CONFIG["blocksize"]),
            "blocks" : len(os.listdir(CONFIG["storage_dir"])), # number of stored blocks
            "st_mtime" : os.stat(CONFIG["storage_dir"]).st_mtime,
            "hashfunc" : CONFIG["hashfunc"],
            "size" : size, # maximum number of blocks storable
            "free" : free # maximum number of blocks left to store
            })

_storage_dir = CONFIG["storage_dir"]
if not os.path.exists(_storage_dir):
    logging.error("creating directory %s", _storage_dir)
    os.mkdir(_storage_dir)
if CONFIG["hashfunc"] == "sha1":
    _hashfunc = hashlib.sha1
else:
    raise Exception("only sha1 hashfunction implemented yet")
_blocksize = CONFIG["blocksize"]
CHECKSUMS = [filename[:-4] for filename in os.listdir(_storage_dir)]
logging.info("found %d checksums in storage directory", len(CHECKSUMS))

class BlockStorage(object):
    """
    Stores Chunks of Data into Blockstorage Directory with sha1 as filename and identifier
    """

    def __init__(self):
        """__init__"""
        self.log = logging.getLogger(self.__class__.__name__)

    def __get_filename(self, checksum):
        """
        build and return absolute filpath

        params:
        checksum <basestring>

        ret:
        <basestring>
        """
        return os.path.join(_storage_dir, "%s.bin" % checksum)

    @property
    def checksums(self):
        return CHECKSUMS

    @authenticator(CONFIG)
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
            return json.dumps(self.checksums)
        else:
            args = path.split("/")
            # get data behaviour
            checksum = args[0]
            filename = self.__get_filename(checksum)
            if os.path.isfile(filename):
                # set to octet stream, binary data
                web.header('Content-Type', 'application/octet-stream')
                with open(filename, "rb") as infile:
                    return infile.read()
            else:
                logging.error("File %s does not exist", filename)
                web.notfound()

    @authenticator(CONFIG)
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
        if len(data) > int(_blocksize):
            web.ctx.status = '501 data too long'
            return
        if len(data) > 0:
            digest = _hashfunc()
            digest.update(data)
            checksum = digest.hexdigest()
            filename = self.__get_filename(checksum)
            if not os.path.isfile(filename):
                with open(filename, "wb") as outfile:
                    outfile.write(data)
                global CHECKSUMS
                CHECKSUMS.append(checksum) # remember newly created block
            else:
                web.ctx.status = '201 exists, not written'
                logging.info("block %s already exists", filename)
            return checksum
        else:
            web.ctx.status = '501 no data to store'

    @authenticator(CONFIG)
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

    @authenticator(CONFIG)
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
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

#!/usr/bin/python3
"""
Webapp to store recipes for files chunked in blocks and stored in BlockStorage
"""
import web
import os
import sys
import time
import logging
FORMAT = '%(module)s.%(funcName)s:%(lineno)s %(levelname)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)
import json
# own modules
from webstorageServer.Decorators import authenticator, calllogger
from webstorageClient import BlockStorageClient

urls = (
    "/info", "FileStorageInfo",
    "/download/(.*)", "FileStorageDownload", # used in JS Appliaction
    "/(.*)", "FileStorage",
)

# load global config and read initial checksums list
module_filename = os.path.basename(sys.argv[0])
logging.info("module_filename %s", module_filename)
config_filename = module_filename[:-3] + ".json" # omit .py extention
logging.info("config_filename %s", config_filename)
with open(os.path.join("/var/www", config_filename), "rt") as infile:
        CONFIG = json.load(infile)
_storage_dir = CONFIG["storage_dir"]
if not os.path.exists(_storage_dir):
    logging.error("creating directory %s", _storage_dir)
    os.mkdir(_storage_dir)
_hashfunc = CONFIG["hashfunc"]
if _hashfunc == "sha1":
    _maxlength = 40 # lenght of sha1 checksum
else:
    raise Exception("Config Error only sha1 checksums are implemented yet")
_checksums = [filename[:-5] for filename in os.listdir(_storage_dir)] 
logging.info("found %d existing checksums", len(_checksums))


class FileStorageInfo(object):
    """
    return inforamtions about FileStorage
    """

    @authenticator(CONFIG)
    def GET(self):
        """
        get some statistical data from FileStorage
        """
        web.header("Content-Type", "application/json")
        statvfs = os.statvfs(CONFIG["storage_dir"])
        free = statvfs.f_bfree * statvfs.f_bsize
        size = statvfs.f_blocks * statvfs.f_bsize
        return json.dumps({
            "files" : len(os.listdir(CONFIG["storage_dir"])), # number of stored files
            "st_mtime" : os.stat(CONFIG["storage_dir"]).st_mtime,
            "hashfunc" : CONFIG["hashfunc"],
            "free" : free,
            "size" : size
            })


class FileStorageDownload(object):
    """
    return inforamtions about FileStorage
    """
    def __init__(self):
        """__init__"""
        self.logger = logging.getLogger(self.__class__.__name__)

    def __get_filename(self, checksum):
        return os.path.join(CONFIG["storage_dir"], "%s.json" % checksum)

    def GET(self, parameters):
        """
        return full data stream of file specified by checksum
        """
        self.logger.info("calling %s", parameters)
        file_checksum = parameters.strip("/").split("/")[0]
        if os.path.isfile(self.__get_filename(file_checksum)):
            self.logger.info("found file with checksum %s", file_checksum)
            web.header('Content-Type', 'application/octet-stream')
            web.header('Transfer-Encoding', 'chunked')
            # omit Content-Length
            # disable compression of apache or other webservers
            with open(self.__get_filename(file_checksum), "rb") as infile:
                data = json.loads(infile.read())
                bsc = BlockStorageClient(cache=False)
                total_size = 0
                if len(data["blockchain"]) == 1:
                    total_size = len(bsc.get(data["blockchain"][0]))
                else:
                    total_size = bsc.blocksize * (len(data["blockchain"]) - 1) + len(bsc.get(data["blockchain"][-1]))
                # web.header('Content-Length', total_size)
                self.logger.info("returning %d blocks, total_length=%d", len(data["blockchain"]), total_size)
                for block_checksum in data["blockchain"]:
                    # goto : https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
                    block = bsc.get(block_checksum)
                    # web.header('Content-Length', str(len(block)))
                    self.logger.info("yielding block %s", block_checksum)
                    prefix = "%x\r\n" % len(block)
                    yield prefix + block + "\r\n"
                # sending end of stream information
                yield "0\r\n" + "\r\n"
        else:
            self.logger.error("File with checksum %s does not exist", self.__get_filename(checksum))
            raise web.notfound()


class FileStorage(object):
    """Stores Chunks of Data into Blockstorage Directory with md5 as filename and identifier"""


    def __get_filename(self, checksum):
        return os.path.join(CONFIG["storage_dir"], "%s.json" % checksum)

    @authenticator(CONFIG)
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
            return json.dumps(_checksums)
        else:
            checksum = args.split("/")[0]
            assert len(checksum) == _maxlength
            filename = self.__get_filename(checksum) 
            if os.path.isfile(filename):
                web.header('Content-Type', 'application/json')
                with open(filename, "rt") as infile:
                    return infile.read()
            else:
                logging.error("File %s does not exist", filename)
                web.notfound()

    @authenticator(CONFIG)
    @calllogger
    def OPTIONS(self, args):
        """
        check if recipre with given checksum exists

        GOOD : 200 if file exists
        BAD  : 404 not found
        UGLY : decorator
        """
        checksum = args.split("/")[0]
        assert len(checksum) == _maxlength
        if not os.path.isfile(self.__get_filename(checksum)):
            web.notfound()

    @authenticator(CONFIG)
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
        checksum = args.split("/")[0]
        if len(checksum) != _maxlength:
            raise web.HTTPError("400 bad Requests: checksum is not sha1")
        try:
            metadata = json.loads(web.data())
        except TypeError as exc:
            raise web.HTTPError("400 Bad Request: JSON format error")
        if metadata is not None:
            filename = self.__get_filename(checksum)
            if metadata["checksum"] == checksum:
                raise web.HTTPError("400 Bad Request: checksum mismatch")
            if not os.path.isfile(filename):
                self._dump(checksum, metadata)
            else:
                # there is already a file, check if this is the same
                with open(filename, "rt") as infile:
                    existing = json.load(infile)
                    try:
                        assert existing == metadata
                        self.logger.debug("Metadata for %s already stored", checksum)
                        web.ctx.status = '201 metadata existed'
                    except AssertionError as exc:
                        self.logger.exception(exc)
                        self.logger.error("existing: %s", existing)
                        self.logger.error("new: %s", metadata)
        else:
            web.notfound()

    @authenticator(CONFIG)
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
        checksum = args.split("/")[0]
        self.logger.error("POST for checksum %s", checksum)
        if len(checksum) != _maxlength:
            raise web.HTTPError("400 bad Requests: checksum is not sha1")
        try:
            metadata = json.loads(web.data())
        except TypeError as exc:
            raise web.HTTPError("400 Bad Request: JSON format error")
        if metadata:
            if metadata["checksum"] == checksum:
                raise web.HTTPError("400 Bad Request: checksum mismatch")
            self._dump(checksum, metadata)
        else:
            web.notfound()
    
    def _dump(self, checksum, metadata):
        filename = self.__get_filename(checksum)
        with open(filename, "wt") as outfile:
            json.dump(metadata, outfile)
            global _checksums
            _checksums.append(checksum)
 
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
    app = web.application(urls, globals())
    app.run()
else:
    application = web.application(urls, globals()).wsgifunc()

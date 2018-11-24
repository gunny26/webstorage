#!/usr/bin/python3
"""
Webapp to store recipes for files chunked in blocks and stored in BlockStorage
"""
import os
import logging
logging.basicConfig(level=logging.INFO)
import json
# non std modules
import yaml
from flask import Flask, request
# own modules
from webstorageClient import BlockStorageClient

app = Flask(__name__)
logger = logging.getLogger("FileStorageFlask")

def xapikey(config):
    def _xapikey(func):
        """
        decorator to check for existance and validity of X-APIKEY header
        """
        def __xapikey(*args, **kwds):
            if request.remote_addr in config["remote_addrs"]:
                app.logger.error("call from trusted client %s", request.remote_addr)
                return func(*args, **kwds)
            x_token = request.headers.get("x-apikey")
            if not x_token:
                app.logger.error("X-APIKEY header not provided")
                return "wrong usage", 401
            if x_token not in config["apikeys"]:
                app.logger.error("X-APIKEY is unknown")
                abort(403)
            if config["apikeys"][x_token]["remote_addrs"] and request.remote_addr not in config["apikeys"][x_token]["remote_addrs"]:
                app.logger.error("call from %s with %s not allowed", request.remote_addr, x_token)
                abort(403)
            app.logger.info("authorized call from %s with %s", request.remote_addr, x_token)
            return func(*args, **kwds)
        __xapikey.__name__ = func.__name__ # crucial setting to not confuse flask
        __xapikey.__doc__ = func.__doc__ # crucial setting to not confuse flask
        return __xapikey
    return _xapikey

@app.route("/info", methods=["GET"])
def info():
    """
    get some statistical data from FileStorage
    """
    statvfs = os.statvfs(CONFIG["storage_dir"])
    free = statvfs.f_bfree * statvfs.f_bsize
    size = statvfs.f_blocks * statvfs.f_bsize
    global CHECKSUMS
    response = app.response_class(
        json.dumps({
            "files" : len(CHECKSUMS), # number of stored files
            "st_mtime" : os.stat(CONFIG["storage_dir"]).st_mtime,
            "hashfunc" : CONFIG["hashfunc"],
            "free" : free,
            "size" : size
            }),
        status=200,
        mimetype="application/json"
    )
    return response

@app.route("/download/<file_checksum>", methods=["GET"])
def download(file_checksum):
    """
    return full data stream of file specified by checksum
    """
    if os.path.isfile(_get_filename(file_checksum)):
        logger.info("found file with checksum %s", file_checksum)
        # omit Content-Length
        # disable compression of apache or other webservers
        with open(_get_filename(file_checksum), "rt") as infile:
            data = json.load(infile)
            bsc = BlockStorageClient(cache=False)
            total_size = 0
            if len(data["blockchain"]) == 1:
                total_size = len(bsc.get(data["blockchain"][0]))
            else:
                total_size = bsc.blocksize * (len(data["blockchain"]) - 1) + len(bsc.get(data["blockchain"][-1]))
            # web.header('Content-Length', total_size)
            logger.info("returning %d blocks, total_length=%d", len(data["blockchain"]), total_size)
            for block_checksum in data["blockchain"]:
                # goto : https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Transfer-Encoding
                block = bsc.get(block_checksum)
                # web.header('Content-Length', str(len(block)))
                logger.info("yielding block %s", block_checksum)
                prefix = "%x\r\n" % len(block)
                yield prefix + block + "\r\n"
            # sending end of stream information
            yield "0\r\n" + "\r\n"
    else:
        logger.error("File with checksum %s does not exist", _get_filename(checksum))
        return "not found", 404

@app.route("/", methods=["GET"])
def get_checksums():
    """
    get block stored in blockstorage directory with hash

    GOOD : 200 : get metadata stored in file, json formatted
    BAD  : 404 : not found
    UGLY : decorator
    """
    # no checksum given, do ls style
    global CHECKSUMS
    response = app.response_class(
        json.dumps(CHECKSUMS),
        status=200,
        mimetype="application/json"
    )
    return response

@app.route("/<checksum>", methods=["GET"])
def get_checksum(checksum):
    """
    get block stored in blockstorage directory with hash

    GOOD : 200 : get metadata stored in file, json formatted
    BAD  : 404 : not found
    UGLY : decorator
    """
    filename = _get_filename(checksum) 
    if os.path.isfile(filename):
        with open(filename, "rt") as infile:
            response = app.response_class(
                infile.read(),
                status=200,
                mimetype="application/json"
            )
            return response
    else:
        logger.error("File %s does not exist", filename)
        return "not found", 404

@app.route("/<checksum>", methods=["OPTIONS"])
def exists(checksum):
    """
    check if recipre with given checksum exists

    GOOD : 200 if file exists
    BAD  : 404 not found
    UGLY : decorator
    """
    if not os.path.isfile(self.__get_filename(checksum)):
        return "checksum not found", 404
    return "checksum found", 200

@app.route("/<checksum>", methods=["PUT", "POST"])
def put_checksum(checksum):
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
    if len(checksum) != CONFIG("maxlength"):
        return "Bad Requests: checksum is not sha1", 400
    try:
        metadata = json.loads(request.data.decode("utf-8"))
    except TypeError as exc:
        return "Bad Request: JSON format error", 400
    if metadata:
        if metadata["checksum"] == checksum:
            return "Bad Request: checksum mismatch", 400
        filename = _get_filename(checksum)
        with open(filename, "wt") as outfile:
            json.dump(metadata, outfile)
            global CHECKSUMS
            CHECKSUMS.append(checksum)
        return "checksum stored", 200
    return "no data to store", 501

def _get_filename(checksum):
    """
    get os filename for provided checksum
    """
    assert len(checksum) == CONFIG["maxlength"]
    return os.path.join(CONFIG["storage_dir"], "%s.json" % checksum)

def _get_config(config_filename):
    """
    read configuration from yaml file
    """
    logger.info("loading config_filename %s", config_filename)
    with open(config_filename, "rt") as infile:
        config = yaml.load(infile)
    if not os.path.exists(config["storage_dir"]):
        logger.error("creating directory %s", config["storage_dir"])
        os.mkdir(config["storage_dir"])
    if config["hashfunc"] == "sha1":
        config["maxlength"] = 40 # lenght of sha1 checksum
    else:
        raise Exception("Config Error only sha1 checksums are implemented yet")
    return config

def _get_checksums(storage_dir):
    """
    generate list of stored checksums in RAM
    """
    logger.info("scanning directory %s", storage_dir)
    checksums = [filename[:-5] for filename in os.listdir(storage_dir)] 
    logger.info("found %d existing checksums", len(checksums))
    return checksums

application = app # needed for WSGI Apache module
CONFIG = {}
for key, value in _get_config("/var/www/FileStorageWebApp.yaml").items():
    CONFIG[key.lower()] = value
    CONFIG[key.upper()] = value
CHECKSUMS = _get_checksums(CONFIG["storage_dir"])

#!/usr/bin/python3
"""
Webapp to store recipes for files chunked in blocks and stored in BlockStorage
"""
import os
import sys
import json
import hashlib
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
try:
    import mod_wsgi
    name = mod_wsgi.process_group
except ImportError:
    name = __name__
# non-stdlib
import yaml
from flask import Flask, request, send_file, send_from_directory, Response
# own modules
sys.path.append("/opt/webstorage/server") #TODO: remove ugly hack
from blockchain import BlockChain

app = Flask(__name__)
bc = BlockChain()
logger = logging.getLogger(name)

def xapikey(func):
    """
    decorator to check for existance and validity of X-APIKEY header
    """
    def _xapikey(*args, **kwds):
        if request.remote_addr in app.config["remote_addrs"]:
            logger.error("call from trusted client %s", request.remote_addr)
            return func(*args, **kwds)
        x_token = request.headers.get("x-apikey")
        if not x_token:
            logger.error("X-APIKEY header not provided")
            return "wrong usage", 401
        if x_token not in app.config["apikeys"]:
            logger.error("X-APIKEY is unknown")
            return "x-apikey unknown", 403
        if app.config["apikeys"][x_token]["remote_addrs"] and request.remote_addr not in app.config["apikeys"][x_token]["remote_addrs"]:
            logger.error("call from %s with %s not allowed", request.remote_addr, x_token)
            return "call from wrong remote_addr", 403
        logger.info("authorized call from %s with %s", request.remote_addr, x_token)
        return func(*args, **kwds)
    _xapikey.__name__ = func.__name__ # crucial setting to not confuse flask
    _xapikey.__doc__ = func.__doc__ # crucial setting to not confuse flask
    return _xapikey




@app.route("/info", methods=["GET"])
def info():
    """
    get some statistical data from FileStorage
    """
    statvfs = os.statvfs(app.config["storage_dir"])
    free = statvfs.f_bfree * statvfs.f_bsize
    size = statvfs.f_blocks * statvfs.f_bsize
    blockchain = bc.last()
    response = app.response_class(
        json.dumps({
            "id" : app.config["id"],
            "blocksize" : int(app.config["blocksize"]),
            "hashfunc" : app.config["hashfunc"],
            "files" : len(app.config["checksums"]), # number of stored files
            "storage_st_mtime" : os.stat(app.config["storage_dir"]).st_mtime,
            "storage_free" : free,
            "storage_size" : size,
            "blockchain_epoch" : blockchain["epoch"], # blockchain epoch
            "blockchain_checksum" : blockchain["sha256_checksum"], # last blockchain hash
            "blockchain_seed" : app.config["blockchain_seed"], # initial seed used
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
    response = app.response_class(
        json.dumps(app.config["checksums"]),
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
    if not os.path.isfile(_get_filename(checksum)):
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
    if len(checksum) != app.config["maxlength"]:
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
            bc.add(checksum) # store in db
            app.config["checksums"].append(checksum) # store in RAM
        return "checksum stored", 200
    return "no data to store", 501

####################### private functions #################################

def _get_filename(checksum):
    """
    get os filename for provided checksum
    """
    assert len(checksum) == app.config["maxlength"]
    return os.path.join(app.config["storage_dir"], "%s.json" % checksum)

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

def application(environ, start_response):
    """
    will be called on every request
    """
    if app.config.get("id") is None:
        logger.info("INIT started")
        root = "/var/www/filestorage" # default root on linux
        if "filestorage.root" in environ:
            logger.info("using Root Directory %s set from WSGI", environ["filestorage.root"])
            root = environ["filestorage.root"]
        else:
            logger.info("using default Root %s", root)
        configfile = os.path.join(root, "filestorage.yaml")
        # application = app # needed for WSGI Apache module
        for key, value in _get_config(configfile).items(): # TODO: make this relative
            app.config[key] = value
        # initialize blockchain database
        bc.set_db(app.config["blockchain_db"])
        logger.info("using blockchain database %s", app.config["blockchain_db"])
        if app.config.get("blockchain_seed") is None:
            logger.info("seed not specified, so calculating from id of blockstorage")
            seed_sha256 = hashlib.sha256()
            seed_sha256.update(app.config["id"].encode("ascii"))
            app.config["blockchain_seed"] = seed_sha256.hexdigest()
        else:
            logger.info("blockchain seed defined in config file")
        logger.info("blockchain seed : %s", app.config["blockchain_seed"])
        bc.init(app.config["blockchain_seed"]) # TODO: check if seed is valid in database
        app.config["checksums"] = bc.checksums()
        logger.info("found %d checksums in blockchain database", len(app.config["checksums"]))
        logger.info("INIT finished")
    return app(environ, start_response)

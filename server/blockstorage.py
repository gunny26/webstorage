#!/usr/bin/python3
"""
Blockstorage Web Application
Backend to store chunks of Blocks to disk, and retrieve thru RestFUL API
"""
import os
import sys
import hashlib
import io
import json
import time
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

@app.route('/info')
@xapikey
def info():
    """
    get some statistical data from BlockStorage Backend
    """
    statvfs = os.statvfs(app.config["storage_dir"])
    b_free = statvfs.f_bfree * statvfs.f_bsize / app.config["blocksize"]
    i_free = statvfs.f_ffree
    free = int(min(b_free, i_free))
    b_size = statvfs.f_blocks * statvfs.f_bsize / app.config["blocksize"]
    i_size = statvfs.f_files
    size = int(min(b_size, i_size))
    blockchain = bc.last()
    response = app.response_class(
        json.dumps({
            "id" : app.config["id"],
            "blocksize" : int(app.config["blocksize"]),
            "hashfunc" : app.config["hashfunc"],
            "blocks" : len(app.config["checksums"]), # number of stored blocks
            "storage_st_mtime" : os.stat(app.config["storage_dir"]).st_mtime,
            "storage_size" : size, # maximum number of blocks storable
            "storage_free" : free, # maximum number of blocks left to store
            "blockchain_epoch" : blockchain["epoch"], # blockchain epoch
            "blockchain_checksum" : blockchain["sha256_checksum"], # last blockchain hash
            "blockchain_seed" : app.config["blockchain_seed"], # initial seed used
            }),
        status=200,
        mimetype="application/json"
    )
    return response

@app.route('/meta', methods=["GET"])
@xapikey
def get_meta():
    """
    stream checksum informations like st_mtime, st_ctime, size ...
    transfer encoded chunked, every line is json encoded structure
    """
    def generator():
        for checksum in app.config["checksums"]:
            filename = _get_filename(checksum)
            stat = os.stat(filename)
            data = {
                "filename" : checksum,
                "st_size" : stat.st_size,
                "st_mtime" : stat.st_mtime,
                "st_ctime" : stat.st_ctime
            }
            yield json.dumps(data) + "\n"
    return Response(generator(), mimetype="text/html")

@app.route('/stream', methods=["GET"])
@xapikey
def get_stream():
    """
    stream checksum binary data according to list of checksums provided
    data has to be in this format
    Content-Type : application/json is necessary to parse data the right way
    {
        "blockchain": [
            "d53b49de8175782729f614026e2155bec9252ec0"
        ],
        "blockhash_exists": 0, # not used at all
        "checksum": "d53b49de8175782729f614026e2155bec9252ec0", # not used at all
        "filehash_exists": false, # not used at all
        "mime_type": "application/octet-stream",
        "size": 854527
    }
    TODO: whats the upper limit on DATA size?
    """
    try:
        data = request.get_json()
        if "blockchain" not in data:
            return "Bad Request: JSON attributes missing", 400
    except (ValueError, TypeError) as exc:
        return "Bad Request: JSON format error", 400
    mimetype = "application/octet-stream"
    if "mime_type" in data:
        mimetype = data["mime_type"]
    def generator():
        length = 0
        for checksum in data["blockchain"]:
            filename = _get_filename(checksum)
            with open(filename, "rb") as infile:
                bin_data = infile.read()
                yield bin_data
                length += len(bin_data)
        logger.info("streamed %d blocks containing %d bytes", len(data["blockchain"]), length)
        logger.info("size in request was %s", data["size"])
    return Response(generator(), mimetype=mimetype)

@app.route('/checksums/<int:epoch>', methods=["GET"])
@xapikey
def get_checksums_stream(epoch=2):
    """
    stream list of checksums as big binary blob
    chunked in blocks

    epoch to indicate from wich epoch number the cecksums should be delivered
    epoch = 2 means start (lowest rowid is 1, and first epoch is seed)
    """
    mimetype = "application/octet-stream"
    blocklength = 512
    start = max(0, epoch-2)
    end = len(app.config["checksums"])
    def generator():
        for index in range(start, end, blocklength):
            byte_buffer = b""
            for checksum in app.config["checksums"][index:min(index+blocklength, end)]:
                byte_buffer += int(checksum, 16).to_bytes(20, "big")
            yield byte_buffer
    return Response(generator(), mimetype=mimetype)

@app.route('/', methods=["GET"])
@xapikey
def get_checksums():
    """
    if no argument is given return a list of available blockchecksums
    """
    starttime = time.time()
    response = app.response_class(
        response=json.dumps(app.config["checksums"]), # TODO: this could use much memory
        status=200,
        mimetype='application/json'
    )
    logger.info("response prepared in %0.2f" % (time.time() - starttime))
    return response

@app.route('/journal/<int:epoch>', methods=["GET"])
@xapikey
def get_journal(epoch):
    """
    return all checksums past epoch provided, or 404 if no data returned
    """
    checksums = bc.journal(epoch)
    if checksums:
        checksum_json = [checksum[0].decode("ascii") for checksum in checksums]
        logger.info("found %d checksums past epoch %d", len(checksums), epoch)
        response = app.response_class(
            response=json.dumps(checksum_json), # TODO: this could use much memory
            status=200,
            mimetype='application/json'
        )
        return response
    else:
        return "epoch not found", 404

@app.route('/epoch/<int:epoch>', methods=["GET"])
@xapikey
def get_epoch(epoch):
    """
    return epoch information, like checksum added, and blockchain checksum
    """
    res = bc.epoch(epoch)
    if res:
        data = {
            "epoch" : res[0],
            "checksum" : res[1].decode("ascii"),
            "sha256" : res[2].decode("ascii"),
        }
        response = app.response_class(
            response=json.dumps(data),
            status=200,
            mimetype='application/json'
        )
        return response
    else:
        return "epoch not found", 404

@app.route('/<checksum>', methods=["GET"])
@xapikey
def get_checksum(checksum):
    """
    send binary block with checksum to client
    mimetype is always set to application/octet-stream
    """
    filename = _get_filename(checksum)
    if os.path.isfile(filename):
        return send_from_directory(app.config["storage_dir"], "%s.bin" % checksum, mimetype="application/octet-stream")
    else:
        logger.error("File %s does not exist", filename)
        return "checksum not found", 404

@app.route('/<checksum>', methods=["PUT"])
@xapikey
def put_checksum(checksum):
    """
    PUT some new data to Blockstorage
    should always be called without arguments
    data in http data segment

    returns 200 if this was write
    returns 201 if this was update

    returns checksum of stored data
    """
    data = request.data
    if len(data) > int(app.config["blocksize"]):
        return "data too long", 501
    if len(data) > 0:
        digest = app.config["hashfunc_func"]()
        digest.update(data)
        own_checksum = digest.hexdigest()
        if own_checksum == checksum:
            filename = _get_filename(checksum)
            if not os.path.isfile(filename):
                with open(filename, "wb") as outfile:
                    outfile.write(data) # store on disk
                    bc.add(checksum) # store in db
                    app.config["checksums"].append(checksum) # store in RAM
                    return checksum, 200 # TODO: think about returning epoch and last hash
            else:
                logger.info("block %s already exists", filename)
                return checksum, 201
        else:
            return "checksum mismatch", 500
    else:
        return "no data to store", 501

@app.route('/<checksum>', methods=["OPTIONS"])
@xapikey
def options(checksum):
    """
    get some information of this checksum, but no data
    used as exists equivalent

    returns 200 if this checksum exists
    returns refcounter in data segment

    either raise 404
    """
    if checksum in app.config["checksums"] or os.path.exists(_get_filename(checksum)):
        return "checksum exists", 200
    return "checksum not found", 404

################# private functions ##############################

def _get_filename(checksum):
    """
    build and return absolute filpath

    params:
    checksum <basestring>

    ret:
    <basestring>
    """
    return os.path.join(app.config["storage_dir"], "%s.bin" % checksum)

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
        config["hashfunc_func"] = hashlib.sha1
        config["maxlength"] = 40 # lenght of sha1 checksum
    else:
        raise Exception("Config Error only sha1 checksums are implemented yet")
    return config

def _get_checksums(storage_dir):
    """
    generate list of stored checksums in RAM
    """
    logger.info("scanning directory %s", storage_dir)
    checksums = [filename.split(".")[0] for filename in os.listdir(storage_dir)] 
    logger.info("found %d existing checksums", len(checksums))
    return checksums

def application(environ, start_response):
    """
    will be called on every request
    """
    if app.config.get("id") is None:
        logger.info("INIT started")
        root = "/var/www/blockstorage" # default root on linux
        if "blockstorage.root" in environ:
            logger.info("using Blockstorage Root Directory %s set from WSGI", environ["blockstorage.root"])
            root = environ["blockstorage.root"]
        else:
            logger.info("using default Blockstorage Root /var/www/blockstorage")
        configfile = os.path.join(root, "blockstorage.yaml")
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

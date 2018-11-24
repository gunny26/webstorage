#!/usr/bin/python3
"""
Blockstorage Web Application
Backend to store chunks of Blocks to disk, and retrieve thru RestFUL API
"""
import os
import hashlib
import io
import json
import logging
logging.basicConfig(level=logging.INFO)
# no-stdlib
import yaml
from flask import Flask, request, send_file

app = Flask(__name__)
logger = logging.getLogger("BlockStorageFlask")

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

@app.route('/info')
def info():
    """
    get some statistical data from blockstorage
    """
    statvfs = os.statvfs(CONFIG["storage_dir"])
    b_free = statvfs.f_bfree * statvfs.f_bsize / CONFIG["blocksize"]
    i_free = statvfs.f_ffree
    free = int(min(b_free, i_free))
    b_size = statvfs.f_blocks * statvfs.f_bsize / CONFIG["blocksize"]
    i_size = statvfs.f_files
    size = int(min(b_size, i_size))
    global CHECKSUMS
    response = app.response_class(
        json.dumps({
            "id" : CONFIG["id"],
            "blocksize" : int(CONFIG["blocksize"]),
            "blocks" : len(CHECKSUMS), # number of stored blocks
            "st_mtime" : os.stat(CONFIG["storage_dir"]).st_mtime,
            "hashfunc" : CONFIG["hashfunc"],
            "size" : size, # maximum number of blocks storable
            "free" : free # maximum number of blocks left to store
            }),
        status=200,
        mimetype="application/json"
    )
    return response

@app.route('/', methods=["GET"])
def get_checksums():
    """
    if no argument is given return a list of available blockchecksums
    """
    response = app.response_class(
        response=json.dumps(CHECKSUMS),
        status=200,
        mimetype='application/json'
    )
    return response

@app.route('/<checksum>', methods=["GET"])
def get_checksum(checksum):
    """
    if called with argument in URI, try to find the specified block,
    """
    filename = _get_filename(checksum)
    if os.path.isfile(filename):
        with open(filename, "rb") as infile:
            return send_file(
                io.BytesIO(infile.read()),
                attachment_filename="%s.bin" % checksum,
                mimetype="application/octet-stream"
            )
    else:
        logger.error("File %s does not exist", filename)
        return "checksum not found", 404

@app.route('/<checksum>', methods=["PUT"])
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
    if len(data) > int(CONFIG["blocksize"]):
        return "data too long", 501
    if len(data) > 0:
        digest = CONFIG["hashfunc_func"]()
        digest.update(data)
        own_checksum = digest.hexdigest()
        assert own_checksum == checksum
        filename = _get_filename(checksum)
        if not os.path.isfile(filename):
            with open(filename, "wb") as outfile:
                outfile.write(data)
            global CHECKSUMS
            CHECKSUMS.append(checksum) # store for further use
            return checksum, 200
        else:
            logger.info("block %s already exists", filename)
            return "checksum already exists", 201
    else:
        return "no data to store", 501

@app.route('/<checksum>', methods=["OPTIONS"])
def options(checksum):
    """
    get some information of this checksum, but no data
    used as exists equivalent

    returns 200 if this checksum exists
    returns refcounter in data segment

    either raise 404
    """
    if checksum in CHECKSUMS or os.path.exists(_get_filename(checksum)):
        return "checksum exists", 200
    return "checksum not foun", 404

def _get_filename(checksum):
    """
    build and return absolute filpath

    params:
    checksum <basestring>

    ret:
    <basestring>
    """
    return os.path.join(CONFIG["storage_dir"], "%s.bin" % checksum)

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


application = app # needed for WSGI Apache module
CONFIG = {}
for key, value in _get_config("/var/www/BlockStorageWebApp.yaml").items():
    CONFIG[key.lower()] = value
    CONFIG[key.upper()] = value
CHECKSUMS = _get_checksums(CONFIG["storage_dir"])


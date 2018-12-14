#!/usr/bin/python3
"""
Blockstorage Web Application
Backend to store chunks of Blocks to disk, and retrieve thru RestFUL API
"""
import os
import hashlib
import io
import json
import time
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
# no-stdlib
import yaml
from flask import Flask, request, send_file, send_from_directory, Response

app = Flask(__name__)
logger = logging.getLogger("BlockStorageFlask")

def xapikey(func):
    """
    decorator to check for existance and validity of X-APIKEY header
    """
    def _xapikey(*args, **kwds):
        if request.remote_addr in CONFIG["remote_addrs"]:
            app.logger.error("call from trusted client %s", request.remote_addr)
            return func(*args, **kwds)
        x_token = request.headers.get("x-apikey")
        if not x_token:
            app.logger.error("X-APIKEY header not provided")
            return "wrong usage", 401
        if x_token not in CONFIG["apikeys"]:
            app.logger.error("X-APIKEY is unknown")
            return "x-apikey unknown", 403
        if CONFIG["apikeys"][x_token]["remote_addrs"] and request.remote_addr not in CONFIG["apikeys"][x_token]["remote_addrs"]:
            app.logger.error("call from %s with %s not allowed", request.remote_addr, x_token)
            return "call from wrong remote_addr", 403
        app.logger.info("authorized call from %s with %s", request.remote_addr, x_token)
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
    statvfs = os.statvfs(CONFIG["storage_dir"])
    b_free = statvfs.f_bfree * statvfs.f_bsize / CONFIG["blocksize"]
    i_free = statvfs.f_ffree
    free = int(min(b_free, i_free))
    b_size = statvfs.f_blocks * statvfs.f_bsize / CONFIG["blocksize"]
    i_size = statvfs.f_files
    size = int(min(b_size, i_size))
    blockchain = _blockchain_last()
    response = app.response_class(
        json.dumps({
            "id" : CONFIG["id"],
            "blocksize" : int(CONFIG["blocksize"]),
            "blocks" : len(CHECKSUMS), # number of stored blocks
            "st_mtime" : os.stat(CONFIG["storage_dir"]).st_mtime,
            "hashfunc" : CONFIG["hashfunc"],
            "size" : size, # maximum number of blocks storable
            "free" : free, # maximum number of blocks left to store
            "epoch" : blockchain["epoch"], # blockchain epoch
            "sha256_checksum" : blockchain["sha256_checksum"], # last blockchain hash
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
        for checksum in CHECKSUMS:
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

@app.route('/', methods=["GET"])
@xapikey
def get_checksums():
    """
    if no argument is given return a list of available blockchecksums
    """
    starttime = time.time()
    response = app.response_class(
        response=json.dumps(CHECKSUMS), # TODO: this could use much memory
        status=200,
        mimetype='application/json'
    )
    logger.info("response prepared in %0.2f" % (time.time() - starttime))
    return response

@app.route('/<checksum>', methods=["GET"])
@xapikey
def get_checksum(checksum):
    """
    send binary block with checksum to client
    mimetype is always set to application/octet-stream
    """
    filename = _get_filename(checksum)
    if os.path.isfile(filename):
        return send_from_directory(CONFIG["storage_dir"], "%s.bin" % checksum, mimetype="application/octet-stream")
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
    if len(data) > int(CONFIG["blocksize"]):
        return "data too long", 501
    if len(data) > 0:
        digest = CONFIG["hashfunc_func"]()
        digest.update(data)
        own_checksum = digest.hexdigest()
        if own_checksum == checksum:
            filename = _get_filename(checksum)
            if not os.path.isfile(filename):
                with open(filename, "wb") as outfile:
                    outfile.write(data) # store on disk
                    _blockchain_add(checksum) # store in db
                    CHECKSUMS.append(checksum) # store in RAM
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
    if checksum in CHECKSUMS or os.path.exists(_get_filename(checksum)):
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

def _blockchain_init(sha256_seed):
    """
    initialize blockchain table with first entry aka epoch 0
    """
    with sqlite3.connect(DB_FILENAME) as con:
        c = con.cursor()
        res = c.execute("SELECT name FROM sqlite_master WHERE name='blockchain'")
        if not res.fetchone():
            logger.info("creating table blockchain and inserting seed checksum")
            c.execute("create table if not exists blockchain (checksum char(40), sha256 char(64))")
            c.execute("insert into blockchain values (?, ?)", (None, sha256_seed.encode("ascii")))
            con.commit()

def _blockchain_add(checksum):
    """
    add checksum to blockchain and return epoch and last sha56
    """
    with sqlite3.connect(DB_FILENAME) as con:
        c = con.cursor()
        last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
        epoch, last_sha256 = last_epoch.fetchone()
        sha256 = hashlib.sha256()
        sha256.update(str(epoch).encode("ascii") + last_sha256 + checksum.encode("ascii"))
        # print(epoch, last_sha256, sha256.hexdigest())
        c.execute("insert into blockchain values (?, ?)", (checksum.encode("ascii"), sha256.hexdigest().encode("ascii")))
        return {"epoch" : epoch, "sha256_checksum" : sha256.hexdigest().encode("ascii")}

def _blockchain_last():
    """
    return last epoch and last sha256
    """
    with sqlite3.connect(DB_FILENAME) as con:
        c = con.cursor()
        last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
        epoch, sha256 = last_epoch.fetchone()
        return {"epoch" : epoch, "sha256_checksum" : sha256.decode("ascii")}

def _blockchain_checksums():
    """
    return all checksums
    """
    with sqlite3.connect(DB_FILENAME) as con:
        c = con.cursor()
        checksums = c.execute("select checksum from blockchain where checksum is not null").fetchall()
        logger.info("found %d checksums in database", len(checksums))
        return [checksum[0].decode("ascii") for checksum in checksums]

def _blockchain_diff(epoch):
    """
    return all checksums after epoch
    """
    with sqlite3.connect(DB_FILENAME) as con:
        c = con.cursor()
        checksums = c.execute("select checksum from blockchain where rowid>?", (epoch,)).fetchall()
        return [checksum[0].decode("ascii") for checksum in checksums]


logger.info("INIT started")
application = app # needed for WSGI Apache module
app.config["app_config"] = {}
CONFIG = app.config["app_config"]
for key, value in _get_config("/var/www/BlockStorageWebApp.yaml").items(): # TODO: make this relative
    CONFIG[key.lower()] = value
app.config["app_checksums"] = {}
# initialize blockchain database
DB_FILENAME = app.config["app_config"]["blockchain_db"]
seed_sha256 = hashlib.sha256()
seed_sha256.update(app.config["app_config"]["id"].encode("ascii"))
_blockchain_init(seed_sha256.hexdigest())
CHECKSUMS = app.config["app_checksums"]
CHECKSUMS = _blockchain_checksums()
logger.info("found %d checksums in blockchain database", len(CHECKSUMS))
#checksums_fs = _get_checksums(CONFIG["storage_dir"])
#logger.info("found %d checksums in filesystem", len(checksums_fs))
#assert len(checksums_fs) == len(CHECKSUMS)
logger.info("INIT finished")

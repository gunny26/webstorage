#!/usr/bin/python3
"""
Webapp to store webstorage archives
"""
import os
import time
import base64
import gzip
import logging
from functools import wraps
# non std
import yaml
from flask import Flask, request, jsonify
from flask import g # g is not PEP-8

app = Flask(__name__)
application = app
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("webarchive")
with open("/var/www/webarchive/webarchive.yaml", "rt") as infile:
    app.config["app_config"] = yaml.load(infile)
    logger.debug(app.config["app_config"])
    CONFIG = app.config["app_config"] # shortcut
if not os.path.exists(CONFIG["storage_dir"]):
    logging.error("creating directory %s", CONFIG["storage_dir"])
    os.mkdir(CONFIG["storage_dir"])

# also on TOP to use it further down
def _apihandler(func):
    @wraps(func)
    def decorated_function(*args, **kwds):
        try:
            g.client_id = request.headers.get("X-Apikey")
            if g.client_id is None:
                logger.error("client_id missing")
                logger.error(dict(request.headers))
                return jsonify({"error" : "client_id missing"}), 400
            g.path = os.path.join(CONFIG["storage_dir"], g.client_id)
            if not os.path.isdir(g.path):
                logger.error("client directory %s does not exist" % g.path)
                return jsonify({"error" : "client directory does not exist"}), 400
            ret = func(*args, **kwds)
            if isinstance(ret, dict) or isinstance(ret, list):
                return jsonify(ret)
            return ret
        except Exception as exc:
            logger.exception(exc)
            return jsonify({"error": str(exc), "status_code": 500})
    return decorated_function

@app.route("/", methods=["GET"])
@_apihandler
def get():
    """
    get either content of named file, or directory listing
    """
    # return all files for this names hostname
    ret_data = {}
    logger.info("directory listing for client subdir %s", g.path)
    for filename in os.listdir(g.path):
        absfile = os.path.join(g.path, filename)
        ret_data[filename] = {
            "size": os.stat(absfile).st_size
        }
    return ret_data

@app.route("/<b64filename>", methods=["GET"])
@_apihandler
def get_archive(b64filename):
    # return named filename
    logger.debug("received b64encoded filename %s", b64filename)
    filename = base64.b64decode(b64filename).decode("utf-8")
    absfilename = os.path.join(g.path, filename)
    if os.path.isfile(absfilename):
        logger.debug("decoded filename %s", filename)
        return gzip.open(absfilename, "rt").read()
    else:
        raise IOError("archive %s not found" % filename)

@app.route("/<b64filename>", methods=["PUT"])
@_apihandler
def put(b64filename):
    """
    same json information to file
    """
    try:
        data = json.loads(request.data.decode("utf-8"))
    except TypeError as exc:
        raise IOError("Bad Request: JSON format error")
    logger.debug("received b64encoded filename %s", args)
    filename = base64.b64decode(args).decode("utf-8")
    logger.debug("decoded filename %s", filename)
    absfilename = os.path.join(g.path, filename)
    if os.path.isfile(absfilename):
        raise IOError("archive %s already exists" % filename)
    else:
        gzip.open(g.path, "wt").write(request.data)
        return {"message": "archive written"}

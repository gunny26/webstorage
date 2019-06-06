#!/usr/bin/python3
import os
import hashlib
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
# non std
import yaml

logger = logging.getLogger(__name__)

def blockchain_init(con, sha256_seed):
    """
    initialize blockchain table with first entry aka epoch 0
    """
    c = con.cursor()
    c.execute("drop table if exists blockchain")
    logger.info("creating table blockchain and inserting seed checksum")
    c.execute("create table if not exists blockchain (checksum char(40), sha256 char(64))")
    c.execute("insert into blockchain values (?, ?)", (None, sha256_seed.encode("ascii")))
    con.commit()

config = yaml.load(open("/var/www/blockstorage/blockstorage.yaml"))
if "blockchain_seed" in config:
    seed = config["blockchain_seed"]
else:
    logger.info("calculating seed from blockstorage id")
    logger.info("this could be a problem, if you plan to mirror from another blockstorage")
    sha256 = hashlib.sha256()
    sha256.update(config["id"].encode("ascii"))
    seed =  sha256.hexdigest()
logger.info("seed for this BlockStorage: %s", seed)
none_checksum = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
with sqlite3.connect(config["blockchain_db"]) as con:
    blockchain_init(con, seed)
    con.commit()

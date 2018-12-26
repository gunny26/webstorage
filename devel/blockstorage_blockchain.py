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

def blockchain_add(con, checksum):
    c = con.cursor()
    last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
    epoch, last_sha256 = last_epoch.fetchone()
    sha256 = hashlib.sha256()
    sha256.update(str(epoch).encode("ascii") + last_sha256 + checksum.encode("ascii"))
    # print(epoch, last_sha256, sha256.hexdigest())
    c.execute("insert into blockchain values (?, ?)", (checksum.encode("ascii"), sha256.hexdigest().encode("ascii")))
    return {"epoch" : epoch, "sha256_checksum" : sha256.hexdigest().encode("ascii")}

def blockchain_last(con):
    c = con.cursor()
    last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
    epoch, sha256 = last_epoch.fetchone()
    return {"epoch" : epoch, "sha256_checksum" : sha256.decode("ascii")}

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
checksums = [filename.split(".")[0] for filename in os.listdir(config["storage_dir"])]
logger.info("found %d checksums stored on filesystem" % len(checksums))
none_checksum = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
with sqlite3.connect(config["blockchain_db"]) as con:
    blockchain_init(con, seed)
    for epoch, checksum in enumerate(checksums):
        if checksum == none_checksum:
            logger.info("skipping None checksum %s", checksum)
            continue
        filename = os.path.join(config["storage_dir"], "%s.bin" % checksum)
        if os.stat(filename).st_size == 0:
            logger.error("found zero sized file %s, deleting" % filename)
            os.unlink(filename)
        else:
            with open(filename, "rb") as infile:
                sha1 = hashlib.sha1()
                sha1.update(infile.read())
                if sha1.hexdigest() == checksum:
                    logger.info("checksum %s verified", checksum)
                    blockchain_add(con, checksum)
                else:
                    logger.error("checksum error for file %s, deleting", filename)
                    os.unlink(filename)
    con.commit()
    c = con.cursor()
    c.execute("vacuum")
    print(blockchain_last(con))

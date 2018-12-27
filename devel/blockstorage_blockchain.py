#!/usr/bin/python3
import os
import hashlib
import sqlite3
import logging
logging.basicConfig(level=logging.INFO)
# non std
import yaml
from webstorageServer import blockchain

logger = logging.getLogger(__name__)

bc = blockchain.BlockChain()
bc.set_db("/var/www/blockstorage/blockstorage_blockchain.db")
config = yaml.load(open("/var/www/blockstorage/blockstorage.yaml"))
if "blockchain_seed" in config and config["blockchain_seed"] is not None:
    seed = config["blockchain_seed"]
else:
    logger.info("calculating seed from blockstorage id")
    logger.info("this could be a problem, if you plan to mirror from another blockstorage")
    sha256 = hashlib.sha256()
    sha256.update(config["id"].encode("ascii"))
    seed =  sha256.hexdigest()
logger.info("seed for this BlockStorage: %s", seed)
bc.init(seed) # it's save to call
existing_checksums = bc.checksums()
checksums = [filename.split(".")[0] for filename in os.listdir(config["storage_dir"])]
logger.info("found %d checksums stored on filesystem" % len(checksums))
none_checksum = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
len_checksums = len(checksums)
for epoch, checksum in enumerate(checksums):
    if checksum in existing_checksums:
        logger.debug("skipping checksum %s, already in blockchain", checksum)
        continue
    pct = 100 * epoch / len_checksums
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
                logger.info("%0.2f %% checksum %s verified", pct, checksum)
                bc.add(checksum)
            else:
                logger.error("checksum error for file %s, deleting", filename)
                os.unlink(filename)
print(bc.last(con))

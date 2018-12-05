#!/usr/bin/python3
import hashlib
import sqlite3
# own modules
from webstorageClient import BlockStorageClient

def blockchain_init(con, sha256_seed):
    """
    initialize blockchain table with first entry aka epoch 0
    """
    c = con.cursor()
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


bsc = BlockStorageClient()
print(bsc.info)
sha256 = hashlib.sha256()
sha256.update("70b1b85c-e64c-4445-a02f-0ed612ff8ff3".encode("ascii"))
seed =  sha256.hexdigest()
print(seed)
with sqlite3.connect("blockchain.db") as con:
    blockchain_init(con, seed)
    for epoch, checksum in enumerate(bsc.checksums):
        blockchain_add(con, checksum)
    con.commit()
    c = con.cursor()
    c.execute("vacuum")
    print(blockchain_last(con))

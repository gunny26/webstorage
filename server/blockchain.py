#!/usr/bin/python3
"""
Blockchain class
"""
import hashlib
import sqlite3
import logging

class BlockChain(object):

    def __init__(self):
        self._db = None

    def set_db(self, db_filename):
        logging.info("set db_filename to %s", db_filename)
        self._db = db_filename
 
    def init(self, sha256_seed):
        """
        initialize blockchain table with first entry aka epoch 0
        """
        with sqlite3.connect(self._db) as con:
            c = con.cursor()
            res = c.execute("SELECT name FROM sqlite_master WHERE name='blockchain'")
            if not res.fetchone(): # if table does not exist
                logging.info("creating table blockchain and inserting seed checksum")
                c.execute("create table if not exists blockchain (checksum char(40), sha256 char(64))")
                c.execute("insert into blockchain values (?, ?)", (None, sha256_seed.encode("ascii")))
                con.commit()
               
    def add(self, checksum):
        """
        add checksum to blockchain and return epoch and last sha256
        """
        with sqlite3.connect(self._db) as con:
            c = con.cursor()
            last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
            epoch, last_sha256 = last_epoch.fetchone()
            sha256 = hashlib.sha256()
            sha256.update(str(epoch).encode("ascii") + last_sha256 + checksum.encode("ascii"))
            c.execute("insert into blockchain values (?, ?)", (checksum.encode("ascii"), sha256.hexdigest().encode("ascii")))
            con.commit()
            return {"epoch" : epoch, "sha256_checksum" : sha256.hexdigest().encode("ascii")}

    def last(self):
        """
        return last epoch and last sha256
        """
        with sqlite3.connect(self._db) as con:
            c = con.cursor()
            last_epoch = c.execute("select rowid, sha256 from blockchain order by rowid desc limit 1")
            epoch, sha256 = last_epoch.fetchone()
            return {"epoch" : epoch, "sha256_checksum" : sha256.decode("ascii")}

    def checksums(self):
        """
        return all checksums available
        """
        with sqlite3.connect(self._db) as con:
            c = con.cursor()
            checksums = c.execute("select checksum from blockchain where checksum is not null").fetchall()
            logging.info("found %d checksums in database", len(checksums))
            return [checksum[0].decode("ascii") for checksum in checksums]

    def journal(self, epoch):
        """
        return list of checksums beginning epoch+1
        """
        with sqlite3.connect(app.config["blockchain_db"]) as con:
            c = con.cursor()
            return c.execute("select checksum from blockchain where rowid > ?", (epoch,)).fetchall()

    def epoch(self, epoch):
        """
        return row with specific epoch
        """
        with sqlite3.connect(self._db) as con:
            c = con.cursor()
            return c.execute("select rowid, checksum, sha256 from blockchain where rowid = ?", (epoch,)).fetchone()

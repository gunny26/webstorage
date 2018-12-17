#!/usr/bin/python
import sys
import os
import time
import json
import random
import hashlib
import unittest
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from BlockStorageClient import BlockStorageClient as BlockStorageClient


class Test(unittest.TestCase):

    def setUp(self):
        self.bs = BlockStorageClient()

    def test_info(self):
        info = self.bs.info
        print(json.dumps(info, indent=4))
        data =  { # example data
            "blocksize": 1048576,
            "blocks": 938481,
            "id": "70b1b85c-e64c-4445-a02f-0ed612ff8ff3",
            "storage_st_mtime": 1544969633.7850456,
            "hashfunc": "sha1",
            "storage_free": 1386971,
            "blockchain_seed": "fbdb26273c17fbbc301da56d02275643b41365300f0b1e657497d1e3564a09ed",
            "storage_size": 1906797,
            "blockchain_epoch": 938482,
            "blockchain_checksum": "7e0a8fd637eb6ac5ab713783cc39b72647f01320aa2e673c557bea8230b95af9"
        }
        self.assertTrue(key in info for key in data)
        self.assertTrue(info["blocksize"] == 1024 * 1024)
        self.assertTrue(isinstance(info["blocksize"], int))
        self.assertTrue(isinstance(info["blocks"], int))
        self.assertTrue(isinstance(info["storage_free"], int))
        self.assertTrue(isinstance(info["storage_size"], int))
        self.assertTrue(isinstance(info["blockchain_epoch"], int))
        self.assertTrue(len(info["blockchain_seed"]) == 64)
        self.assertTrue(len(info["blockchain_checksum"]) == 64)
        self.assertTrue(info["blockchain_epoch"] == info["blocks"] + 1)

    def test_epoch(self):
        info = self.bs.info
        epoch = self.bs.get_epoch(info["blockchain_epoch"])
        print(json.dumps(epoch, indent=4))
        self.assertTrue(epoch["epoch"] == info["blockchain_epoch"])
        self.assertTrue(epoch["sha256"] == info["blockchain_checksum"])

    def test_journal(self):
        info = self.bs.info
        epoch = self.bs.get_epoch(info["blockchain_epoch"])
        journal = self.bs.get_journal(info["blockchain_epoch"] - 1)
        print(json.dumps(journal, indent=4))
        self.assertTrue(len(journal) == 1)
        self.assertTrue(journal[0] == epoch["checksum"])

    def test_checksums(self):
        info = self.bs.info
        epoch = self.bs.get_epoch(info["blockchain_epoch"])
        journal = self.bs.get_journal(info["blockchain_epoch"] - 1)
        bin_data = self.bs.get_checksums(epoch=info["blockchain_epoch"] - 1)
        print(json.dumps(bin_data, indent=4))

    def test_put(self):
        data = "A" * 1024
        data = data.encode("ascii")
        sha1 = hashlib.sha1()
        sha1.update(data)
        self.assertEqual(len(data), 1024)
        checksum, status = self.bs.put(data)
        self.assertEqual(sha1.hexdigest(), checksum)
        self.assertTrue(checksum == "746c3f4d286c531e065e8af76e0ac0868831c6b4")
        data1 = self.bs.get("746c3f4d286c531e065e8af76e0ac0868831c6b4")
        self.assertTrue(data == data1)

    def test_put_too_long(self):
        data = "A" * 1025 * 1024
        data = data.encode("ascii")
        print(len(data))
        checksum, status = self.bs.put(data)

    def test_get_verify(self):
        data = "A" * 1024
        data = data.encode("ascii")
        checksum, status = self.bs.put(data)
        data1 = self.bs.get_verify(checksum)
        self.assertTrue(data == data1)

    def test_get_exists(self):
        data = "A" * 1024
        data = data.encode("ascii")
        checksum, status = self.bs.put(data)
        self.assertTrue(self.bs.exists(checksum))
        self.assertTrue(self.bs.exists("0" * 40)) # illegal checksum



if __name__ == "__main__":
    unittest.main()

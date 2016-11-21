#ndard.at/!/usr/bin/python

import sys
import os
import json
import requests
import hashlib
import time
import tempfile
import base64
import logging
#pool = urllib3.PoolManager()
from WebStoreageClient import BlockStorage as BlockStorage
from WebStoreageClient import FileStorage as FileStorage


import unittest
class TestClass(unittest.TestCase):

    BLOCKSIZE = 1024 * 1024
    bs = BlockStorageClient()
    fs = FileStorageClient(bs=bs, blocksize=BLOCKSIZE)
    fi = FileIndexClient(fs)

    def no_test_fileindex(self):
        """
        complete walk over every file,
        test existance of every used block
        """
        # this should be directory
        data = self.fi.listdir("/")
        assert isinstance(data, list)
        assert len(data) > 0
        for item in data:
            print self.fi.stats(item)
            if self.fi.isfile(item):
                print "file : %s" % item
                checksum = self.fi.get(item)
                #print "\tfi-checksum : %s" % checksum
                #checksum.replace("", "")
                metadata = self.fs.view(checksum)
                #print "\tfs-checksum : %s" % metadata["checksum"]
                assert checksum == metadata["checksum"]
                print "\tchecking %d blocks" % len(metadata["blockchain"])
                for block in metadata["blockchain"]:
                    assert self.bs.exists(block)
            elif self.fi.isdir(item):
                print "dir  : %s" % item
                dirlist = self.fi.listdir(item)
                print len(dirlist)
                print dirlist
            else:
                print "other: %s" % item

    def no_test_fi_walk(self):
        blockdigest = {}
        for item in self.fi.walk("/"):
            print item
            for block in self.fs.view(self.fi.get(item))["blockchain"]:
                if block in blockdigest:
                    blockdigest[block] += 1
                else:
                    blockdigest[block] = 1
        print "number of blocks stored : %s" % len(blockdigest.keys())
        print "blocks used more than once : %s" % len([value for digest, count in blockdigest.items() if count > 1])

    def test_up_down(self):
        fh = tempfile.NamedTemporaryFile("wb", delete=False)
        name = fh.name
        basename = os.path.basename(name)
        testdata = "0123456789" * 1000000
        fh.write(testdata)
        fh.close()
        print "wrote %d bytes to %s" % (len(testdata), name)
        metadata = self.fs.put(open(name, "rb"))
        self.fi.put(basename, metadata["checksum"])
        # self.fi.write(open(name, "rb"), basename)
        # get it back
        checksum = self.fi.get(basename)
        fh = tempfile.NamedTemporaryFile("wb", delete=False)
        name_2 = fh.name
        for chunk in self.fs.get(checksum):
            fh.write(chunk)
        fh.close()
        # compare
        assert open(name).read() == open(name_2).read()
        os.unlink(name)
        os.unlink(name_2)

    def test_up(self):
        fh = tempfile.NamedTemporaryFile("wb", delete=False)
        name = fh.name
        basename = os.path.basename(name)
        testdata = "0123456789" * 1000000
        fh.write(testdata)
        fh.close()
        print "wrote %d bytes to %s" % (len(testdata), name)
        metadata = self.fs.put(open(name, "rb"))
        self.fi.put(basename, metadata["checksum"])
        # copy
        copy_name = "%s_1" % basename
        self.fi.copy(basename, copy_name)
        assert self.fi.get(copy_name) == self.fi.get(basename)
        # delete copy
        self.fi.delete(copy_name)
        try:
            self.fi.get(copy_name)
        except HTTP404:
            pass
        # move
        self.fi.move(basename, copy_name)
        try:
            self.fi.get(basename)
            raise AssertionError("This should except")
        except HTTP404:
            pass
        self.fi.delete(copy_name)
        assert self.fi.isfile(basename) is False
        assert self.fi.isfile(copy_name) is False
        # there should be nothing left in fileindex
        os.unlink(name)


if __name__ == "__main__":
    unittest.main()
    sys.exit(0)
    bs = BlockStorageClient("http://srvlxtest1.tilak.cc/blockstorage")
    fs = FileStorageClient("http://srvlxtest1.tilak.cc/filestorage", bs, BLOCKSIZE)
    fi = FileIndexClient("http://srvlxtest1.tilak.cc/fileindex")

    # FileStorage Tests
    metadata = fs.put(open("/home/mesznera/Downloads/isos/VMware-vcb-64559.exe", "rb"))
    metadata = fs.put_fast(open("/home/mesznera/Downloads/isos/VMware-vcb-64559.exe", "rb"))
    for block in fs.get(metadata["checksum"]):
        print len(block)
    print fs.exists(metadata["checksum"])
    print fs.view(metadata["checksum"])
    fs.delete(metadata["checksum"])

    # FileIndex Tests
    fi.put("testfile.dmp", metadata["checksum"])    

    # BlockStorage Tests
    fh = open("/home/mesznera/Downloads/isos/VMware-vcb-64559.exe", "rb")
    def read_block():
        return fh.read(BLOCKSIZE)
    print "storing file"
    blockchain = []
    filehash = hashlib.md5()
    filesize = 0
    starttime = time.time()
    dedups = 0
    for data in iter(read_block, ""):
        md5 = hashlib.md5()
        md5.update(data)
        filehash.update(data)
        filesize += len(data)
        md5hash1 = md5.hexdigest()
        #print "put %s %s" % (len(data), md5hash1)
        checksum, status = bs.put(data)
        if status == 202:
            dedups += 1
        assert checksum == md5hash1
        blockchain.append(checksum)
    print "file checksum : %s, length : %s, duration : %s, throughput: %0.2f kB/s" % (filehash.hexdigest(), filesize, time.time() - starttime, filesize/1024/(time.time() - starttime))
    print "already existing blocks : %s of %s" % (dedups, len(blockchain))
    print "getting stored file back"
    for block in blockchain:
        data = bs.get(block)
        md5 = hashlib.md5()
        md5.update(data)
        assert block == md5.hexdigest()
    print "deleting blocks"
    deleted = 0
    for block in blockchain:
        status = bs.delete(block)
        if status == 202:
            deleted += 1
    print "blocks already deleted : %s" % deleted

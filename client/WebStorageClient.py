#ndard.at/!/usr/bin/python

import sys
import os
import json
#import urllib2
#import urllib3
import requests
import hashlib
import time
import tempfile
import base64
import logging
#logging.basicConfig(level=logging.DEBUG)


#pool = urllib3.PoolManager()

CONFIG={}
for line in open("WebStorageClient.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value

class HTTP404(Exception):
    pass

class WebAppClient(object):

    def get_url(self, *args):
        return u"/".join((self.url, "/".join(args)))


class BlockStorageClient(WebAppClient):
    """store chunks of data into blockstorage"""

    def __init__(self, url=None):
        if url is None:
            self.url = CONFIG["URL_BLOCKSTORAGE"]
        else:
            self.url = url

    def put(self, data):
        """put some arbitrary data into storage"""
        res = requests.get(self.get_url("put"), data=data)
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("block existed, but rewritten")
            return res.text, res.status_code
        raise HTTP404("webapplication delivered status %s" % res.status_code)

    def get(self, hexdigest):
        """get data defined by hexdigest from storage"""
        res = requests.get(self.get_url("get", hexdigest))
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % hexdigest)
        return res.content

    def delete(self, hexdigest):
        """delete data defined by hexdigest from storage"""
        res = requests.get(self.get_url("delete", hexdigest))
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % hexdigest)

    def exists(self, hexdigest):
        """check if data defined by hexdigest exists"""
        res = requests.get(self.get_url("exists", hexdigest))
        if res.status_code == 200:
            return True
        return False

    def ls(self):
        """return all availabel data defined by hexdigest as list of hexdigests"""
        res = requests.get(self.get_url("ls"))
        if res.status_code == 200:
            return res.json()
        raise HTTP404("webapplication delivered status %s" % res.status_code)

class FileStorageClient(WebAppClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    """

    def __init__(self, bs, url=None, blocksize=1024*1024):
        if url is None:
            self.url = CONFIG["URL_FILESTORAGE"]
        else:
            self.url = url
        self.bs = bs
        self.blocksize = blocksize

    def put(self, fh):
        """
        save data of fileobject in Blockstorage
        data is chunked in self.blocksize pieces and sent to BlockStorage

        the data is anyway transfered to BlockStorage, no matter if
        this data is already stored
        """
        def read_block():
            return fh.read(self.blocksize)
        metadata = {
            "blockchain" : [],
            "size" : 0,
            "checksum" : None,
        }
        filehash = hashlib.md5()
        for data in iter(read_block, ""):
            filehash.update(data)
            metadata["size"] += len(data)
            checksum, status = self.bs.put(data)
            metadata["blockchain"].append(checksum)
        metadata["checksum"] = filehash.hexdigest()
        res = requests.get(self.get_url("put", metadata["checksum"]), data=json.dumps(metadata))
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("file for this checksum already existed")
            return metadata
        raise HTTP404("webapplication returned status %s" % res.status_code)

    def put_fast(self, fh):
        """
        save data of fileobject in Blockstorage
        data is chunked in self.blocksize pieces and sent to BlockStorage

        if hexdigest of chunk exists in blockstorage, the data is not transfered
        """
        def read_block():
            return fh.read(self.blocksize)
        metadata = {
            "blockchain" : [],
            "size" : 0,
            "checksum" : None,
        }
        filehash = hashlib.md5()
        for data in iter(read_block, ""):
            filehash.update(data)
            metadata["size"] += len(data)
            md5 = hashlib.md5()
            md5.update(data)
            if not self.bs.exists(md5.hexdigest()):
                checksum, status = self.bs.put(data)
                metadata["blockchain"].append(checksum)
            else:
                metadata["blockchain"].append(md5.hexdigest())
        logging.debug("uploaded all blocks")
        metadata["checksum"] = filehash.hexdigest()
        logging.debug("checksum of file: %s", metadata["checksum"])
        logging.debug("storing metadata in FileStorage")
        res = requests.get(self.get_url("put", metadata["checksum"]), data=json.dumps(metadata))
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("file for this checksum already existed")
            return metadata
        raise HTTP404("webapplication returned status %s" % res.status_code)

    def read(self, hexdigest):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almoust all times less than self.blocksize
        """
        res = requests.get(self.get_url("get", hexdigest))
        if res.status_code == 200:
            metadata = res.json()
            for block in metadata["blockchain"]:
                yield self.bs.get(block)
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def delete(self, hexdigest):
        """
        delete blockchain defined by hexdigest
        the unerlying data in BlockStorage will not be deleted
        """
        res = requests.get(self.get_url("delete", hexdigest))
        if res.status_code != 200:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get(self, hexdigest):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        res = requests.get(self.get_url("get", hexdigest))
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def ls(self):
        """
        return list of hexdigests stored in FileStorage
        """
        res = requests.get(self.get_url("ls"))
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def exists(self, hexdigest):
        """
        check if file defined by hexdigest is already stored
        """
        res = requests.get(self.get_url("exists", hexdigest))
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        raise HTTP404("webapplication returned status %s" % res.status_code)


class FileIndexClient(WebAppClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    """

    def __init__(self, fs, url=None):
        if url is None:
            self.url = CONFIG[u"URL_FILEINDEX"]
        else:
            self.url = url
        self.fs = fs


    # here begins main API

    def put(self, filepath, checksum):
        """
        save filename to checksum in FileIndex
        """
        data = {"name" : filepath.encode("utf-8"), "checksum" : checksum}
        res = requests.get(self.get_url("put"), data=json.dumps(data))
        if res.status_code == 201:
            logging.info("File put in store, but already existed")
        elif re.status_code != 200:
            raise HTTP404("got status %d on put" % res.status_code)

    def get(self, filepath):
        """
        return hexdigest of file, named by filepath
        """
        res = requests.get(self.get_url("get"), data=filepath.encode("utf-8"))
        if res.status_code == 404:
            raise HTTP404("File not found")
        checksum = res.json()
        # TODO: remove this workaround
        if checksum.startswith("\""):
            checksum = checksum[1:-1]
        # length of md5 hexdigest is fixed
        assert len(checksum) == len("7233cd2eaf78da883a54bc81513f021f")
        return checksum

    def read(self, filepath):
        """
        return data for file defined by filepath
        """
        checksum = self.get(filepath)
        return self.fs.read(checksum)

    def write(self, fh, filepath):
        """
        write (upload) data read from filehandle to FileStorage/BlockStorage
        and store file hexdigest with name filepath in FileIndex
        """
        metadata = self.fs.put(fh)
        self.put(filepath, metadata[u"checksum"])

    def upload(self, filename, path="/"):
        """
        open local file named by filename and put data to WebStorage,
        afterwards store file in FileIndex
        """
        fh = open(filename, "rb")
        self.write(fh, os.path.join(path, os.path.basename(filename)))
        fh.close()

    def listdir(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("listdir"), data=filepath.encode("utf-8"), headers=headers)
        logging.error(res.encoding)
        # list of utf-8 encoded strings, must decode
        return res.json()

    def delete(self, filepath):
        res = requests.get(self.get_url("delete"), data=filepath.encode("utf-8"))
        if res.status_code != 200:
            raise HTTP404("File %s could not be deleted" % filepath.encode("utf-8"))

    def stats(self, filepath):
        res = requests.get(self.get_url("stats"), data=filepath.encode("utf-8"))
        return res.text

    def exists(self, filepath):
        res = requests.get(self.get_url("exists"), data=filepath.encode("utf-8"))
        if res.status_code == 200:
            return True
        return False

    def isfile(self, filepath):
        res = requests.get(self.get_url("isfile"), data=filepath.encode("utf-8"))
        if res.status_code == 200:
            return True
        return False

    def isdir(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("isdir"), data=filepath.encode("utf-8"), headers=headers)
        if res.status_code == 200:
            return True
        return False

    def mkdir(self, filepath):
        if not self.exists(filepath):
            res = requests.get(self.get_url("mkdir"), data=filepath.encode("utf-8"))
        else:
            logging.error("file or directory %s exists")

    def copy(self, source, target):
        if self.exists(source):
            checksum = self.get(source)
            self.put(target, checksum)
        else:
            logging.error("file or directory does not %s exist")

    def move(self, source, target):
        self.copy(source, target)
        self.delete(source)


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

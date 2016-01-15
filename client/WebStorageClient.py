#ndard.at/!/usr/bin/python

import sys
import os
import json
import urllib2
import urllib3
import hashlib
import time
import tempfile
import base64
import logging
#logging.basicConfig(level=logging.DEBUG)


pool = urllib3.PoolManager()

CONFIG={}
for line in open("WebStorageClient.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value

class HTTP404(Exception):
    pass

class WebAppClient(object):

    def _call_url(self, method="GET", data=None, params=()):
        logging.debug("called %s with params %s", method, str(params))
        logging.debug("calling url: %s", self.url)
        url = u"/".join((self.url, u"/".join(params)))
        #logging.info("calling %s %s", method, url)
        try:
            #req = urllib2.Request(url,
            #    data,
            #    {'Content-Type': 'application/octet-stream'})
            #req.get_method = lambda: method
            #res = urllib2.urlopen(req)
            headers = {'Content-Type': 'application/octet-stream'}
            res = pool.urlopen(method, url, headers=headers, body=data)
            return res
        except urllib2.HTTPError as exc:
            if exc.code == 404:
                raise HTTP404()
            logging.exception(exc)
            logging.error("error calling %s %s", method, url)
        except urllib2.URLError as exc:
            logging.exception(exc)
            logging.error("error calling %s %s", method, url)

    def urlcall(self, method, urlparams=None, data=None):
        params = u""
        if urlparams is not None:
            if not (isinstance(urlparams, list) or isinstance(urlparams, tuple)):
                params = (method.lower(), urlparams)
            else:
                params = [method.lower(), ] + list(urlparams)
        else:
            params = [method.lower(), ]
        assert isinstance(method, basestring)
        return self._call_url(u"GET", data=data, params=params)

    @staticmethod
    def uri_encode(text):
        return base64.urlsafe_b64encode(text.encode("utf-8"))

    @staticmethod
    def uri_decode(text):
        return base64.urlsafe_b64decode(text.decode("utf-8"))


class BlockStorageClient(WebAppClient):
    """store chunks of data into blockstorage"""

    def __init__(self, url=None):
        if url is None:
            self.url = CONFIG["URL_BLOCKSTORAGE"]
        else:
            self.url = url

    def put(self, data):
        res = self.urlcall("put", urlparams=(), data=data)
        if res.status in (200, 201):
            if res.status == 201:
                logging.info("block existed, but rewritten")
            return json.loads(res.data), res.status
        raise HTTP404("webapplication delivered status %s" % res.status)

    def get(self, hexdigest):
        res = self.urlcall("get", urlparams=(hexdigest, ))
        if res.status == 404:
            raise HTTP404("block with checksum %s does not exist" % hexdigest)
        return res.data

    def delete(self, hexdigest):
        res = self.urlcall("delete", urlparams=(hexdigest, ))
        if res.status == 404:
            raise HTTP404("block with checksum %s does not exist" % hexdigest)

    def exists(self, hexdigest):
        res = self.urlcall("exists", urlparams=(hexdigest, ))
        if res.status == 200:
            return True
        return False

    def ls(self):
        res = self.urlcall("ls", urlparams=(), data=None)
        if res.status == 200:
            return json.loads(res.data)
        raise HTTP404("webapplication delivered status %s" % res.status)

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
        """save data of fileobject in Blockstorage"""
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
        res = self.urlcall("put", data=json.dumps(metadata), urlparams=(metadata["checksum"], ))
        if res.status in (200, 201):
            if res.status == 201:
                logging.info("file for this checksum already existed")
            return metadata
        raise HTTP404("webapplication returned status %s" % res.status)

    def put_fast(self, fh):
        """save data of fileobject in Blockstorage"""
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
        res = self.urlcall("put", data=json.dumps(metadata), urlparams=(metadata["checksum"], ))
        if res.status in (200, 201):
            if res.status == 201:
                logging.info("file for this checksum already existed")
            return metadata
        raise HTTP404("webapplication returned status %s" % res.status)
            
    def read(self, hexdigest):
        res = self.urlcall("get", urlparams=(hexdigest,))
        if res.status == 200:
            metadata = json.loads(res.data)
            for block in metadata["blockchain"]:
                yield self.bs.get(block)
        else:
            raise HTTP404("webapplication returned status %s" % res.status)

    def delete(self, hexdigest):
        res = self.urlcall("delete", urlparams=(hexdigest,))
        if res.status != 200:
            raise HTTP404("webapplication returned status %s" % res.status)

    def get(self, hexdigest):
        res = self.urlcall("get", urlparams=(hexdigest,))
        if res.status == 200:
            return json.loads(res.data)
        else:
            raise HTTP404("webapplication returned status %s" % res.status)

    def ls(self):
        res = self.urlcall("ls", urlparams=())
        if res.status == 200:
            return json.loads(res.data)
        else:
            raise HTTP404("webapplication returned status %s" % res.status)

    def exists(self, hexdigest):
        res = self.urlcall("exists", urlparams=(hexdigest,))
        if res.status == 200:
            return True
        if res.status == 404:
            return False
        raise HTTP404("webapplication returned status %s" % res.status)


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

    def _call_url(self, method, url, headers, data=None, params=None):
        res = pool.urlopen(method, url, headers=headers, body=data)
        if res.status == 404:
            raise HTTP404("HTTP 404 returned")
        return res

    def get_json(self,method, data):
        headers = {'Content-Type': 'application/json'}
        res =  self.get_raw(method, json.dumps(unicode(data).encode("utf-8")), headers)
        return json.loads(res.data)

    def get_raw(self, method, data, headers={'Content-Type': 'application/json'}):
        url = u"/".join((self.url, method))
        res = self._call_url("GET", url, headers=headers, data=data)
        return res

    # here begins main API

    def put(self, filepath, checksum):
        """save filename to checksum in FileIndex"""
        data = {"name" : filepath, "checksum" : checksum}
        res = self.get_raw(u"put", json.dumps(data))
        return res # todo any reason to do this?

    def get(self, filepath):
        checksum = self.get_json(u"get", filepath)
        # TODO: remove this workaround
        if checksum.startswith("\""):
            checksum = checksum[1:-1]
        # length of md5 hexdigest is fixed
        assert len(checksum) == len("7233cd2eaf78da883a54bc81513f021f")
        return checksum

    def read(self, filepath):
        checksum = self.get(filepath)
        return self.fs.read(checksum)

    def write(self, fh, filepath):
        metadata = self.fs.put(fh)
        self.put(filepath, metadata[u"checksum"])

    def listdir(self, filepath):
        data = self.get_json(u"listdir", filepath)
        return data

    def delete(self, filepath):
        res = self.get_raw(u"delete", json.dumps(unicode(filepath).encode("utf-8")))
        if res.status == 200:
            return True
        raise StandardError("File %s could not be deleted" % filepath)

    def stats(self, filepath):
        res = self.get_json(u"stats", filepath)
        return res

    def exists(self, filepath):
        try:
            res = self.get_raw(u"exists", json.dumps(unicode(filepath).encode("utf-8")))
            if res.status == 200:
                return True
            return False
        except HTTP404:
            return False

    def isfile(self, filepath):
        try:
            res = self.get_raw(u"isfile", json.dumps(unicode(filepath).encode("utf-8")))
            if res.status == 200:
                return True
            return False
        except HTTP404:
            return False # mimic os.path.isdir behaviour

    def isdir(self, filepath):
        try:
            res = self.get_raw(u"isdir", json.dumps(unicode(filepath).encode("utf-8")))
            if res.status == 200:
                return True
            return False
        except HTTP404:
            return False # mimic os.path.isdir behaviour

    def walk(self, filepath):
        assert self.isdir(filepath)
        result = []
        for item in self.listdir(filepath):
            if self.isfile(item):
                result.append(item)
            else:
                result += self.walk(item)
        return result

    def mkdir(self, filepath):
        if not self.exists(filepath):
            res = self.get_raw(u"mkdir", json.dumps(unicode(filepath).encode("utf-8"))) # any reason to do this?
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

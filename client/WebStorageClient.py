#ndard.at/!/usr/bin/python

import sys
import json
import urllib2
import hashlib
import time
import logging
logging.basicConfig(level=logging.DEBUG)


CONFIG={}
for line in open("WebStorageClient.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value

class HTTP404(Exception):
    pass

class WebAppClient(object):

    def _call_url(self, method="GET", data=None, params=()):
        url = "/".join((self.url, "/".join(params)))
        #logging.info("calling %s %s", method, url)
        try:
            req = urllib2.Request(url, 
                data,
                {'Content-Type': 'application/octet-stream'})
            req.get_method = lambda: method
            res = urllib2.urlopen(req)
            return res
        except urllib2.HTTPError as exc:
            if exc.code == 404:
                raise HTTP404()
            logging.exception(exc)
            logging.error("error calling %s %s", method, url)
        except urllib2.URLError as exc:
            logging.exception(exc)
            logging.error("error calling %s %s", method, url)


class BlockStorageClient(WebAppClient):
    """store chunks of data into blockstorage"""

    def __init__(self, url):
        if url is None:
            self.url = CONFIG["URL_BLOCKSTORAGE"]
        else:
            self.url = url

    def put(self, data):
        res = self._call_url("PUT", data=data)
        return json.loads(res.read()), res.code
            
    def get(self, hexdigest):
        res = self._call_url("GET", params=(hexdigest,))
        return res.read()

    def delete(self, hexdigest):
        res = self._call_url("DELETE", params=(hexdigest,))
        return res.code

    def exists(self, hexdigest):
        res = self._call_url("EXISTS", params=(hexdigest,))
        if res.code == 200:
            return True
        return False


class FileStorageClient(WebAppClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    """

    def __init__(self, url, bs, blocksize=1024*1024):
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
        self._call_url("PUT", data=json.dumps(metadata), params=(metadata["checksum"], ))
        return metadata
 
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
        metadata["checksum"] = filehash.hexdigest()
        self._call_url("PUT", data=json.dumps(metadata), params=(metadata["checksum"], ))
        return metadata
            
    def get(self, hexdigest):
        metadata = json.loads(self._call_url("GET", params=(hexdigest,)).read())
        for block in metadata["blockchain"]:
            yield self.bs.get(block)

    def delete(self, hexdigest):
        res = self._call_url("DELETE", params=(hexdigest,))
        return res.code

    def view(self, hexdigest):
        res = self._call_url("GET", params=(hexdigest,))
        return json.loads(res.read())

    def exists(self, hexdigest):
        res = self._call_url("EXISTS", params=(hexdigest,))
        return res.code

class FileIndexClient(WebAppClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    """

    def __init__(self, url):
        if url is None:
            self.url = CONFIG["URL_FILEINDEX"]
        else:
            self.url = url

    def put(self, filepath, checksum):
        """save filename to checksum in FileIndex"""
        return self._call_url("PUT", data=json.dumps(checksum), params=filepath.split("/"))
            
    def get(self, filepath):
        return self._call_url("GET", params=filepath.split("/"))

    def delete(self, filepath):
        return self._call_url("DELETE", params=filepath.split("/"))

    def exists(self, filepath):
        try:
            res = self._call_url("EXISTS", params=filepath.split("/"))
            if res.code == 200:
                return True
            return False
        except HTTP404:
            return False

    def isfile(self, filepath):
        try:
            res = self._call_url("EXISTS", params=filepath.split("/"))
            if res.code == 200:
                return True
            return False
        except HTTP404:
            return False

    def isdir(self, filepath):
        try:
            res = self._call_url("EXISTS", params=filepath.split("/"))
            if res.code == 201:
                return True
            return False
        except HTTP404:
            return False

    def mkdir(self, filepath):
        if not self.exists(filepath):
            res = self._call_url("PUT", params=filepath.split("/"))
        else:
            logging.error("file or directory %s exists")


if __name__ == "__main__":
    BLOCKSIZE = 1024 * 1024
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

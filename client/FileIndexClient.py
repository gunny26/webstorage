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

CONFIG={}
for line in open("WebStorageClient.ini", "rb"):
    key, value = line.strip().split("=")
    CONFIG[key] = value

class HTTPError(Exception):
    pass

class HTTP404(Exception):
    pass

class WebAppClient(object):

    def get_url(self, arg=None):
        if arg is None:
            return self.url + "/"
        url = u"%s/%s" % (self.url, arg)
        return url


class BlockStorageClient(WebAppClient):
    """store chunks of data into blockstorage"""

    def __init__(self, url=None):
        if url is None:
            self.url = CONFIG["URL_BLOCKSTORAGE"]
        else:
            self.url = url
        logging.debug("URL: %s", self.url)

    def put(self, data):
        """put some arbitrary data into storage"""
        digest = hashlib.sha1()
        digest.update(data)
        checksum = digest.hexdigest()
        url = self.get_url(checksum)
        logging.debug("PUT %s", url)
        res = requests.put(self.get_url(checksum), data=data)
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("block existed, but rewritten")
            assert res.text == checksum
            return res.text, res.status_code
        raise HTTPError("call to %s delivered status %s" % (self.get_url(), res.status_code))

    def get(self, checksum):
        """get data defined by hexdigest from storage"""
        url = self.get_url(checksum)
        logging.debug("GET %s", url)
        res = requests.get(self.get_url(checksum))
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)
        return res.content

    def delete(self, checksum):
        """delete data defined by hexdigest from storage"""
        url = self.get_url(checksum)
        logging.debug("DELETE %s", url)
        res = requests.delete(url)
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)

    def exists(self, checksum):
        """check if data defined by hexdigest exists"""
        url = self.get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = requests.options(url)
        if res.status_code == 200:
            return True
        return False

    def ls(self):
        """return all availabel data defined by hexdigest as list of hexdigests"""
        url = self.get_url()
        logging.debug("GET %s", url)
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()
        raise HTTP404("webapplication delivered status %s" % res.status_code)


class FileStorageClient(WebAppClient):
    """
    put some arbitrary file like data object into BlockStorage and remember how to reassemble it
    """

    def __init__(self):
        self.url = CONFIG["URL_FILESTORAGE"]
        logging.debug("URL: %s", self.url)
        self.bs = BlockStorage()
        self.blocksize = CONFIG["BLOCKSIZE"]
        logging.debug("BLOCKSIZE: %s", self.blocksize)

    def put(self, fh, mime_type="text/html"):
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
            "mime_type" : mime_type
        }
        filehash = hashlib.sha1()
        for data in iter(read_block, ""):
            filehash.update(data)
            metadata["size"] += len(data)
            checksum, status = self.bs.put(data)
            metadata["blockchain"].append(checksum)
        metadata["checksum"] = filehash.hexdigest()
        res = requests.put(self.get_url(metadata["checksum"]), data=json.dumps(metadata))
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.info("file for this checksum already existed")
            return metadata
        raise HTTP404("webapplication returned status %s" % res.status_code)

    def put_fast(self, fh, mime_type="octet/stream"):
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
            "mime_type" : mime_type
        }
        filehash = hashlib.sha1()
        blockcount = 0
        existscount = 0
        for data in iter(read_block, ""):
            filehash.update(data)
            metadata["size"] += len(data)
            md5 = hashlib.sha1()
            md5.update(data)
            blockcount += 1
            if not self.bs.exists(md5.hexdigest()):
                checksum, status = self.bs.put(data)
                logging.debug("checksum: %s, status: %s", checksum, status)
                metadata["blockchain"].append(checksum)
            else:
                metadata["blockchain"].append(md5.hexdigest())
                existscount += 1
        logging.debug("put %d blocks in BlockStorage, %d existed already", blockcount, existscount)
        metadata["checksum"] = filehash.hexdigest()
        logging.debug("storing recipe for filechecksum: %s", metadata["checksum"])
        res = requests.put(self.get_url(metadata["checksum"]), data=json.dumps(metadata))
        if res.status_code in (200, 201):
            if res.status_code == 201:
                logging.debug("recipe for checksum %s exists already", metadata["checksum"])
            return metadata
        raise HTTPError("webapplication returned status %s" % res.status_code)

    def read(self, checksum):
        """
        return data as generator
        yields data blocks of self.blocksize
        the last block is almoust all times less than self.blocksize
        """
        url = self.get_url(checksum)
        logging.debug("GET %s", url)
        res = requests.get(url)
        if res.status_code == 200:
            metadata = res.json()
            for block in metadata["blockchain"]:
                yield self.bs.get(block)
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def delete(self, checksum):
        """
        delete blockchain defined by hexdigest
        the unerlying data in BlockStorage will not be deleted
        """
        url = self.get_url(checksum)
        logging.debug("DELETE %s", url)
        res = requests.delete(url)
        if res.status_code != 200:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        url = self.get_url(checksum)
        logging.debug("GET %s", url)
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def ls(self):
        """
        return list of hexdigests stored in FileStorage
        """
        url = self.get_url()
        logging.debug("GET %s", url)
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()
        else:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def exists(self, checksum):
        """
        check if file defined by hexdigest is already stored
        """
        url = self.get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = requests.options(url)
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
        headers = {'Content-Type' : 'appliaction/json; charset=utf-8'}
        data = {"name" : filepath.encode("utf-8"), "checksum" : checksum}
        res = requests.get(self.get_url("put"), data=json.dumps(data), headers=headers)
        if res.status_code == 201:
            logging.info("File put in store, but already existed")
        elif re.status_code != 200:
            raise HTTP404("got status %d on put" % res.status_code)

    def get(self, filepath):
        """
        return hexdigest of file, named by filepath
        """
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
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
        # list of utf-8 encoded strings, must decode
        return res.json()

    def delete(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("delete"), data=filepath.encode("utf-8"), headers=headers)
        if res.status_code != 200:
            raise HTTP404("File %s could not be deleted" % filepath.encode("utf-8"))

    def stats(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("stats"), data=filepath.encode("utf-8"), headers=headers)
        return res.text

    def exists(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("exists"), data=filepath.encode("utf-8"), headers=headers)
        if res.status_code == 200:
            return True
        return False

    def isfile(self, filepath):
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        res = requests.get(self.get_url("isfile"), data=filepath.encode("utf-8"), headers=headers)
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
        headers = {'Content-Type' : 'text/html; charset=utf-8'}
        if not self.exists(filepath):
            res = requests.get(self.get_url("mkdir"), data=filepath.encode("utf-8"), headers=headers)
        else:
            logging.error("file or directory %s exists", filepath)

    def copy(self, source, target):
        if self.exists(source):
            checksum = self.get(source)
            self.put(target, checksum)
        else:
            logging.error("file or directory does not %s exist")

    def move(self, source, target):
        self.copy(source, target)
        self.delete(source)

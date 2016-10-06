#!/usr/bin/python3
import json
import hashlib
import logging
import requests

CONFIG={}
for line in open("WebStorageClient.ini", "r"):
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
        url = "%s/%s" % (self.url, arg)
        return url


class BlockStorageClient(WebAppClient):
    """store chunks of data into blockstorage"""

    def __init__(self, url=None):
        if url is None:
            self.url = CONFIG["URL_BLOCKSTORAGE"]
        else:
            self.url = url
        logging.debug("URL: %s", self.url)
        self.session = requests.Session()

    def put(self, data):
        """put some arbitrary data into storage"""
        assert len(data) <= int(CONFIG["BLOCKSIZE"])
        digest = hashlib.sha1()
        digest.update(data)
        checksum = digest.hexdigest()
        url = self.get_url(checksum)
        logging.debug("PUT %s", url)
        res = self.session.put(self.get_url(checksum), data=data)
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
        res = self.session.get(self.get_url(checksum))
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)
        return res.content

    def delete(self, checksum):
        """delete data defined by hexdigest from storage"""
        url = self.get_url(checksum)
        logging.debug("DELETE %s", url)
        res = self.session.delete(url)
        if res.status_code == 404:
            raise HTTP404("block with checksum %s does not exist" % checksum)

    def exists(self, checksum):
        """check if data defined by hexdigest exists"""
        url = self.get_url(checksum)
        logging.debug("OPTIONS %s", url)
        res = self.session.options(url)
        if res.status_code == 200:
            return True
        return False

    def ls(self):
        """return all availabel data defined by hexdigest as list of hexdigests"""
        url = self.get_url()
        logging.debug("GET %s", url)
        res = self.session.get(url)
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
        self.bs = BlockStorageClient()
        self.blocksize = int(CONFIG["BLOCKSIZE"])
        logging.debug("BLOCKSIZE: %s", self.blocksize)
        self.session = requests.Session()

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
        res = self.session.put(self.get_url(metadata["checksum"]), data=json.dumps(metadata))
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
        data = fh.read(self.blocksize)
        while data:
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
            data = fh.read(self.blocksize)
        logging.debug("put %d blocks in BlockStorage, %d existed already", blockcount, existscount)
        metadata["checksum"] = filehash.hexdigest()
        logging.debug("storing recipe for filechecksum: %s", metadata["checksum"])
        res = self.session.put(self.get_url(metadata["checksum"]), data=json.dumps(metadata))
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
        res = self.session.get(url)
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
        res = self.session.delete(url)
        if res.status_code != 200:
            raise HTTP404("webapplication returned status %s" % res.status_code)

    def get(self, checksum):
        """
        returns blockchain of file defined by hexdigest

        this is not the data of this file, only the plan how to assemble the file
        """
        url = self.get_url(checksum)
        logging.debug("GET %s", url)
        res = self.session.get(url)
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
        res = self.session.get(url)
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
        res = self.session.options(url)
        if res.status_code == 200:
            return True
        if res.status_code == 404:
            return False
        raise HTTP404("webapplication returned status %s" % res.status_code)

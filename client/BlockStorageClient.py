#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use BlockStorage WebApps
"""
import os
import array
import logging
# own modules
from webstorageClient.ClientConfig import ClientConfig
from webstorageClient.WebStorageClient import WebStorageClient

class BlockStorageError(Exception):
    pass

class BlockStorageClient(WebStorageClient):
    """stores chunks of data into BlockStorage"""

    def __init__(self, url=None, cache=True):
        """__init__"""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client_config = ClientConfig()
        if url is None:
            self._url = self._client_config.blockstorage_url
        else:
            self._url = url
        super().__init__()
        # get info from backend
        self._cache = cache # cache blockdigests or not
        self._info = self._get_json("info")
        # search for cachefiles, and load local data
        self._checksums = None
        cachefile, cache_epoch = self._choose_cachefile(self._client_config.homepath, self._info["id"], self._info["blockchain_epoch"])
        if cache_epoch == 2: # fresh file
            self._dump_checksums(cachefile, 2)
        elif cache_epoch < self._info["blockchain_epoch"]:
            self._logger.info("TODO: update local cache, fallback get whole data")
            self._dump_checksums(cachefile, 2)
        # load stored data
        self._checksums = self._load_checksums(cachefile)

    @property
    def blocksize(self):
        return int(self._info["blocksize"])

    @property
    def info(self):
        return self._info

    @property
    def checksums(self):
        if (self._cache == True) and (not self._checksums):
            self._logger.info("getting existing checksums")
            self._checksums = self._get_json()
        return self._checksums

    def get_info(self):
        """
        get some information about backend
        """
        self._info = self._get_json("info")
        return self._info

    def get_epoch(self, epoch):
        """
        return blockchain data at epoch
        """
        return self._get_json("epoch/%d" % epoch)

    def get_journal(self, epoch):
        """
        return blockchain journal starting at epoch
        """
        return self._get_json("journal/%d" % epoch)

    def get_checksums(self, epoch, filename):
        """
        write binary blob of checksums to some file
        """
        res = self._get_chunked("checksums/%d" % epoch)
        self._logger.info("writing %s", filename)
        with open(filename, "wb") as outfile:
            for chunk in res:
                outfile.write(chunk)

    def put(self, data, use_cache=False):
        """put some arbitrary data into storage"""
        if len(data) > self.blocksize: # assure maximum length
            raise BlockStorageError("length of providede data (%s) is above maximum blocksize of %s" % (len(data), self.blocksize))
        checksum = self._blockdigest(data)
        if use_cache and checksum in self.checksums:
            self._logger.debug("202 - skip this block, checksum is in list of cached checksums")
            return checksum, 202
        else:
            res = self._put(checksum, data=data)
            if res.status_code == 201:
                self._logger.debug("201 - block rewritten")
            if res.text != checksum:
                raise BlockStorageError("checksum mismatch, sent %s to save, but got %s from backend" % (checksum, res.text))
            self._checksums.append(checksum) # add to local cache
            return res.text, res.status_code

    def get(self, checksum, verify=False):
        """
        get data defined by hexdigest from storage
        if verify - recheck checksum locally
        """
        res = self._get(checksum)
        data = res.content
        if verify:
            if checksum != self._blockdigest(data):
                raise BlockStorageError("Checksum mismatch %s requested, %s get" % (checksum, self._blockdigest(data)))
        return data

    def get_verify(self, checksum):
        return self.get(checksum, True)

    def exists(self, checksum):
        """
        exists method if caching is on
        if the searched checksum is not available, the filestorage backend is queried
        """
        if checksum in self._checksums: # check local cache first
            return True
        return self._exists(checksum)

    def meta(self):
        """
        receive meta information of any checksum stored in bockstorage
        """
        res = self._get_chunked("meta")
        for chunk in res.iter_lines(decode_unicode=True):
            yield json.loads(chunk)

    def verify_bockchain(self):
        """
        verify blockchain providede by backend

        returns: True if blockchain is valid
        """
        # epoch counting starts at 1
        # epoch 1 has no checksum only seed
        # epoch 2 is the first full entry
        epoch = 1
        print("seed                         : ", info["blockchain_seed"])
        last_sha256 = info["blockchain_seed"]
        sha256 = hashlib.sha256()
        for index, checksum in enumerate(bsc.checksums):
            epoch = index + 2 # forst real epoch is 2
            sha256 = hashlib.sha256()
            # use epoch of last sha256 key + last sha256 key + actual checksum
            sha256.update(str(epoch-1).encode("ascii") + last_sha256.encode("ascii") + checksum.encode("ascii"))
            last_sha256 = sha256.hexdigest()
        print("calculated until epoch       : ", epoch)
        print("latest checksum              : ", checksum)
        print("resulting blockchain_checksum: ", last_sha256)
        print("blockchain_checksum          : ", info["blockchain_checksum"])
        if last_sha256 == info["blockchain_checksum"]:
            print("blockchain is valid")
            return True
        print("blockchain is invalid")
        return False

##################### private section #####################################

    def _dump_checksums(self, cachefile, epoch=2):
        """
        write binary blob of checksums to cachefile

        to get whole checksums from backend use
            epoch=2
        returns: None
        """
        res = self._get_chunked("checksums/%d" % epoch)
        self._logger.info("writing %s", cachefile)
        mode = "wb"
        if epoch != 2:
            mode = "ab" # append if epoch higher than 2
        with open(cachefile, mode) as outfile:
            for chunk in res:
                outfile.write(chunk)

    def _load_checksums(self, cachefile):
        """
        loading list of checksums from locally stored binary blob

        using: cachefile
        returning: checksums
        """
        checksums = []
        self._logger.info("using cachefile %s", cachefile)
        data = array.array("B")
        with open(cachefile, "rb") as infile:
            data.fromfile(infile, os.stat(cachefile).st_size)
            for index in range(0, len(data), 20):
                checksum = "".join(["%02x" % item for item in data[index:index+20]])
                checksums.append(checksum)
        self._logger.info("loaded %d checksum from cache", len(checksums))
        return checksums

    def _choose_cachefile(self, directory, backend_id, backend_epoch):
        """
        try to find the best cachefile available
        using: self._client_config, self._info
        modfying: self._cachefile
        returning: cachefile, cachefile_epoch
        """
        cache_file = None
        cache_epoch = 2 # the lowest possible
        cache_file = "%s.bin" % backend_id
        absfilename = os.path.join(directory, cache_file)
        self._logger.info("absfilename: %s", absfilename)
        if os.path.isfile(absfilename):
            cache_epoch = int(os.stat(absfilename).st_size / 20) + 1
            self._logger.info("cache_epoch: %s", cache_epoch)
            leftover = os.stat(absfilename).st_size % 20
            self._logger.info("leftover: %s", leftover)
            if leftover != 0:
                self._logger.error("cachefile %s is corrupted, deleting file", cache_file)
                os.unlink(absfilename)
                cache_epoch = 2
            else:
                self._logger.info("found checksum cache file until epoch %d", cache_epoch)
                if cache_epoch > backend_epoch: # something wrong
                    self._logger.error("cachefile %s epoch is higher than backend epoch, deleting file", cache_file)
                    os.unlink(absfilename)
                    cache_epoch = 2
        return absfilename, cache_epoch


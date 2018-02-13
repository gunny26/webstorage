#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import os
import hashlib
import sys
import socket
import argparse
import json
import gzip
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA256
# own modules
from webstorage import WebStorageArchive as WebStorageArchive

def get_signature(data, private_key_filename):
    """
    returns string of hex signature
    data some sort of string
    private_key_filename ... path to private key
    """
    key = open(os.path.expanduser(private_key_filename), "rb").read()
    rsakey = RSA.importKey(key)
    signer = PKCS1_v1_5.new(rsakey)
    digest = SHA256.new()
    digest.update(data)
    sign = signer.sign(digest)
    return sign.hex()

def save_webstorage_archive(data, filename, private_key):
    """
    add duration, checksum and signature to data,
    afterwards store in WebStorageArchive
    """
    # build sah256 checksum
    sha256 = hashlib.sha256()
    sha256.update(json.dumps(data, sort_keys=True).encode("utf-8"))
    data["checksum"] = sha256.hexdigest()
    logging.info("checksum of archive %s", data["checksum"])
    # sorting keys is essential to ensure same signature
    data["signature"] = get_signature(json.dumps(data, sort_keys=True).encode("utf-8"), private_key)
    logging.info("%(totalcount)d files of %(totalsize)s bytes size", data)
    # store
    wsa = WebStorageArchive()
    wsa.put(data, filename)

def main():
    """
    get options, then call specific functions
    """
    parser = argparse.ArgumentParser(description='to upload some locally stored wstar.gz file')
    parser.add_argument("-f", "--file", help="local file to read from")
    parser.add_argument('--public-key', default="~/.webstorage/public.der", help="path to public key file in DER format")
    parser.add_argument('--private-key', default="~/.webstorage/private.der", help="path to private key file in DER format")
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR")
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG")
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    if not os.path.isfile(args.file):
        logging.error("file %s does not exist", args.file)
        sys.exit(1)
    data = json.loads(gzip.open(args.file, "r").read())
    filename = os.path.basename(args.file)
    logging.info("uploading %s", filename)
    save_webstorage_archive(data, filename, args.private_key)

if __name__ == "__main__":
    main()

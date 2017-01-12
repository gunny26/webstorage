#!/usr/bin/python3
"""
Module to work with Webstorage backends
"""
from webstorage.BlockStorageClient import BlockStorageClient as BlockStorageClient
from webstorage.FileStorageClient import FileStorageClient as FileStorageClient
#from webstorage.WebStorageArchiveS3 import WebStorageArchiveS3 as WebStorageArchiveS3
from webstorage.WebStorageArchive import WebStorageArchive as WebStorageArchive
from webstorage.WebStorageClient import HTTP404 as HTTP404
from webstorage.WebStorageClient import HTTPError as HTTPError

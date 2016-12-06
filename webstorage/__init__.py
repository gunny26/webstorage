#!/usr/bin/python3
"""
Module to work with Webstorage backends
"""
from webstorage.WebStorageClient import BlockStorageClient as BlockStorageClient
from webstorage.WebStorageClient import FileStorageClient as FileStorageClient
from webstorage.WebStorageClient import WebStorageArchiveS3 as WebStorageArchiveS3
from webstorage.WebStorageClient import WebStorageArchive as WebStorageArchive
from webstorage.WebStorageClient import HTTP404 as HTTP404
from webstorage.WebStorageClient import HTTPError as HTTPError

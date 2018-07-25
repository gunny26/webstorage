#!/usr/bin/python3
# pylint: disable=line-too-long
"""
RestFUL Webclient to use FileStorage and BlockStorage WebApps
"""
import os
import sys
import re
import json
import logging
import boto3


CONFIG = {}
HOMEPATH = os.path.expanduser("~/.webstorage")
if not os.path.isdir(HOMEPATH):
    print("first create INI file in directory {}".format(HOMEPATH))
    sys.exit(1)
else:
    for line in open(os.path.join(HOMEPATH, "WebStorageClient.ini"), "r"):
        key, value = line.strip().split("=")
        CONFIG[key] = value


class HTTPError(Exception):
    """indicates general exception"""
    pass


class HTTP404(Exception):
    """indicates not found"""
    pass



class WebStorageArchiveS3(object):
    """
    store and retrieve S3 Data, specific for WebStorageArchives
    """

    def __init__(self):
        """
        bucket <str> S3 bucket name
        path <str> Path
        """
        self.bucket = CONFIG["S3_BUCKET"]
        # path will be without trailing slash
        if CONFIG["S3_PATH"][-1] == "/":
            self.path = CONFIG["S3_PATH"][:-1]
        else:
            self.path = CONFIG["S3_PATH"]

    def get_backupsets(self, hostname):
        """
        return data of available backupsets for this specific hostname
        """
        logging.info("searching for wstar archives in bucket %s path %s", self.bucket, self.path)
        result = {}
        rex = re.compile(r"^(.+)_(.+)_(.+)\.wstar\.gz$")
        s3client = boto3.client("s3")
        things = s3client.list_objects(Bucket=self.bucket)
        if "Contents" in things:
            for entry in things["Contents"]:
                if entry["Key"].startswith(self.path):
                    basename = entry["Key"][len(self.path) + 1:]
                    size = entry["Size"]
                    match = rex.match(basename)
                    if match is not None:
                        thishostname = match.group(1)
                        tag = match.group(2)
                        timestamp = match.group(3)
                        # 2016-10-25T20:23:17.782902
                        thisdate, thistime = timestamp.split("T")
                        thistime = thistime.split(".")[0]
                        if hostname == thishostname:
                            result[entry["Key"]] = {
                                "date": thisdate,
                                "time" : thistime,
                                "size" : size,
                                "tag" : tag,
                                "basename" : basename
                            }
        return result

    def get_latest_backupset(self, hostname):
        """
        get the latest backupset stored on s3

        hostname <str>
        """
        backupsets = self.get_backupsets(hostname)
        latest = sorted(backupsets.keys())[-1]
        filename = backupsets[latest]["basename"]
        logging.info("latest backupset found %s", filename)
        return filename

    def get(self, filename):
        """
        get wstar archive data from s3, returned data will bi dict

        key <str> Key of existing S3 object
        """
        s3client = boto3.client("s3")
        key = "/".join((self.path, filename))
        logging.info("getting data forbackupset %s", key)
        res = s3client.get_object(Bucket=self.bucket, Key=key)
        # TODO is this the only and best way, i'm not sure
        json_str = res["Body"].read().decode("utf-8")
        return json.loads(json_str)

    def put(self, data, filename):
        """
        store data to filename, S3 key will be auto generated in conjunction with path
        """
        s3client = boto3.client("s3")
        key = "/".join((self.path, filename))
        logging.info("save data to bucket %s key %s", self.bucket, key)
        res = s3client.put_object(Body=json.dumps(data), Bucket=self.bucket, Key=key)
        return res



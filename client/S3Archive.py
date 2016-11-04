#!/usr/bin/python3
import boto3
import pprint
import re
import socket
import gzip
import json
import tempfile
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO
    from io import BytesIO

def get_s3_backupsets(myhostname, bucket, path, mytag="backup"):
    """
    return data of available backupsets on this specific s3 location
    """
    result = {}
    rex = re.compile("^(.+)_(.+)_(.+)\.wstar\.gz$")
    s3 = boto3.client("s3")
    things = s3.list_objects(Bucket=bucket)
    if "Contents" in things:
        for entry in things["Contents"]:
            if entry["Key"].startswith(path):
                basename = entry["Key"][len(path) + 1:]
                size = entry["Size"]
                match = rex.match(basename)
                if match is not None:
                    hostname = match.group(1)
                    tag = match.group(2)
                    timestamp = match.group(3)
                    # 2016-10-25T20:23:17.782902
                    thisdate, thistime = timestamp.split("T")
                    thistime = thistime.split(".")[0]
                    if hostname == myhostname and tag == mytag:
                        result[entry["Key"]] = {
                            "date": thisdate,
                            "time" : thistime,
                            "size" : size,
                            "tag" : tag,
                            "basename" : basename
                        }
    return result

def get_s3_latest_backupset(hostname, bucket, path, mytag="backup"):
    """
    get the latest backupset stored on s3

    hostname <str>
    bucket <str>
    path <str>
    mytag <str> defaults to "backup"
    """
    backupsets = get_s3_backupsets(hostname, bucket, path, mytag)
    latest = sorted(backupsets.keys())[-1]
    pprint.pprint(latest)
    return latest

def get_s3_data(bucket, key):
    """
    get wstar archive data from s3, returned data will bi dict

    bucket <str>
    key <str> Key of existing S3 object
    """
    s3 = boto3.client("s3")
    res = s3.get_object(Bucket=bucket, Key=key)
    pprint.pprint(res)
    # TODO is this the only and best way, i'm not sure
    json_str = res["Body"].read().decode("utf-8")
    return json.loads(json_str)

def save_s3(data, filename, s3_bucket, s3_path):
    """
    store data to s3
    """
    s3 = boto3.client("s3")
    # convert data to StringIO to get file like interface
    res = s3.put_object(Body=json.dumps(data), Bucket=s3_bucket, Key="%s/%s" % (s3_path, filename))
    # pprint.pprint(res)

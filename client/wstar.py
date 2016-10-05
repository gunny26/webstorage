#!/usr/bin/python
import glob
import os
import hashlib
import datetime
import time
import json
import StringIO
import gzip
import sys
import socket
import argparse
import pprint
import logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# own modules
from WebStorageClient import FileStorageClient as FileStorageClient

def blacklist_match(blacklist, absfilename):
    """
    return True is any item in absfilename is in blacklist
    """
    for item in blacklist:
        if item in absfilename:
            return True
    return False

def create(path, blacklist, outfile):
    """
    create a new archive of files under path
    filter out filepath which mathes some item in blacklist
    and write file to outfile in FileIndex
    """
    archive_dict = {
        "path" : path,
        "filedata" : {},
        "hashmap" : {},
        "blacklist" : blacklist,
        "starttime" : time.time(),
        "stoptime" : None,
    }
    totalsize = 0
    totalcount = 0
    for root, dirs, files in os.walk(unicode(path)):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(blacklist, absfilename):
                logging.info("%s blacklisted", absfilename)
                continue
            if not os.path.isfile(absfilename):
                # only save regular files
                continue
            try:
                size = os.path.getsize(absfilename)
                stat = os.stat(absfilename)
                filemeta_md5 = hashlib.md5()
                filemeta_md5.update(absfilename.encode("utf-8"))
                filemeta_md5.update(str(stat))
                metadata = fs.put_fast(open(absfilename, "rb"))
                archive_dict["filedata"][absfilename] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                }
                if filemeta_md5.hexdigest() in archive_dict["hashmap"]:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()].append(absfilename)
                else:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()] = [absfilename,]
                logging.info("%s %s", metadata["checksum"], absfilename)
                totalcount += 1
                totalsize += size
            except OSError as exc:
                logging.exception(exc)
            except IOError as exc:
                logging.exception(exc)
    archive_dict["totalcount"] = totalcount
    archive_dict["totalsize"] = totalsize
    archive_dict["stoptime"] = time.time()
    outfile.write(json.dumps(archive_dict))
    logging.info("stored %d files of %s bytes size", totalcount, totalsize)
    duration = archive_dict["stoptime"] - archive_dict["starttime"]
    logging.info("duration %0.2f s, bandwith %0.2f kB/s", duration, totalsize / 1024 / duration)

def diff(data):
    difffiles = []
    # check if some files are missing or have changed
    for absfile, filedata in data["filedata"].items():
        if not os.path.isfile(absfile):
            logging.info("deleted [%s] %s", filedata["checksum"], absfile)
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size  = filedata["stat"]
            stat = os.stat(absfile)
            if  (stat.st_mtime != st_mtime) or \
                (stat.st_atime != st_atime) or \
                (stat.st_ctime != st_ctime) or \
                (stat.st_uid != st_uid) or \
                (stat.st_gid != st_gid) or \
                (stat.st_mode != st_mode) or \
                (stat.st_size != st_size):
                logging.info("changed [%s] %s", filedata["checksum"], absfile)
                difffiles.append(absfile)
    # check for new files on local storage
    for root, dirs, files in os.walk(unicode(data["path"])):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(data["blacklist"], absfilename):
                continue
            if absfilename not in data["filedata"]:
                logging.info("new %s", absfilename)
                difffiles.append(absfilename)
    return difffiles

def check(data):
    """
    check backup archive for consistency
    """
    difffiles = []
    # check if some files are missing or have changed
    filecount = 0
    blockcount = 0
    for absfile, filedata in data["filedata"].items():
        logging.info("checking file with checksum %s", filedata["checksum"])
        metadata = fs.get(filedata["checksum"])
        filecount += 1
        for block in metadata["blockchain"]:
            logging.info("    checking block with checksum %s", block)
            blockcount += 1
    logging.info("all files %d available, %d blocks used", filecount, blockcount)

def restore(backupdata, targetpath):
    """
    check backup archive for consistency
    """
    difffiles = []
    # check if some files are missing or have changed
    filecount = 0
    blockcount = 0
    for absfile, filedata in backupdata["filedata"].items():
        st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size  = filedata["stat"]
        newfilename = absfile.replace(backupdata["path"], targetpath)
        logging.info("restoring %s", newfilename)
        if not os.path.isdir(os.path.dirname(newfilename)):
            logging.info("creating directory %s", os.path.dirname(newfilename))
            os.makedirs(os.path.dirname(newfilename))
        outfile = open(newfilename, "wb")
        for data in fs.read(filedata["checksum"]):
            outfile.write(data)
        outfile.close()
        try:
            os.chmod(newfilename, st_mode)
            os.utime(newfilename, (st_atime, st_mtime))
            os.chown(newfilename, st_uid, st_gid)
        except OSError as exc:
            logging.exception(exc)


if __name__ == "__main__":
    fs = FileStorageClient()
    parser = argparse.ArgumentParser(description='create/manage/restore WebStorage Archives')
    parser.add_argument("-c", '--create', action="store_true", help="create a new archive", required=False) 
    parser.add_argument("-x", '--extract', action="store_true", help="restore content of file", required=False)
    parser.add_argument("-t", '--test', action="store_true", help="shown onventory of archive", required=False)
    parser.add_argument("-d", '--diff', action="store_true", help="show differences between local and given archive") 
    parser.add_argument("-b", '--blacklist', default="blacklist.json", help="blacklist file in JSON Format")
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR", required=False)
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG", required=False)
    parser.add_argument('-f', "--file", help="local output file", required=True)
    parser.add_argument("path", metavar="N", type=str, nargs="+", help="path")
    args = parser.parse_args()
    basedir = "/wstar_%s" % socket.gethostname()
    blacklist = json.load(open(args.blacklist, "r"))
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    if args.create is True:
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        outfile = args.file
        create(args.path[0], blacklist, gzip.open(outfile, "wb"))
    elif args.test is True:
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
        else:
            data = json.loads(gzip.open(args.file, "rb").read())
            check(data)
    elif args.diff is True:
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
        else:
            data = json.loads(gzip.open(args.file, "rb").read())
            pprint.pprint(diff(data))
    elif args.extract is True:
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
        else:
            data = json.loads(gzip.open(args.file, "rb").read())
            pprint.pprint(data)
            restore(data, args.path[0])

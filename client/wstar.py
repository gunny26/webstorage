#!/usr/bin/python3
import os
import hashlib
import datetime
import time
import json
import gzip
import sys
import socket
import argparse
import pprint
import stat
import logging
logging.basicConfig(level=logging.INFO)
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)
# own modules
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import HTTP404 as HTTP404

BLOCKSIZE = 2 ** 20
HASH_MINSIZE = 50 * BLOCKSIZE

def filemode(st_mode):
    """
    convert stat st_mode number to human readable string
    taken from https://stackoverflow.com/questions/17809386/how-to-convert-a-stat-output-to-a-unix-permissions-string
    """
    is_dir = 'd' if stat.S_ISDIR(st_mode) else '-'
    dic = {'7':'rwx', '6' :'rw-', '5' : 'r-x', '4':'r--', '0': '---'}
    perm = str(oct(st_mode)[-3:])
    return is_dir + ''.join(dic.get(x, x) for x in perm)

def sizeof_fmt(num, suffix='B'):
    """
    function to convert numerical size number into human readable number
    taken from https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    """
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

def ppls(absfile, filedata):
    """ 
    pritty print ls
    return long filename format, like ls -al does
    """
    st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
    datestring = datetime.datetime.fromtimestamp(int(st_mtime))
    return "%10s %s %s %10s %19s %s" % (filemode(st_mode), st_uid, st_gid, sizeof_fmt(st_size), datestring, absfile)

def blacklist_match(blacklist, absfilename):
    """
    return True if any item in absfilename is in blacklist
    """
    for item in blacklist:
        if item in absfilename:
            return True
    return False

def get_filechecksum(absfile):
    """
    return checksum of file
    """
    filehash = hashlib.sha1()
    with open(absfile, "rb") as fh:
        data = fh.read(BLOCKSIZE)
        while data:
            filehash.update(data)
            data = fh.read(BLOCKSIZE)
    return filehash.hexdigest()

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
    for root, dirs, files in os.walk(path):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(blacklist, absfilename):
                logging.info("EXCLUDE %s", absfilename)
                continue
            if not os.path.isfile(absfilename):
                # only save regular files
                continue
            try:
                size = os.path.getsize(absfilename)
                stat = os.stat(absfilename)
                filemeta_md5 = hashlib.md5()
                filemeta_md5.update(absfilename.encode("utf-8"))
                filemeta_md5.update(str(stat).encode("utf-8"))
                # if size is below HASH_MINSIZE, calculate file checksum,
                # then check if this file is already stored
                metadata = None
                if size < HASH_MINSIZE:
                    checksum = get_filechecksum(absfilename)
                    try:
                        metadata = fs.get(checksum)
                        logging.info("DEDUP %s, already in FileStorage", absfilename)
                    except HTTP404:
                        metadata = fs.put_fast(open(absfilename, "rb"))
                else:
                    logging.info("PUT %s", absfilename)
                    metadata = fs.put_fast(open(absfilename, "rb"))
                archive_dict["filedata"][absfilename] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                }
                if filemeta_md5.hexdigest() in archive_dict["hashmap"]:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()].append(absfilename)
                else:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()] = [absfilename,]
                logging.info(ppls(absfilename, archive_dict["filedata"][absfilename]))
                totalcount += 1
                totalsize += size
            except OSError as exc:
                logging.exception(exc)
            except IOError as exc:
                logging.exception(exc)
    archive_dict["totalcount"] = totalcount
    archive_dict["totalsize"] = totalsize
    archive_dict["stoptime"] = time.time()
    outfile.write(json.dumps(archive_dict).encode("utf-8"))
    logging.info("stored %d files of %s bytes size", totalcount, totalsize)
    duration = archive_dict["stoptime"] - archive_dict["starttime"]
    logging.info("duration %0.2f s, bandwith %0.2f kB/s", duration, totalsize / 1024 / duration)

def diff(data):
    difffiles = []
    # check if some files are missing or have changed
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        if not os.path.isfile(absfile):
            logging.info("DELETED %s", ppls(absile, filedata))
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
            stat = os.stat(absfile)
            if  (stat.st_mtime != st_mtime) or \
                (stat.st_atime != st_atime) or \
                (stat.st_ctime != st_ctime) or \
                (stat.st_uid != st_uid) or \
                (stat.st_gid != st_gid) or \
                (stat.st_mode != st_mode) or \
                (stat.st_size != st_size):
                logging.info("CHANGE %s", ppls(absfile, filedata))
                difffiles.append(absfile)
            else:
                logging.info("OK     %s", ppls(absfile, filedata))
    # check for new files on local storage
    for root, dirs, files in os.walk(data["path"]):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(data["blacklist"], absfilename):
                continue
            if absfilename not in data["filedata"]:
                logging.info("ADD   %s", absfilename)
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

def test(data):
    """
    show archive content
    """
    difffiles = []
    # check if some files are missing or have changed
    filecount = 0
    sizecount = 0
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        logging.info(ppls(absfile, filedata))
        # drwxrwxr-x 2 mesznera mesznera  4096 Okt  6 09:14 .
        # drwxrwxr-x 5 mesznera mesznera  4096 Jun  7 11:59 ..
        # -rw-rw-r-- 1 mesznera mesznera   129 Okt  5 16:06 blacklist.json
        # -rw-rw-r-- 1 mesznera mesznera   129 Jun  7 11:59 blacklist.json.txt
        st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
        #datestring = datetime.datetime.fromtimestamp(int(st_mtime))
        #logging.info("%10s %s %s %10s %19s %s", filemode(st_mode), st_uid, st_gid, sizeof_fmt(st_size), datestring, absfile)
        filecount += 1
        sizecount += st_size
    logging.info("%d files, total size %s", filecount, sizeof_fmt(sizecount))

def restore(backupdata, targetpath):
    """
    check backup archive for consistency
    """
    difffiles = []
    # check if some files are missing or have changed
    filecount = 0
    blockcount = 0
    for absfile, filedata in backupdata["filedata"].items():
        st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
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


def main():
    parser = argparse.ArgumentParser(description='create/manage/restore WebStorage Archives')
    parser.add_argument("-c", '--create', action="store_true", help="create a new archive", required=False) 
    parser.add_argument("-x", '--extract', action="store_true", help="restore content of file", required=False)
    parser.add_argument("-t", '--test', action="store_true", help="shown onventory of archive", required=False)
    parser.add_argument("-d", '--diff', action="store_true", help="show differences between local and given archive", required=False) 
    parser.add_argument("-b", '--blacklist', default="blacklist.json", help="blacklist file in JSON Format", required=False)
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
        if os.path.isfile(args.file):
            logging.error("output file %s already exists, delete it first", args.file)
            sys.exit(1)
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        outfile = args.file
        try:
            create(args.path[0], blacklist, gzip.open(outfile, "wb"))
        except Exception as exc:
            logging.exception(exc)
            os.unlink(outfile)
    elif args.test is True:
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
            sys.exit(1)
        else:
            data = json.loads(str(gzip.open(args.file, "rt").read()))
            test(data)
    elif args.diff is True:
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
        else:
            data = json.loads(str(gzip.open(args.file, "rt").read()))
            pprint.pprint(diff(data))
    elif args.extract is True:
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        if not os.path.isfile(args.file):
            logging.error("you have to provide -f/--file")
            sys.exit(1)
        else:
            data = json.loads(str(gzip.open(args.file, "rt").read()))
            pprint.pprint(data)
            restore(data, args.path[0])


if __name__ == "__main__":
    fs = FileStorageClient()
    main()

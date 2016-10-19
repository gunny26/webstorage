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
import re
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# own modules
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import HTTP404 as HTTP404

BLOCKSIZE = 2 ** 20
HASH_MINSIZE = 1024 * BLOCKSIZE

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

def create_blacklist(absfilename):
    """
    generator for blacklist function

    read exclude file and generate blacklist function
    blacklist function returns True if filenae matches (re.match) any pattern
    """
    patterns = []
    logging.debug("reading exclude file")
    with open(absfilename) as exclude_file:
        for row in exclude_file:
            if len(row) <= 1:
                continue
            if row[0] == "#":
                continue
            operator = row.strip()[0]
            pattern = row.strip()[2:]
            logging.debug("%s %s", operator, pattern)
            rex = re.compile(pattern)
            if operator == "-":
                patterns.append(rex.match)
    def blacklist_func(filename):
        logging.debug("matching %s", filename)
        return any((func(filename) for func in patterns))
    return blacklist_func

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

def create(fs, path, blacklist_func):
    """
    create a new archive of files under path
    filter out filepath which mathes some item in blacklist
    and write file to outfile in FileIndex
    """
    archive_dict = {
        "path" : path,
        "filedata" : {},
        "hashmap" : {},
        "blacklist" : None,
        "starttime" : time.time(),
        "stoptime" : None,
    }
    action_str = "PUT"
    for root, dirs, files in os.walk(path):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_func(absfilename):
                logging.debug("EXCLUDE %s", absfilename)
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
                checksum = None
                if size < HASH_MINSIZE:
                    # calculate checksum localy, and test if this checksum already exists
                    checksum = get_filechecksum(absfilename)
                    if fs.exists(checksum):
                        action_str = "DEDUP"
                    else:
                        # it does not exists, put it up to Filestorage
                        metadata = fs.put_fast(open(absfilename, "rb"))
                        try:
                            assert metadata["checksum"] == checksum
                        except AssertionError as exc:
                            logging.error(exc)
                            logging.error("checksum mismatch at file %s", absfilename)
                            logging.error("locally calculated sha1 checksum: %s", checksum)
                            logging.error("remote  calculated sha1 checksum: %s", metadata["checksum"])
                            raise exc
                else:
                    action_str = "PUT"
                    metadata = fs.put_fast(open(absfilename, "rb"))
                    checksum = metadata["checksum"]
                archive_dict["filedata"][absfilename] = {
                    "checksum" : checksum,
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                }
                if filemeta_md5.hexdigest() in archive_dict["hashmap"]:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()].append(absfilename)
                else:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()] = [absfilename,]
                logging.info("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
            except OSError as exc:
                logging.exception(exc)
            except IOError as exc:
                logging.exception(exc)
    archive_dict["stoptime"] = time.time()
    archive_dict["totalcount"] = len(archive_dict["filedata"])
    archive_dict["totalsize"] = sum((archive_dict["filedata"][absfilename]["stat"][-1] for absfilename in archive_dict["filedata"].keys()))
    return archive_dict

def diff(fs, data, blacklist_func):
    """
    doing differential backup
    criteriat to check if some is change will be the stats informations
    there is a slight possiblity, that the file has change by checksum
    """
    # check if some files are missing or have changed
    changed = False
    data["starttime"] = time.time()
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        if not os.path.isfile(absfile):
            # remove informaion from data, if file was deleted
            logging.info("%8s %s", "DELETED", ppls(absfile, filedata))
            del data["filedata"][absfile]
            changed = True
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
            # check all except atime
            stat = os.stat(absfile)
            change = False
            metadata = None
            # long version to print every single criteria
            if  (stat.st_mtime != st_mtime):
                logging.info("%8s %s", "MTIME", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            elif (stat.st_ctime != st_ctime):
                logging.info("%8s %s", "CTIME", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            elif (stat.st_uid != st_uid):
                logging.info("%8s %s", "UID", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            elif (stat.st_gid != st_gid):
                logging.info("%8s %s", "GID", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            elif (stat.st_mode != st_mode):
                logging.info("%8s %s", "MODE", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            elif (stat.st_size != st_size):
                logging.info("%8s %s", "SIZE", ppls(absfile, filedata))
                metadata = fs.put_fast(open(absfile, "rb"))
                change = True
            # update data dictionary if something has changed
            if change is False:
                logging.debug("%8s %s", "OK", ppls(absfile, filedata))
            else:
                # update data
                data["filedata"][absfile] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                }
                changed = True
    # search for new files on local storage
    for root, dirs, files in os.walk(data["path"]):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_func(absfilename):
                logging.debug("%8s %s", "EXCLUDE", absfilename)
                continue
            if absfilename not in data["filedata"]:
                # there is some new file
                logging.info("%8s %s", "ADD", absfilename)
                try:
                    stat = os.stat(absfilename)
                    metadata = fs.put_fast(open(absfilename, "rb"))
                    data["filedata"][absfilename] = {
                        "checksum" : metadata["checksum"],
                        "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                    }
                    changed = True
                except OSError as exc:
                    logging.error(exc)
                except IOError as exc:
                    logging.error(exc)
    data["stoptime"] = time.time()
    data["totalcount"] = len(data["filedata"])
    data["totalsize"] = sum((data["filedata"][absfilename]["stat"][-1] for absfilename in data["filedata"].keys()))
    return changed

def check(fs, data, deep=False):
    """
    check backup archive for consistency
    check if the filecheksum is available in FileStorage

    if deep is True also every block will be checked
        this operation could be very time consuming!
    """
    bs = None
    if deep is True:
        bs = BlockStorageClient()
    # check if some files are missing or have changed
    filecount = 0
    fileset = set()
    blockcount = 0
    blockset = set()
    for absfile, filedata in data["filedata"].items():
        logging.info("checking file with checksum %s", filedata["checksum"])
        metadata = fs.get(filedata["checksum"])
        filecount += 1
        fileset.add(filedata["checksum"])
        if deep is True:
            logging.info("checking blocks in BlockStorage")
            for block in metadata["blockchain"]:
                blockset.add(block)
                if bs.exists(block) is True:
                    logging.info("%s exists", block)
                else:
                    logging.error("%s block missing", block)
                blockcount += 1
    logging.info("all files %d(%d) available, %d(%d) blocks used", filecount, len(fileset), blockcount, len(blockset))

def restore(fs, backupdata, targetpath):
    """
    restore all files of archive to targetpath
    backuppath will be replaced by targetpath
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

def get_filename(tag):
    return "%s_%s_%s.wstar.gz" % (socket.gethostname(), tag, datetime.datetime.today().isoformat())

def save_archive(data, absfilename):
    """
    save resulting archive data to some kind of storage
    file://, s3:// something else
    """
    outfile = gzip.open(absfilename, "wb")
    outfile.write(json.dumps(data).encode("utf-8"))
    outfile.flush()
    outfile.close()

def save_s3(data, absfilename, s3_bucket, s3_path):
    """
    store loacl file to s3 storage
    """
    import boto3
    client = boto3.client("s3")
    client.upload_file(absfilename, s3_bucket, "%s/%s" % (s3_path, os.path.basename(absfilename)))


def main():
    parser = argparse.ArgumentParser(description='create/manage/restore WebStorage Archives')
    parser.add_argument("-c", '--create', help="create archive of -p/--path to this path", required=False)
    parser.add_argument("-d", '--diff', help="create differential to this archive", required=False)
    parser.add_argument("-e", '--exclude-file', help="exclude file, rsync style, in conjunction with -c/-d", required=False)
    parser.add_argument("-x", '--extract', help="restore content of file to -p location", required=False)
    parser.add_argument("-t", '--test', help="show inventory of archive, like ls", required=False)
    parser.add_argument('--list', help="list content of archive with sha1 checksums and filepath, handy to grab single files with wscat", required=False)
    parser.add_argument('--verify', help="verify archive against Filestorage", required=False)
    parser.add_argument('--verify-deep', action="store_true", default=False, help="in conjunction with --verify, verify also every Block against BlockStorage", required=False)
    parser.add_argument("-p", "--path", help="path to extraxt/create/output", required=False)
    parser.add_argument('--tag', default="backup", help="optional string to implement in auto generated archive filename")
    parser.add_argument('--s3', action="store_true", help="stor wstar archive also on amazon s3, you have to configure aws s3 credentials for this")
    parser.add_argument('--s3-bucket', help="S3 bucket to use")
    parser.add_argument('--s3-path', default="/", help="path in s3 bucket to use")
    parser.add_argument('--cache', action="store_true", default=True, help="in caching mode, alls available FileStorage checksum will be preloaded from backend. consumes more memory")
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR", required=False)
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG", required=False)
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    fs = FileStorageClient(args.cache)
    # exclude file pattern of given
    blacklist_func = None
    if args.exclude_file is not None:
        logging.info("using exclude file %s", args.exclude_file)
        blacklist_func = create_blacklist(args.exclude_file)
    else:
        blacklist_func = lambda a: False
    # CREATE new Archive
    if args.create is not None:
        if os.path.isfile(args.create):
            logging.error("output file %s already exists, delete it first", args.create)
            sys.exit(1)
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.path[0])
            sys.exit(1)
        # create
        try:
            filename = get_filename(args.tag)
            absfilename = os.path.join(args.path, filename)
            logging.info("archiving content of %s to %s", args.path, absfilename)
            archive_dict = create(fs, args.path, blacklist_func)
            logging.info("%(totalcount)d files of %(totalsize)s bytes size" % archive_dict)
            duration = archive_dict["stoptime"] - archive_dict["starttime"]
            logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(archive_dict["totalsize"] / duration))
            # store local
            save_archive(archive_dict, absfilename)
            # store in s3
            if args.s3 is True:
                save_s3(archive_dict, absfilename, args.s3_bucket, args.s3_path)
        except Exception as exc:
            logging.exception(exc)
            os.unlink(absfilename)
    # list content or archive
    elif args.test is not None:
        if not os.path.isfile(args.test):
            logging.error("you have to provide a existing wstar file")
            sys.exit(1)
        else:
            data = json.loads(gzip.open(args.test, "rt").read())
            test(data)
    # Verify and verify deep
    elif args.verify is not None:
        if not os.path.isfile(args.verify):
            logging.error("you have to provide a existing wstar file")
            sys.exit(1)
        else:
            data = json.loads(gzip.open(args.verify, "rt").read())
            if args.verify_deep is True:
                check(fs, data, deep=True)
            else:
                check(fs, data)
    # List content with SHA1 checksums
    elif args.list is not None:
        if not os.path.isfile(args.list):
            logging.error("you have to provide a existing wstar file")
            sys.exit(1)
        else:
            data = json.loads(gzip.open(args.list, "rt").read())
            for absfile in sorted(data["filedata"].keys()):
                filedata = data["filedata"][absfile]
                logging.info("%s %s", filedata["checksum"], absfile)
    # DIFFERENTIAL Backup
    elif args.diff is not None:
        if not os.path.isfile(args.diff):
            logging.error("you have to provide a existing wstar file")
        else:
            data = json.loads(gzip.open(args.diff, "rt").read())
            changed = diff(fs, data, blacklist_func)
            if changed is False:
                logging.info("Nothing changed")
            else:
                logging.info("%(totalcount)d files of %(totalsize)s bytes size" % data)
                duration = data["stoptime"] - data["starttime"]
                logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(data["totalsize"] / duration))
                if args.path is None:
                    logging.error("you have to provide -p/--path to write new archive data to file")
                else:
                    filename = get_filename(args.tag)
                    absfilename = os.path.join(args.path, filename)
                    save_archive(data, absfilename)
                    # store in s3
                    if args.s3 is True:
                        save_s3(archive_dict, absfilename, args.s3_bucket, args.s3_path)
    # EXTRACT to path
    elif args.extract is not None:
        if not os.path.isdir(args.path):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        if not os.path.isfile(args.extract):
            logging.error("you have to provide a existing wstar file")
            sys.exit(1)
        else:
            data = json.loads(gzip.open(args.extract, "rt").read())
            pprint.pprint(data)
            restore(fs, data, args.path[0])
    else:
        logging.error("nice, you have started this program without any purpose?")

if __name__ == "__main__":
    main()


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
from S3Archive import *
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import HTTP404 as HTTP404

#HASH_MINSIZE = 1024 * BLOCKSIZE

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
    # TODO get hashfunc from BlockStorage
    blocksize = 2 ** 20 * 4
    filehash = hashlib.sha1()
    with open(absfile, "rb") as fh:
        data = fh.read(blocksize)
        while data:
            filehash.update(data)
            data = fh.read(blocksize)
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
#        "hashmap" : {},
        "blacklist" : None,
        "starttime" : time.time(),
        "stoptime" : None,
    }
    action_stat = {
      "PUT" : 0,
      "FDEDUP" : 0,
      "BDEDUP" : 0,
      "EXCLUDE" : 0,
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
                metadata = fs.put_fast(open(absfilename, "rb"))
                if metadata["filehash_exists"] is True:
                    action_str = "FDEDUP"
                else:
                    if metadata["blockhash_exists"] > 0:
                        action_str = "BDEDUP"
                    else:
                        action_str = "PUT"
                metadata["checksum"]
                archive_dict["filedata"][absfilename] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode, stat.st_size)
                }
                if action_str == "PUT":
                    logging.error("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                else:
                    logging.info("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                action_stat[action_str] += 1
            except OSError as exc:
                logging.exception(exc)
            except IOError as exc:
                logging.exception(exc)
    for action, count in action_stat.items():
        logging.info("%8s : %s", action, count)
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
        if os.path.isfile(absfile) is False:
            # remove informaion from data, if file was deleted
            logging.info("%8s %s", "DELETED", ppls(absfile, filedata))
            del data["filedata"][absfile]
            changed = True
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
            # check all except atime
            stat = os.stat(absfile)
            change = False
            # long version to print every single criteria
            if  (stat.st_mtime != st_mtime):
                logging.info("%8s %s", "MTIME", ppls(absfile, filedata))
                change = True
            elif (stat.st_ctime != st_ctime):
                logging.info("%8s %s", "CTIME", ppls(absfile, filedata))
                change = True
            elif (stat.st_uid != st_uid):
                logging.info("%8s %s", "UID", ppls(absfile, filedata))
                change = True
            elif (stat.st_gid != st_gid):
                logging.info("%8s %s", "GID", ppls(absfile, filedata))
                change = True
            elif (stat.st_mode != st_mode):
                logging.info("%8s %s", "MODE", ppls(absfile, filedata))
                change = True
            elif (stat.st_size != st_size):
                logging.info("%8s %s", "SIZE", ppls(absfile, filedata))
                change = True
            # update data dictionary if something has changed
            if change is False:
                logging.debug("%8s %s", "OK", ppls(absfile, filedata))
            else:
                metadata = fs.put_fast(open(absfile, "rb"))
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
            if os.path.isfile(absfilename) is False:
                logging.debug("%8s %s", "NOFILE", absfilename)
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

def test(fs, data, deep=False):
    """
    check backup archive for consistency
    check if the filechecksum is available in FileStorage

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

def restore(fs, data, targetpath, overwrite=False):
    """
    restore all files of archive to targetpath
    backuppath will be replaced by targetpath
    """
    difffiles = []
    # check if some files are missing or have changed
    filecount = 0
    blockcount = 0
    if targetpath[-1] == "/":
        targetpath = targetpath[:-1]
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
        newfilename = absfile.replace(data["path"], targetpath)
        # remove double slashes
        if not os.path.isdir(os.path.dirname(newfilename)):
            logging.debug("creating directory %s", os.path.dirname(newfilename))
            os.makedirs(os.path.dirname(newfilename))
        if (os.path.isfile(newfilename)) and (overwrite is True):
            logging.info("REPLACE %s", newfilename)
            outfile = open(newfilename, "wb")
            for block in fs.read(filedata["checksum"]):
              outfile.write(block)
            outfile.close()
        elif (os.path.isfile(newfilename)) and (overwrite is False):
            logging.info("SKIPPING %s", newfilename)
        else:
            logging.info("RESTORE %s", newfilename)
            outfile = open(newfilename, "wb")
            for block in fs.read(filedata["checksum"]):
              outfile.write(block)
            outfile.close()
        try:
            os.chmod(newfilename, st_mode)
            os.utime(newfilename, (st_atime, st_mtime))
            os.chown(newfilename, st_uid, st_gid)
        except OSError as exc:
            logging.error(exc)

def list_content(data):
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

def main():
    parser = argparse.ArgumentParser(description='create/manage/restore WebStorage Archives')
    parser.add_argument("-c", "--create", action="store_true", default=False, help="create archive of -p/--path to this path", required=False)
    parser.add_argument("-d", '--diff', action="store_true", default=False, help="create differential to this archive, needs --file/--s3 to point to a file", required=False)
    parser.add_argument("-e", '--exclude-file', help="local exclude file, rsync style, in conjunction with -c/-d", required=False)
    parser.add_argument("-x", '--extract', action="store_true", default=False, help="restore content of file to -p location, needs --file/--s3 to point to a file", required=False)
    parser.add_argument('--overwrite', action="store_true", default=False, help="overwrite existing files during restore", required=False)
    parser.add_argument("-l", '--list', action="store_true", default=False, help="show inventory of archive, like ls, needs --file/--s3 to point to a file", required=False)
    parser.add_argument('--list-checksums', action="store_true", default=False, help="in conjunction with -t/--test to ouutput sha1 checksums also", required=False)
    parser.add_argument("-b", '--backupsets', action="store_true", default=False, help="list stored wstar archives, needs --file/--s3 to point to a path", required=False)
    parser.add_argument('-t', "--test", action="store_true", default=False, help="verify archive against Filestorage, needs --file/--s3 to point to a file", required=False)
    parser.add_argument('--test-deep', action="store_true", default=False, help="in conjunction with --verify, verify also every Block against BlockStorage", required=False)
    parser.add_argument("-p", "--path", help="path to extract/create", required=False)
    parser.add_argument('--tag', help="optional string to implement in auto generated archive filename, otherwise last portion of -p is used")
    parser.add_argument('--file', help="store wstar archive locally in this path, filename will be auto-generated")
    parser.add_argument('--s3', help="stor wstar archive also on amazon s3, you have to configure aws s3 credentials for this, format <Bucket>/<Path>")
    parser.add_argument('--cache', action="store_true", default=True, help="in caching mode, alls available FileStorage checksum will be preloaded from backend. consumes more memory")
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR", required=False)
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG", required=False)
    args = parser.parse_args()
    logging.debug(args)
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    # exclude file pattern of given
    blacklist_func = None
    if args.exclude_file is not None:
        logging.info("using exclude file %s", args.exclude_file)
        blacklist_func = create_blacklist(args.exclude_file)
    else:
        blacklist_func = lambda a: False
    # use last portion of path for tag
    tag = None
    if args.tag is None:
        if args.path is not None:
            tag = os.path.basename(args.path)
        else:
            # use backup as fallback
            tag = "backup"
    else:
        tag = args.tag
    logging.info("using tag %s", tag)
    # LIST Function
    if args.backupsets is True:
        if args.s3 is None:
            logging.error("you have to provide some s3 path with option --s3")
            sys.exit(1)
        myhostname = socket.gethostname()
        s3_bucket = args.s3.split("/")[0] # the fist part
        s3_path = "/".join(args.s3.split("/")[1:]) # the remaining part
        logging.info("using S3-Bucket : %s", s3_bucket)
        logging.info("showing available wstar archives for host %s in path %s/%s", myhostname, s3_bucket, s3_path)
        backupsets = get_s3_backupsets(myhostname, s3_bucket, s3_path)
        for key in sorted(backupsets.keys()):
            value = backupsets[key]
            logging.info("%(date)10s %(time)8s %(size)s\t%(tag)s\t%(basename)s" % value)
    # CREATE new Archive
    elif args.create is True:
        # caching works best while creating backups
        fs = FileStorageClient(cache=args.cache)
        if not os.path.isdir(args.path[0]):
            logging.error("%s does not exist", args.path[0])
            sys.exit(1)
        # create
        try:
            filename = get_filename(tag)
            logging.info("archiving content of %s to %s", args.path, filename)
            archive_dict = create(fs, args.path, blacklist_func)
            logging.info("%(totalcount)d files of %(totalsize)s bytes size" % archive_dict)
            duration = archive_dict["stoptime"] - archive_dict["starttime"]
            logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(archive_dict["totalsize"] / duration))
            # store in s3
            if args.s3 is not None:
                s3_bucket = args.s3.split("/")[0] # the fist part
                s3_path = "/".join(args.s3.split("/")[1:]) # the remaining part
                save_s3(archive_dict, filename, s3_bucket, s3_path)
        except Exception as exc:
            logging.exception(exc)
    # list content or archive
    elif args.list is True:
        if args.s3 is None:
            logging.error("you have to provide option --s3")
            sys.exit(1)
        else:
            myhostname = socket.gethostname()
            s3_bucket = args.s3.split("/")[0]
            s3_path_or_file = "/".join(args.s3.split("/")[1:])
            s3_key = None
            if s3_path_or_file[-1] == "/":
                # choose lates backupset, if only path is given
                logging.info("searching for latest backuset in %s", s3_path_or_file)
                # remove trailiung slash, s3 wouldnt return any data if this is present
                s3_key = get_s3_latest_backupset(myhostname, s3_bucket, s3_path_or_file[:-1], mytag=tag)
            else:
                # assume this is a file
                s3_key = s3_path_or_file
            data = get_s3_data(s3_bucket, s3_key)
            if args.list_checksums is True:
                for absfile in sorted(data["filedata"].keys()):
                    filedata = data["filedata"][absfile]
                    logging.info("%s %s", filedata["checksum"], absfile)
            else:
                list_content(data)
    # Verify and verify deep
    elif args.test is True:
        if args.s3 is None:
            logging.error("you have to provide option --s3")
            sys.exit(1)
        else:
            myhostname = socket.gethostname()
            s3_bucket = args.s3.split("/")[0]
            s3_path_or_file = "/".join(args.s3.split("/")[1:])
            s3_key = None
            if s3_path_or_file[-1] == "/":
                # choose lates backupset, if only path is given
                logging.info("searching for latest backupset in %s", s3_path_or_file)
                # remove trailiung slash, s3 wouldnt return any data if this is present
                s3_key = get_s3_latest_backupset(myhostname, s3_bucket, s3_path_or_file[:-1], mytag=tag)
            else:
                # assume this is a file
                s3_key = s3_path_or_file
            data = get_s3_data(s3_bucket, s3_key)
            fs = FileStorageClient(cache=args.cache)
            if args.test_deep is True:
                test(fs, data, deep=True)
            else:
                test(fs, data)
    # DIFFERENTIAL Backup
    elif args.diff is True:
        if args.s3 is None:
            logging.error("you have to provide option --s3")
            sys.exit(1)
        else:
            myhostname = socket.gethostname()
            s3_bucket = args.s3.split("/")[0]
            s3_path_or_file = "/".join(args.s3.split("/")[1:])
            s3_key = None # the name of the file
            s3_path = None # the folder the file is in
            if s3_path_or_file[-1] == "/":
                # choose lates backupset, if only path is given
                logging.info("searching for latest backupset in %s", s3_path_or_file)
                # remove trailiung slash, s3 wouldnt return any data if this is present
                s3_key = get_s3_latest_backupset(myhostname, s3_bucket, s3_path_or_file[:-1], mytag=tag)
                s3_path = s3_path_or_file
            else:
                # assume this is a file
                s3_key = s3_path_or_file
                s3_path = s3_path_or_file.split("/")[:-1]
            data = get_s3_data(s3_bucket, s3_key)
            # data will be modified, side-effect
            fs = FileStorageClient(cache=False)
            changed = diff(fs, data, blacklist_func)
            if changed is False:
                logging.info("Nothing changed")
            else:
                logging.info("%(totalcount)d files of %(totalsize)s bytes size" % data)
                duration = data["stoptime"] - data["starttime"]
                logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(data["totalsize"] / duration))
                filename = get_filename(tag)
                # store in s3
                s3_bucket = args.s3.split("/")[0] # the fist part
                s3_path = "/".join(args.s3.split("/")[1:]) # the remaining part
                save_s3(data, filename, s3_bucket, s3_path)
    # EXTRACT to path
    elif args.extract is True:
        if not os.path.isdir(args.path):
            logging.error("folder %s to restore to does not exist", args.create)
            sys.exit(1)
        if args.s3 is None:
            logging.error("you have to provide option --s3")
            sys.exit(1)
        else:
            myhostname = socket.gethostname()
            s3_bucket = args.s3.split("/")[0]
            s3_path_or_file = "/".join(args.s3.split("/")[1:])
            s3_key = None # the name of the file
            s3_path = None # the folder the file is in
            if s3_path_or_file[-1] == "/":
                # choose lates backupset, if only path is given
                logging.info("searching for latest backupset in %s", s3_path_or_file)
                # remove trailiung slash, s3 wouldnt return any data if this is present
                s3_key = get_s3_latest_backupset(myhostname, s3_bucket, s3_path_or_file[:-1], mytag=tag)
                s3_path = s3_path_or_file
            else:
                # assume this is a file
                s3_key = s3_path_or_file
                s3_path = s3_path_or_file.split("/")[:-1]
        # no caching for restore needed
        data = get_s3_data(s3_bucket, s3_key)
        fs = FileStorageClient(cache=False)
        restore(fs, data, args.path, overwrite=args.overwrite)
    else:
        logging.error("nice, you have started this program without any purpose?")

if __name__ == "__main__":
    main()


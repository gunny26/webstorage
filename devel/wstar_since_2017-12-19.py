#!/usr/bin/python3
# pylint: disable=line-too-long
# disable=locally-disabled, multiple-statements, fixme, line-too-long
"""
command line program to create/restore/test WebStorageArchives
"""
import os
import hashlib
import datetime
import time
import sys
import socket
import argparse
import stat
import re
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# own modules
from webstorage import WebStorageArchive as WebStorageArchive
from webstorage import FileStorageClient as FileStorageClient

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
        """
        returned closure to use for blacklist checking
        """
        logging.debug("matching %s", filename)
        return any((func(filename) for func in patterns))
    return blacklist_func

def create(filestorage, path, blacklist_func):
    """
    create a new archive of files under path
    filter out filepath which mathes some item in blacklist
    and write file to outfile in FileIndex
    """
    archive_dict = {
        "path" : path,
        "filedata" : {},
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
                stats = os.stat(absfilename)
                metadata = filestorage.put_fast(open(absfilename, "rb"))
                if metadata["filehash_exists"] is True:
                    action_str = "FDEDUP"
                else:
                    if metadata["blockhash_exists"] > 0:
                        action_str = "BDEDUP"
                    else:
                        action_str = "PUT"
                archive_dict["filedata"][absfilename] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                }
                if action_str == "PUT":
                    logging.error("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                else:
                    logging.info("%8s %s", action_str, ppls(absfilename, archive_dict["filedata"][absfilename]))
                action_stat[action_str] += 1
            except (OSError, IOError) as exc:
                logging.exception(exc)
    logging.info("file operations statistics:")
    for action, count in action_stat.items():
        logging.info("%8s : %s", action, count)
    archive_dict["stoptime"] = time.time()
    archive_dict["totalcount"] = len(archive_dict["filedata"])
    archive_dict["totalsize"] = sum((archive_dict["filedata"][absfilename]["stat"][-1] for absfilename in archive_dict["filedata"].keys()))
    return archive_dict

def diff(filestorage, data, blacklist_func):
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
            stats = os.stat(absfile)
            change = False
            # long version to print every single criteria
            if  stats.st_mtime != st_mtime:
                logging.info("%8s %s", "MTIME", ppls(absfile, filedata))
                change = True
            elif stats.st_ctime != st_ctime:
                logging.info("%8s %s", "CTIME", ppls(absfile, filedata))
                change = True
            elif stats.st_uid != st_uid:
                logging.info("%8s %s", "UID", ppls(absfile, filedata))
                change = True
            elif stats.st_gid != st_gid:
                logging.info("%8s %s", "GID", ppls(absfile, filedata))
                change = True
            elif stats.st_mode != st_mode:
                logging.info("%8s %s", "MODE", ppls(absfile, filedata))
                change = True
            elif stats.st_size != st_size:
                logging.info("%8s %s", "SIZE", ppls(absfile, filedata))
                change = True
            # update data dictionary if something has changed
            if change is False:
                logging.debug("%8s %s", "OK", ppls(absfile, filedata))
            else:
                metadata = filestorage.put_fast(open(absfile, "rb"))
                # update data
                data["filedata"][absfile] = {
                    "checksum" : metadata["checksum"],
                    "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
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
                    stats = os.stat(absfilename)
                    metadata = filestorage.put_fast(open(absfilename, "rb"))
                    data["filedata"][absfilename] = {
                        "checksum" : metadata["checksum"],
                        "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                    }
                    changed = True
                except (OSError, IOError) as exc:
                    logging.error(exc)
    data["stoptime"] = time.time()
    data["totalcount"] = len(data["filedata"])
    data["totalsize"] = sum((data["filedata"][absfilename]["stat"][-1] for absfilename in data["filedata"].keys()))
    return changed

def test(filestorage, data, level=0):
    """
    check backup archive for consistency
    check if the filechecksum is available in FileStorage

    if deep is True also every block will be checked
        this operation could be very time consuming!
    """
    filecount = 0 # number of files
    fileset = set() # unique list of filechecksums
    blockcount = 0 # number of blocks
    blockset = set() # unique list of blockchecksums
    if level == 0: # check only checksum existance in filestorage
        for absfile, filedata in data["filedata"].items():
            if filestorage.exists(filedata["checksum"]) is True:
                logging.info("FILE-CHECKSUM %s EXISTS  for %s", filedata["checksum"], absfile)
                filecount += 1
                fileset.add(filedata["checksum"])
    elif level == 1: # get filemetadata and check also block existance
        blockstorage = filestorage.blockstorage
        for absfile, filedata in data["filedata"].items():
            metadata = filestorage.get(filedata["checksum"])
            logging.info("FILE-CHECKSUM %s OK     for %s", filedata["checksum"], absfile)
            filecount += 1
            fileset.add(filedata["checksum"])
            for blockchecksum in metadata["blockchain"]:
                blockset.add(blockchecksum)
                if blockstorage.exists(blockchecksum) is True:
                    logging.info("BLOCKCHECKSUM %s EXISTS", blockchecksum)
                else:
                    logging.error("BLOCKCHECKSUM %s MISSING", blockchecksum)
                blockcount += 1
    elif level == 2: # get filemetadata and read every block, very time consuming
        blockstorage = filestorage.blockstorage
        for absfile, filedata in data["filedata"].items():
            metadata = filestorage.get(filedata["checksum"])
            logging.info("FILE-CHECKSUM %s OK      for %s", filedata["checksum"], absfile)
            filecount += 1
            fileset.add(filedata["checksum"])
            for blockchecksum in metadata["blockchain"]:
                blockset.add(blockchecksum)
                blockstorage.get(blockchecksum)
                logging.info("BLOCKCHECKSUM %s OK", blockchecksum)
                blockcount += 1
    logging.info("all files %d(%d) available, %d(%d) blocks used", filecount, len(fileset), blockcount, len(blockset))

def restore(filestorage, data, targetpath, overwrite=False):
    """
    restore all files of archive to targetpath
    backuppath will be replaced by targetpath
    """
    # check if some files are missing or have changed
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
            for block in filestorage.read(filedata["checksum"]):
                outfile.write(block)
            outfile.close()
        elif (os.path.isfile(newfilename)) and (overwrite is False):
            logging.info("SKIPPING %s", newfilename)
        else:
            logging.info("RESTORE %s", newfilename)
            outfile = open(newfilename, "wb")
            for block in filestorage.read(filedata["checksum"]):
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
    # check if some files are missing or have changed
    filecount = 0
    sizecount = 0
    for absfile in sorted(data["filedata"].keys()):
        filedata = data["filedata"][absfile]
        logging.info(ppls(absfile, filedata))
        # st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
        filecount += 1
        sizecount += filedata["stat"][6]
    logging.info("%d files, total size %s", filecount, sizeof_fmt(sizecount))

def get_filename(tag):
    """
    return standard wstar archive name, composed from hostname,
    tag and datetime
    """
    return "%s_%s_%s.wstar.gz" % (socket.gethostname(), tag, datetime.datetime.today().isoformat())

def main():
    """
    get options, then call specific functions
    """
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
    parser.add_argument('--test-level', default=0, help="in conjunction with --test, 0=fast, 1=medium, 2=fully", required=False)
    parser.add_argument("-p", "--path", help="path to extract/create", required=False)
    parser.add_argument('--tag', help="optional string to implement in auto generated archive filename, otherwise last portion of -p is used")
    parser.add_argument('--filename', help="filename to get from WebStorageArchive Store, if not given the latest available will be used")
    parser.add_argument('--cache', action="store_true", default=True, help="in caching mode, alls available FileStorage checksum will be preloaded from backend. consumes more memory")
    parser.add_argument('--nocache', dest="cache", action="store_false", default=True, help="disable caching mode")
    parser.add_argument('-q', "--quiet", action="store_true", help="switch to loglevel ERROR", required=False)
    parser.add_argument('-v', "--verbose", action="store_true", help="switch to loglevel DEBUG", required=False)
    args = parser.parse_args()
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
    if args.tag is None and args.path is not None:
        tag = os.path.basename(os.path.dirname(args.path))
        logging.info("--tag not provided, using auto generated tag %s", tag)
    else:
        tag = args.tag
    # LIST available backupsets
    if args.backupsets is True:
        myhostname = socket.gethostname()
        wsa = WebStorageArchive()
        backupsets = wsa.get_backupsets(myhostname)
        for key in sorted(backupsets.keys()):
            value = backupsets[key]
            logging.info("%(date)10s %(time)8s %(size)s\t%(tag)s\t%(basename)s", value)
    # CREATE new Archive
    elif args.create is True:
        # caching works best while creating backups
        filestorage = FileStorageClient(cache=args.cache)
        if not os.path.isdir(args.path):
            logging.error("%s does not exist", args.path)
            sys.exit(1)
        # create
        filename = get_filename(tag)
        logging.info("archiving content of %s to %s", args.path, filename)
        archive_dict = create(filestorage, args.path, blacklist_func)
        logging.info("%(totalcount)d files of %(totalsize)s bytes size", archive_dict)
        duration = archive_dict["stoptime"] - archive_dict["starttime"]
        logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(archive_dict["totalsize"] / duration))
        # store
        wsa = WebStorageArchive()
        wsa.put(archive_dict, filename)
    # list content or archive
    elif args.list is True:
        myhostname = socket.gethostname()
        wsa = WebStorageArchive()
        filename = None
        if args.filename is None:
            logging.info("-f not provided, using latest available archive")
            filename = wsa.get_latest_backupset(myhostname)
        else:
            filename = args.filename
        if filename is not None:
            data = wsa.get(filename)
            if args.list_checksums is True:
                for absfile in sorted(data["filedata"].keys()):
                    filedata = data["filedata"][absfile]
                    logging.info("%s %s", filedata["checksum"], absfile)
            else:
                list_content(data)
        else:
            logging.info("no backupset found")
    # Verify and verify deep
    elif args.test is True:
        myhostname = socket.gethostname()
        wsa = WebStorageArchive()
        filename = None
        if args.filename is None:
            logging.info("-f not provided, using latest available archive")
            filename = wsa.get_latest_backupset(myhostname)
        else:
            filename = args.filename
        data = wsa.get(filename)
        filestorage = FileStorageClient(cache=args.cache)
        test(filestorage, data, level=int(args.test_level))
    # DIFFERENTIAL Backup
    elif args.diff is True:
        myhostname = socket.gethostname()
        wsa = WebStorageArchive()
        filename = None
        if args.filename is None:
            logging.info("-f not provided, using latest available archive")
            filename = wsa.get_latest_backupset(myhostname)
        else:
            filename = args.filename
        data = wsa.get(filename)
        # data will be modified, side-effect
        filestorage = FileStorageClient(cache=False)
        changed = diff(filestorage, data, blacklist_func)
        if changed is False:
            logging.info("Nothing changed")
        else:
            logging.info("%(totalcount)d files of %(totalsize)s bytes size", data)
            duration = data["stoptime"] - data["starttime"]
            logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(data["totalsize"] / duration))
            # store
            newfilename = get_filename(tag)
            wsa.put(data, newfilename)
    # EXTRACT to path
    elif args.extract is True:
        if args.path is None:
            logging.error("you have to provide some path to restore to with parameter -p")
            sys.exit(1)
        if not os.path.isdir(args.path):
            logging.error("folder %s to restore to does not exist", args.create)
            sys.exit(1)
        myhostname = socket.gethostname()
        wsa = WebStorageArchive()
        filename = None
        if args.filename is None:
            logging.info("-f not provided, using latest available archive")
            filename = wsa.get_latest_backupset(myhostname)
        else:
            filename = args.filename
        # get archive Data
        data = wsa.get(filename)
        # no caching for restore needed
        filestorage = FileStorageClient(cache=False)
        restore(filestorage, data, args.path, overwrite=args.overwrite)
    else:
        logging.error("nice, you have started this program without any purpose?")

if __name__ == "__main__":
    main()


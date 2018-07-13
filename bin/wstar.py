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
import json
import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# from Crypto.PublicKey import RSA
# from Crypto.Signature import PKCS1_v1_5
# from Crypto.Hash import SHA256
# own modules
#import webstorage
from webstorage import WebStorageArchiveClient
from webstorage import FileStorageClient

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
    for file statistics use filedate["stat"] segment
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
    with open(absfilename, "rt") as exclude_file:
        for row in exclude_file:
            if len(row) <= 1:
                continue
            if row[0] == "#":
                continue
            operator = row.strip()[0] # -/+
            pattern = row.strip()[2:] # some string to use in match
            logging.debug("%s %s", operator, pattern)
            if operator == "-":
                patterns.append(pattern)
    def blacklist_func(filename):
        """
        returned closure to use for blacklist checking
        """
        logging.debug("matching %s", filename)
        return any((pattern in filename for pattern in patterns))
    return blacklist_func

def create(filestorage, path, blacklist_func):
    """
    create a new archive of files under path
    filter out filepath which mathes some item in blacklist
    and write file to outfile in FileIndex

    filestorage ... <FileStorage> Object
    path ... <str> must be valid os path
    blacklist_func ... <func> called with absfilename, if True is returned, skip this file
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
                metadata = filestorage.put(open(absfilename, "rb"))
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
    archive_dict["totalsize"] = sum((archive_dict["filedata"][absfilename]["stat"][-1] for absfilename in archive_dict["filedata"]))
    return archive_dict

def diff(filestorage, data, blacklist_func):
    """
    doing differential backup
    criteriat to check if some file is change will be the stats informations
    there is a slight possiblity, that the file has change by checksum but non in stats information

    filestorage ... <FileStorage> Object
    data ... <dict> existing data to compare with existing files
    blacklist_func ... <func> called with absfilename, if True is returned, skip this file
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
                try:
                    with open(absfile, "rb") as infile:
                        metadata = filestorage.put(infile)
                        # update data
                        data["filedata"][absfile] = {
                            "checksum" : metadata["checksum"],
                            "stat" : (stats.st_mtime, stats.st_atime, stats.st_ctime, stats.st_uid, stats.st_gid, stats.st_mode, stats.st_size)
                        }
                        changed = True
                except PermissionError as exc:
                    logging.error(exc)
                    logging.error("skipping file %s", absfile)
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
                    metadata = filestorage.put(open(absfilename, "rb"))
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

def restore_single(filestorage, filedata, targetpath, name, checksum, overwrite=False):
    """
    restore singl file, identified by name and checksum,
    to targetpath, named by basename in backupset
    """
    st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode, st_size = filedata["stat"]
    newfilename = os.path.join(targetpath, os.path.basename(name))
    # replace, skip or restore
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
    try: # change permissions and times
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

def signature_valid(data, signature, public_key_filename):
    """
    verify if given signature (in hex notation) is valid

    data - some sort string data
    signature - string of hex as returned by get_signature
    public_key_filename ... path to public key in DER Format
    """
    key = open(os.path.expanduser(public_key_filename), "rb").read()
    rsakey = RSA.importKey(key)
    digest = SHA256.new()
    digest.update(data)
    verifier = PKCS1_v1_5.new(rsakey)
    if verifier.verify(digest, bytes.fromhex(signature)):
        return True
    return False

def save_webstorage_archive(data, filename, private_key=None):
    """
    add duration, checksum and signature to data,
    afterwards store in WebStorageArchive
    """
    duration = data["stoptime"] - data["starttime"]
    logging.info("duration %0.2f s, bandwith %s /s", duration, sizeof_fmt(data["totalsize"] / duration))
    # build sah256 checksum
    sha256 = hashlib.sha256()
    sha256.update(json.dumps(data, sort_keys=True).encode("utf-8"))
    data["checksum"] = sha256.hexdigest()
    logging.info("checksum of archive %s", data["checksum"])
    # sorting keys is essential to ensure same signature
    if private_key:
        data["signature"] = get_signature(json.dumps(data, sort_keys=True).encode("utf-8"), private_key)
    else:
        data["signature"] = ""
    logging.info("%(totalcount)d files of %(totalsize)s bytes size", data)
    # store
    wsa = WebStorageArchiveClient()
    wsa.put(data, filename)

def get_webstorage_data(public_key=None, filename=None):
    """
    return data from webstorage archive

    public_key ... path to public key file, to verify signature, if present
    filename ... to name a file, or otherwise use the latest available backupset
    """
    wsa = WebStorageArchiveClient()
    if filename is None:
        logging.info("-f not provided, using latest available archive")
        filename = wsa.get_latest_backupset(args.hostname)
    data = wsa.get(filename)
    if (public_key is not None) and ("signature" in data):
        signature = data["signature"]
        del data["signature"]
        # sorting keys is essential to ensure same signature
        data_str = json.dumps(data, sort_keys=True).encode("utf-8")
        if signature_valid(data_str, signature, public_key):
            logging.info("digital signature in archive is valid")
        else:
            logging.error("digital signature in archive is invalid")
            raise AttributeError("digital signature in archive is invalid")
    else:
        logging.info("no digital signature provided")
    return data

def main():
    """
    get options, then call specific functions
    """
    parser = argparse.ArgumentParser(description="create/manage/restore WebStorage Archives")
    group_create = parser.add_argument_group("create backupset from scratch")
    group_create.add_argument("-c", dest="create", help="create archive of this path")
    group_diff = parser.add_argument_group("create incremental backupset, some pre existing backupset must exist")
    group_diff.add_argument("-d", dest="diff", action="store_true", help="create differential to latest backupset or expliit given backupset")
    group_extract = parser.add_argument_group("extract archive")
    group_extract.add_argument("-x", dest="extract", action="store_true", help="restore content of backupset to path location")
    group_extract.add_argument("--backupset", help="backupset to get from backend, if not given use the latest available backupset")
    group_extract.add_argument("--overwrite", action="store_true", default=False, help="overwrite existing files during restore default %(default)s")
    group_extract.add_argument("--extract-path", help="path to restore to")
    group_get = parser.add_argument_group("get single file from backupset")
    group_get.add_argument("-g", dest="get", action="store_true", help="get single files from backupset")
    group_get.add_argument("--checksum", help="file checksum")
    group_get.add_argument("--name", help="file checksum")
    group_optional = parser.add_argument_group("optional")
    group_optional.add_argument("--exclude-file", help="exclude file, in conjunction with --create and --diff")
    group_optional.add_argument("--tag", help="optional tag for this archive, otherwise last portion of path is used")
    group_optional.add_argument("--nocache", dest="cache", action="store_false", default=True, help="disable caching mode, using less memory")
    group_optional.add_argument("--hostname", dest="hostname", help="set specific hostname")
    group_test = parser.add_argument_group("testing of backupsets and retrieving existing archive informations")
    group_test.add_argument("-l", dest="list", action="store_true", help="list backupsets, use --backupset to specify one specific")
    group_test.add_argument("--list-checksums", action="store_true", default=False, help="in conjunction with -list to output also checksums")
    group_test.add_argument("-t", dest="test", action="store_true", help="verify archive, use --backupset to specify one specific")
    group_test.add_argument("--test-level", default=0, help="in conjunction with --test, 0=fast, 1=medium, 2=fully")
    group_sign = parser.add_argument_group("signing of archive files")
    group_sign.add_argument("--sign", dest="sign", action="store_true", default=False, help="turn on signing of backupsets, defauls %(default)s")
    group_sign.add_argument("--public-key", default="~/.webstorage/public.der", help="path to public key file in DER format")
    group_sign.add_argument("--private-key", default="~/.webstorage/private.der", help="path to private key file in DER format")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-q", "--quiet", action="store_true", help="switch to loglevel ERROR")
    group.add_argument("-v", "--verbose", action="store_true", help="switch to loglevel DEBUG")
    args = parser.parse_args()
    # set logging level
    if args.quiet is True:
        logging.getLogger("").setLevel(logging.ERROR)
    if args.verbose is True:
        logging.getLogger("").setLevel(logging.DEBUG)
    # exclude file pattern of given
    blacklist_func = None
    if args.exclude_file is not None:
        logging.debug("using exclude file %s", args.exclude_file)
        blacklist_func = create_blacklist(args.exclude_file)
    else:
        blacklist_func = lambda a: False
    # load crypto libraries if signing enabled
    if args.sign is True:
        if not os.path.isfile(args.private_key):
            logging.error("private_key file %s does not exist")
            sys.exit(1)
        if not os.path.isfile(args.public_key):
            logging.error("public_key file %s does not exist")
            sys.exit(1)
        from Crypto.PublicKey import RSA
        from Crypto.Signature import PKCS1_v1_5
        from Crypto.Hash import SHA256
    else:
        logging.info("signing of backupsets is disabled")
        args.private_key = None
        args.public_key = None
    #
    #
    # MAIN OPTIONS Sections
    #
    if not args.hostname:
        args.hostname = socket.gethostname()
    wsa = WebStorageArchiveClient()
    filestorage = FileStorageClient(cache=args.cache)
    # CREATE new Backupset
    if args.create:
        if not args.tag:
            args.tag = os.path.basename(os.path.dirname(args.create))
        if not os.path.isdir(args.create):
            logging.error("%s does not exist", args.create)
            sys.exit(1)
        # create
        filename = get_filename(args.tag)
        logging.info("archiving content of %s to backupset %s", args.create, filename)
        data = create(filestorage, args.create, blacklist_func)
        save_webstorage_archive(data, filename, args.private_key)
    # LIST Backupsets
    elif args.list:
        args.cache = False # set this explicit, not useful
        # list all available backupsets
        if not args.backupset: # list all available
            for value in wsa.get_backupsets(args.hostname):
                logging.info("%(date)10s %(time)8s %(size)s\t%(tag)s\t%(basename)s", value)
        else: # list content of specific backupset
            logging.info("getting backupset %s", args.backupset)
            data = get_webstorage_data(args.public_key, args.backupset)
            if data is not None:
                if args.list_checksums is True:
                    for absfile in sorted(data["filedata"].keys()):
                        filedata = data["filedata"][absfile]
                        logging.info("%s %s", filedata["checksum"], absfile)
                else:
                    list_content(data)
            else:
                logging.info("backupset not found found")
    # TEST Backupset
    elif args.test:
        if not args.backupset:
            logging.info("using latest backupset, otherwise use --backupset to specify one specific")
            args.backupset = wsa.get_lastest_backupset()
        logging.info("testing backupset %s", args.backupset)
        data = get_webstorage_data(args.public_key, args.backupset)
        test(filestorage, data, level=int(args.test_level))
    # DIFFERENTIAL Backupset
    elif args.diff:
        if not args.backupset:
            logging.info("using latest backupset, otherwise use --backupset to specify one specific")
            args.backupset = wsa.get_latest_backupset()
        logging.info("creating differential backupset to existing backupset %s", args.backupset)
        if not args.tag:
            args.tag = args.backupset.split("_")[1]
        data = get_webstorage_data(args.public_key, args.backupset)
        changed = diff(filestorage, data, blacklist_func)
        if changed is False:
            logging.info("Nothing changed")
        else:
            newfilename = get_filename(args.tag)
            save_webstorage_archive(data, newfilename, args.private_key)
    # EXTRACT Backupset to path
    elif args.extract:
        if args.extract_path is None:
            logging.error("you have to provide some path to restore to with parameter -p")
            sys.exit(1)
        if not os.path.isdir(args.extract_path):
            logging.error("folder %s to restore to does not exist", args.extract_path)
            sys.exit(1)
        if not args.backupset: # to prevent accidentially restores
            logging.info("you have to specify --backupset explicitly")
            sys.exit(1)
        data = get_webstorage_data(args.public_key, args.backupset)
        restore(filestorage, data, args.extract_path, overwrite=args.overwrite)
    # GET Backupset to path
    elif args.get:
        if args.extract_path is None:
            logging.error("you have to provide some path to restore file to with -p")
            sys.exit(1)
        if not os.path.isdir(args.extract_path):
            logging.error("folder %s to restore file does not exist", args.extract_path)
            sys.exit(1)
        if not args.backupset: # to prevent accidentially restores
            logging.info("you have to specify --backupset explicitly")
            sys.exit(1)
        if not args.name or not args.checksum:
            logging.info("you have to provide both --name and --checksum")
        data = get_webstorage_data(args.public_key, args.backupset)
        if args.name not in data["filedata"]:
            logging.error("provided --name %s does not exist in backupset")
            sys.exit(2)
        filedata = data["filedata"][args.name]
        if args.checksum != filedata["checksum"]:
            logging.error("provided --checksum %s does not match stored checksum")
            sys.exit(2)
        restore_single(filestorage, filedata, args.extract_path, args.name, args.checksum, overwrite=args.overwrite)
    else:
        logging.error("nice, you have started this program without any purpose?")

if __name__ == "__main__":
    main()

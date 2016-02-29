#!/usr/bin/python
import glob
import os
import hashlib
import time
import json
import StringIO
import gzip
import sys
import socket
# own modules
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient

def blacklist_match(blacklist, absfilename):
    for item in blacklist:
        if item in absfilename:
            return True
    return False


def create(path, blacklist, outfile):
    archive_dict = {
        "path" : path,
        "filedata" : {},
        "hashmap" : {},
        "outfile" : outfile,
        "blacklist" : blacklist,
        "starttime" : time.time(),
        "stoptime" : None,
    }
    totalsize = 0
    totalcount = 0
    for root, dirs, files in os.walk(unicode(arg)):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(blacklist, absfilename):
                print "%s blacklisted" % absfilename
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
                    "stat" : (stat.st_mtime, stat.st_atime, stat.st_ctime, stat.st_uid, stat.st_gid, stat.st_mode)
                }
                if filemeta_md5.hexdigest() in archive_dict["hashmap"]:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()].append(absfilename)
                else:
                    archive_dict["hashmap"][filemeta_md5.hexdigest()] = [absfilename,]
                print "%s %s" % (metadata["checksum"], absfilename)
                totalcount += 1
                totalsize += size
            except OSError as exc:
                print exc
            except IOError as exc:
                print exc
    print "stored %d files of %s bytes size" % (totalcount, totalsize)
    archive_dict["totalcount"] = totalcount
    archive_dict["totalsize"] = totalsize
    archive_dict["stoptime"] = time.time()
    gzip_data = StringIO.StringIO()
    gzip_handle = gzip.GzipFile(fileobj=gzip_data, mode="w")
    gzip_handle.write(json.dumps(archive_dict))
    gzip_handle.close()
    fi.write(StringIO.StringIO(gzip_data.getvalue()), outfile)
    #fh = gzip.open(outfile, "w+b")
    #json.dump(archive_dict, fh)
    #fh.close()
    #fi.upload(outfile)
    filehash = fi.get(outfile)
    return filehash

def diff(checksum):
    difffiles = []
    blockchain = fs.get(checksum)
    print blockchain
    data = ""
    for block in fs.read(checksum):
        data += block
    gzip_handle = gzip.GzipFile(fileobj=StringIO.StringIO(data))
    backupdata = json.loads(gzip_handle.read())
    # check if some files are missing or have changed
    for absfile, filedata in backupdata["filedata"].items():
        if not os.path.isfile(absfile):
            print "%s [%s] does no longer exist" % (absfile, filedata["checksum"])
        else:
            st_mtime, st_atime, st_ctime, st_uid, st_gid, st_mode  = filedata["stat"]
            stat = os.stat(absfile)
            if  (stat.st_mtime != st_mtime) or \
                (stat.st_atime != st_atime) or \
                (stat.st_ctime != st_ctime) or \
                (stat.st_uid != st_uid) or \
                (stat.st_gid != st_gid) or \
                (stat.st_mode != st_mode):
                print "%s [%s] changed" % (absfile, filedata["checksum"])
                difffiles.append(absfile)
    # check for new files on local storage
    for root, dirs, files in os.walk(unicode(backupdata["path"])):
        for filename in files:
            absfilename = os.path.join(root, filename)
            if blacklist_match(backupdata["blacklist"], absfilename):
                continue
            if absfilename not in backupdata["filedata"]:
                print "new file %s detected" % absfilename
                difffiles.append(absfilename)
    return difffiles

if __name__ == "__main__":
    bs = BlockStorageClient()
    fs = FileStorageClient(bs)
    fi = FileIndexClient(fs)
    try:
        arg = sys.argv[1]
    except IndexError:
        arg = "/home/arthur/"
    basedir = "/wstar_%s" % socket.gethostname()
    wstarname = "%s_%d_0.wstar" % (socket.gethostname(), int(time.time()))
    outfile = "%s/%s" % (basedir, wstarname)
    if not fi.isdir(basedir):
        fi.mkdir(basedir)
    blacklist = json.load(open("blacklist.json", "r"))
    filehash = create(arg, blacklist, outfile)
    print "Backup stored in %s" % filehash
    difffiles = diff(filehash)
    print difffiles

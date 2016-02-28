#!/usr/bin/python
import glob
import os
import hashlib
import time
import json
import StringIO
import gzip
from WebStorageClient import BlockStorageClient as BlockStorageClient
from WebStorageClient import FileStorageClient as FileStorageClient
from WebStorageClient import FileIndexClient as FileIndexClient

bs = BlockStorageClient()
fs = FileStorageClient(bs)
fi = FileIndexClient(fs)
arg = "/home/arthur/Dokumente"
outfile = "wtar_home_backup_%d.json.gz" % int(time.time())

def blacklist_match(blacklist, absfilenam):
    for item in blacklist:
        if item in absfilename:
            return True
    return False

blacklist = (
    u"/home/arthur/Downloads",
    u"/home/arthur/Videos",
    u"/home/arthur/.cache",
    u"/home/arthur/.local/share/Trash/",
    u"/home/arthur/gits",
    )

archive_dict = {
    "filedata" : {},
    "hashmap" : {}
}
totalsize = 0
totalcount = 0
archive_dict["starttime"] = time.time()
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
fh = gzip.open(outfile, "w+b")
json.dump(archive_dict, fh)
fi.upload(outfile)
data = list(fi.read(u"/" + outfile))
print data
fh.close()


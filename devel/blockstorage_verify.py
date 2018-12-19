#!/usr/bin/python3
"""
program to validate blockchain of blockstorage backend
"""
import hashlib
# own modules
from webstorageClient import BlockStorageClient

bsc = BlockStorageClient()
info = bsc.info
# epoch counting starts at 1
# epoch 1 has no checksum only seed
# epoch 2 is the first full entry
epoch = 1
print("seed                         : ", info["blockchain_seed"])
last_sha256 = info["blockchain_seed"]
sha256 = hashlib.sha256()
for index, checksum in enumerate(bsc.checksums):
    epoch = index + 2 # forst real epoch is 2
    sha256 = hashlib.sha256()
    # use epoch of last sha256 key + last sha256 key + actual checksum
    sha256.update(str(epoch-1).encode("ascii") + last_sha256.encode("ascii") + checksum.encode("ascii"))
    last_sha256 = sha256.hexdigest()
print("calculated until epoch       : ", epoch)
print("latest checksum              : ", checksum)
print("resulting blockchain_checksum: ", last_sha256)
print("blockchain_checksum          : ", info["blockchain_checksum"])
if last_sha256 == info["blockchain_checksum"]:
    print("blockchain is valid")
else:
    print("blockchain is invalid")



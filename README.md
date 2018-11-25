# Webstorage

A webbased File-/BlockStorage written in 100% in python3.

## design considerations

  * using SHA1 to calculate block and file checksums
  * low memory consumption
  * should run on minimum resources like on raspberrypi
  * stateless
  * Restful Backend
  * deduplication on block level
  * deduplication on file level
  * distributable, syncable, scaleable

## BlockStorage

this restful webservice will store chunks of binary data to filesystem.
probably you will give BlockStorage a whole volume (either classical disk or LVM) to store it's chunks on.
BlockStorage will produce many inodes so it will be best to format with XFS.

Chunks will be stored under SHA1 checksum name on disk, so there is a built in verification.

## FileStorage

FileStorage will store a plan to build large binary data out of chunks from BlockStorage.
FileStorage will therefor save some json data, called the recipe, to reproduce every binary data.
These recipe will be stored unter SHA1 checksum of the whole binary data, so there is a built in verfication.

Filestorage will not store the original name nor meta data of this file,
it's only a binary data stream cut into pieces with maximum 1MB length.

### how to get sha1 checksum of any file

```bash
pi@cloud:/opt/webstorage/devel $ sha1sum ../setup.py 
ebc4f5817cdb41be913ee3ffebe5e65b4ae9662f  ../setup.py
```

This file will be stored in recipe format like this
```json
{
    "blockchain": [
        "ebc4f5817cdb41be913ee3ffebe5e65b4ae9662f"
    ],
    "blockhash_exists": 0,
    "checksum": "ebc4f5817cdb41be913ee3ffebe5e65b4ae9662f",
    "filehash_exists": false,
    "mime_type": "application/octet-stream",
    "size": 1014
}
```

so it seems the file consists only of one single block, the block will be in binary notation

```bash
mesznera@ws00007999:~/gits/webstorage$ hexdump test.bin 
0000000 7266 6d6f 6420 7369 7574 6974 736c 632e
0000010 726f 2065 6d69 6f70 7472 7320 7465 7075
0000020 202c 7845 6574 736e 6f69 0a6e 6623 6f72
0000030 206d 7943 6874 6e6f 422e 6975 646c 6920
0000040 706d 726f 2074 7963 6874 6e6f 7a69 0a65
0000050 6d69 6f70 7472 7320 7379 202c 7473 6972
0000060 676e 202c 736f 690a 706d 726f 2074 6873
0000070 7475 6c69 0a0a 7261 7367 3d20 7b20 6e22
0000080 6d61 2265 203a 7722 6265 7473 726f 6761
0000090 2265 0a2c 2020 2020 2020 2020 6122 7475
00000a0 6f68 2272 203a 4122 7472 7568 2072 654d
00000b0 7373 656e 2272 0a2c 2020 2020 2020 2020
00000c0 6122 7475 6f68 5f72 6d65 6961 226c 203a
00000d0 6122 7472 7568 2e72 656d 7373 656e 4072
00000e0 6d67 6961 2e6c 6f63 226d 0a2c 2020 2020
00000f0 2020 2020 6422 7365 7263 7069 6974 6e6f
0000100 3a22 2220 6557 5362 6f74 6172 6567 4120
0000110 6372 6968 6976 676e 5320 7379 6574 226d
0000120 0a2c 2020 2020 2020 2020 7522 6c72 2022
0000130 203a 6822 7474 7370 2f3a 672f 7469 7568
0000140 2e62 6f63 2f6d 7567 6e6e 3279 2f36 6577
0000150 7362 6f74 6172 6567 2c22 200a 2020 2020
0000160 2020 2220 6f6c 676e 645f 7365 7263 7069
0000170 6974 6e6f 3a22 5f20 645f 636f 5f5f 0a2c
0000180 2020 2020 2020 2020 7022 616c 6674 726f
0000190 736d 3a22 5b20 6122 796e 2c22 5d20 0a2c
00001a0 2020 2020 2020 2020 6c22 6369 6e65 6573
00001b0 3a22 2220 474c 4c50 3276 2c22 200a 2020
00001c0 2020 2020 2220 6170 6b63 6761 7365 3a22
00001d0 5b20 7722 6265 7473 726f 6761 4365 696c
00001e0 6e65 2274 202c 7722 6265 7473 726f 6761
00001f0 5365 7265 6576 2272 2c5d 200a 2020 2020
0000200 2020 2220 6373 6972 7470 2273 203a 225b
0000210 6962 2f6e 7377 6174 2e72 7970 2c22 2220
0000220 6962 2f6e 7377 6c63 6569 746e 702e 2279
0000230 202c 6222 6e69 662f 7473 726f 702e 2279
0000240 202c 6222 6e69 662f 6567 2e74 7970 5d22
0000250 0a2c 2020 2020 2020 2020 2023 614d 656b
0000260 7020 6361 616b 6567 2073 6e69 7220 6f6f
0000270 2074 6964 2072 7061 6570 7261 6920 206e
0000280 7970 6277 6d65 6d20 646f 6c75 0a65 2020
0000290 2020 2020 2020 7022 6361 616b 6567 645f
00002a0 7269 3a22 7b20 200a 2020 2020 2020 2020
00002b0 2020 2220 6577 7362 6f74 6172 6567 6c43
00002c0 6569 746e 3a22 2220 6c63 6569 746e 2c22
00002d0 200a 2020 2020 2020 2020 2020 2220 6577
00002e0 7362 6f74 6172 6567 6553 7672 7265 2022
00002f0 203a 7322 7265 6576 2272 200a 2020 2020
0000300 2020 2020 2020 7d20 0a2c 2020 2020 2020
0000310 2020 2023 614d 656b 6520 7478 6e65 6973
0000320 6e6f 2073 6e69 7220 6f6f 2074 6964 2072
0000330 7061 6570 7261 6920 206e 7970 6277 6d65
0000340 6d20 646f 6c75 0a65 2020 2020 2020 2020
0000350 2223 7865 5f74 6170 6b63 6761 2265 203a
0000360 7722 6265 7473 726f 6761 2265 0a2c 2020
0000370 2020 2020 2020 2023 6522 7478 6d5f 646f
0000380 6c75 7365 2022 203a 7963 6874 6e6f 7a69
0000390 2865 2a22 702e 7879 2922 0a2c 2020 2020
00003a0 2020 2020 7222 7165 6975 6572 2273 3a20
00003b0 5b20 7222 7165 6575 7473 2273 202c 2c5d
00003c0 200a 2020 2020 2020 2220 6576 7372 6f69
00003d0 226e 3a20 2220 2e32 2e30 2232 0a2c 2020
00003e0 2020 2020 2020 0a7d 6573 7574 2870 2a2a
00003f0 7261 7367 0a29                         
00003f6
```

## WebstorageArchive

Backend Webservice to store TAR like archives, providing checksums and metadata of files.
It is using FileStorage and BlockStorage under his hood to provide file and block deduplication.

## WebstorageClient

Python based client module to use BlockStorage and FileStorage very easy.

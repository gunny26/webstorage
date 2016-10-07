# webstorage
Webbased File/BlockStorage written in python

it is really imple to use, stores either whole files named with their sha1 checkum
or blocks of 1MB stored by their checksum.
the original filename is not stored ob WebStorage, so this ould be really anonymous.
It would be also possible to implement some sort of encryption to further improve
privacy.

Everthing a thief would find is a bunch of checksums without names.

FileStorage

for every file the sha1 checksum is created, so there is file deduplication implemented.
Everby 1MB Block of this file will be stored in BlockStorage.
In FileStorage every only the recipe or blockchain of the conatining blocks is stored.

BlockStorage

stores chunks ov max 1MB of data by their sha1 checksum


#!/usr/bin/python
import sys
import os
import time
import random
import hashlib
import json
import logging
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
import concurrent.futures
# own modules
from webstorage import ClientConfig as ClientConfig
from webstorage import BlockStorageClient as BlockStorageClient


datamap = {
    "europe" : { # region
        "home" : { # datacenter
            "cloud": { # server
                "pool1": ["https://cloud.messner.click/blockstorage", ]
                },
            "neutrino" : { # server
                "pool1": ["http://neutrino.messner.click/blockstorage", ]
            }
        },
        "frankfurt": { # datacenter
            "qualle": { # server
                "pool1" : ["http://qualle.messner.click/blockstorage", ]
            }
        }
    },
    "america" : { # region
        "mountainview": { # datacenter
            "krake": { # server
                "pool1" : ["http://krake.messner.click/blockstorage", ]
            }
        }
    }
}



def get_checksums(bs, filename, maxage=3600):
    if os.path.isfile(filename) and (os.stat(filename).st_mtime + maxage > time.time()):
        with open(filename, "rt") as infile:
            return json.load(infile)
    else:
        with open(filename, "wt") as outfile:
            checksums = bs.checksums
            json.dump(list(checksums), outfile)
            return checksums

if __name__ == "__main__":
    cc = ClientConfig()
    for config in cc.blockstorages:
        print(config)
    bs1_config = cc.blockstorages[0]
    bs1 = BlockStorageClient(url=bs1_config["url"], apikey=bs1_config["apikey"])
    print("found %d existing checksums in BlockStorage named %s" % (len(bs1.checksums), bs1_config["description"]))
    for checksum in get_checksums(bs1, "checksums.json"):
        regions = list(datamap.keys())
        region_index = (int(checksum[:2], 16) + 1) % len(regions) # +1 to prevent modulo by zero
        region = regions[region_index]
        datacenters = list(datamap[region])
        datacenter_index = (int(checksum[2:4], 16) + 1) % len(datacenters) # +1 to prevent modulo by zero
        datacenter = datacenters[datacenter_index]
        servers = list(datamap[region][datacenter])
        server_index = (int(checksum[4:6], 16) + 1) % len(servers) # +1 to prevent modulo by zero
        server = servers[server_index]
        pools = list(datamap[region][datacenter][server])
        pool_index = (int(checksum[6:8], 16) + 1) % len(pools) # +1 to prevent modulo by zero
        pool = pools[pool_index]
        #print("region_index = ", region_index)
        #print("will be placed in %s" % regions[region_index])
        print("%s\t%s\t%s\t%s\t%s" % (region, datacenter, server, pool, checksum))

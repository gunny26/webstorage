#!/usr/bin/python3

import requests
import pprint

HASH = "0d2c251dbf1bf3cf47da6d8118679995a979ad2a"
APIKEY =  "3bdd8789e84329e47becadb4430117c2d9465c5568ba2cde25f1cd77d9848fb7"
URL = "https://www.virustotal.com/vtapi/v2/file/report"

data = {"resource": HASH,
        "apikey": APIKEY
        }
res = requests.post(URL, data=data)
if res.status_code == 204:
    print("204 Public API Limit reached")
elif res.status_code == 403:
    print("403 Access denied")
else:
    print(res.status_code)
    res_dict = res.json()
    detected = dict(((scanner, result["detected"]) for scanner, result in res_dict["scans"].items()))
    pprint.pprint(detected)

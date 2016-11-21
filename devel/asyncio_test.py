#!/usr/bin/python3

import asyncio
import aiohttp
import bs4
import requests
import time

#@asyncio.coroutine
#def get(*args, **kwargs):
#    response = yield from aiohttp.request('GET', *args, **kwargs)
#    return (yield from response.read_and_close(decode=True))

session = requests.Session()

# @asyncio.coroutine
async def get(url):
    response = session.get(url)
    print(response.json())
    return response.status_code

async def print_magnet(checksum):
    url = 'https://www.messner.click/filestorage/{}'.format(checksum)
    page = await get(url)

def async_check():
    checksums = [
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    ]
    sem = asyncio.Semaphore(5)
    loop = asyncio.get_event_loop()
    f = asyncio.wait([print_magnet(checksum) for checksum in checksums])
    loop.run_until_complete(f)

def sync_check():
    checksums = [
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    "3842d6b537f4a062fb9191341b21181515fd3f0f",
    "18170cdd7f384c732b4f3df87a9fd746f20d9aba",
    "42f8c155d8a02857b9625e98acd9ee3ceca38ca0",
    "d3d80dd39b686e04bf31db6ac9335084e841ef73",
    "e68ab3c04ec547ff853c92a581832f6d33c79b00",
    "388d67c57cc874a4a999b310594fe59920033baf",
    ]
    for checksum in checksums:
        url = 'https://www.messner.click/filestorage/{}'.format(checksum)
        response = session.get(url)
        print(response.json())

if __name__ == "__main__":
    starttime = time.time()
    sync_check()
    print("Duration {}".format(time.time() - starttime))
    starttime = time.time()
    async_check()
    print("Duration {}".format(time.time() - starttime))
    starttime = time.time()
    sync_check()
    print("Duration {}".format(time.time() - starttime))
    starttime = time.time()
    async_check()
    print("Duration {}".format(time.time() - starttime))


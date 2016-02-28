#!/usr/bin/python
# -*- coding: utf-8 -*-
import json

a = u"Meßner"
print type(a), a
a_utf8 = a.encode("utf-8")
print type(a_utf8), a_utf8
a_utf8_json = json.dumps(a_utf8)
print type(a_utf8_json), a_utf8_json
b = json.loads(a_utf8_json)
print type(b), b
assert a == b

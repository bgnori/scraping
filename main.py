#!/usr/bin/env python


import requests


r = requests.get('http://jp.python-requests.org/en/latest/')
print(r.status_code)
print(r.headers['content-type'])
print(r.encoding)
print(r.text)


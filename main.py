#!/usr/bin/env python


import requests

def get(location):
    r = requests.get(location)
    print(r.status_code)
    print(r.headers['content-type'])
    print(r.encoding)
    print(r.text)

#'http://jp.python-requests.org/en/latest/')

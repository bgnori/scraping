#!/usr/bin/env python


import requests

from celery import Celery

celery = Celery('tasks', broker='amqp://guest@localhost//')


@celery.task
def get(location):
    r = requests.get(location)
    print(r.status_code)
    print(r.headers['content-type'])
    print(r.encoding)
    print(r.text)

#'http://jp.python-requests.org/en/latest/')

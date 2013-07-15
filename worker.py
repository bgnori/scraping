#!/usr/bin/env python


import requests

from celery import Celery

import models

celery = Celery('tasks', broker='amqp://guest@localhost//')


@celery.task
def get(location):

    r = requests.get(location)

    if r.status_code == 200:
        print(r.headers['content-type'])
        models.Pages.add(url=location, encoding=r.encoding, content=r.text)


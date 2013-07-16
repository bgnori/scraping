#!/usr/bin/env python


import lxml.html


import requests

from celery import Celery

import models

celery = Celery('tasks', broker='amqp://guest@localhost//')

celery.config_from_object('celeryconfig')

@celery.task
def get(location):
    r = requests.get(location)

    if r.status_code == 200:
        models.Pages.add(url=location, encoding=r.encoding, content=r.text)


@celery.task
def parse(page_id):

    page = models.Pages.from_id(page_id)

    t = lxml.html.fromstring(page.content)
    t.make_links_absolute(page.url)

    for elem, attr, link, pos in t.iterlinks(): 
        #print(elem, attr, link, pos)
        models.URLs.parse(link)


@celery.task
def fetch():
    with open('test', 'w') as f:
        print('fetch!!', file=f)



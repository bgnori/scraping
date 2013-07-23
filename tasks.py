#!/usr/bin/env python


import lxml.html


import requests

from celery import Celery

import models

celery = Celery('tasks', broker='amqp://guest@localhost//')

celery.config_from_object('celeryconfig')

from connection import hub

@celery.task
def get(location):
    r = requests.get(location)

    if r.status_code == 200:
        with hub.transaction():
            models.Pages.add(url=location, http_encoding=r.encoding,
                    content=r.content)
            u = models.URLs.parse(location)
            u.mark('Got')


@celery.task
def parse(page_id):
    with hub.transaction():
        page = models.Pages.from_id(page_id)
        t = lxml.html.fromstring(page.content)
        t.make_links_absolute(page.url)
        for elem, attr, link, pos in t.iterlinks(): 
            print(link)
            models.URLs.parse(link)

@celery.task
def fetch():
    with hub.transaction():
        u = models.URLs.head()
        assert u
        print(u.unparse())
        print(u.status)
        if u.authority_id == 1:
            get.delay(u.unparse())
            u.mark('Requested')
        else:
            u.mark('Ignored')


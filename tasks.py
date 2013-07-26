#!/usr/bin/env python


import lxml.html

import requests

from celery import Celery

import models

celery = Celery('tasks')
celery.config_from_object('celeryconfig')

from connection import hub

hub.connect()

from project import pat_maker

pat  = pat_maker()

@celery.task
def get(location):
    r = requests.get(location)

    if r.status_code == 200:
        with hub.transaction():
            p = models.Pages.add(url=location, http_encoding=r.encoding,
                    content=r.content)
            u = models.URLs.parse(location)
            u.mark('Got')
            parse.delay(p.id)


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
        url = u.unparse()
        print(url)
        if pat.match(url) and models.URLs.is_known(url):
            get.delay(u.unparse())
            u.mark('Requested')
        else:
            u.mark('Ignored')


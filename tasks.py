#!/usr/bin/env python


import lxml.html


import requests

from celery import Celery

import models

celery = Celery('tasks', broker='amqp://guest@localhost//')

celery.config_from_object('celeryconfig')

if True:
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.orm import scoped_session
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('sqlite:///./moebius.sqlite', poolclass=QueuePool)
    conn = engine.connect()
    models.get_session = scoped_session(sessionmaker(bind=conn, autocommit=True))

@celery.task
def get(location):
    r = requests.get(location)

    if r.status_code == 200:
        models.Pages.add(url=location, http_encoding=r.encoding,
                content=r.content)
        u = models.URLs.parse(location)
        u.mark('Got')


@celery.task
def parse(page_id):

    page = models.Pages.from_id(page_id)

    t = lxml.html.fromstring(page.content)
    t.make_links_absolute(page.url)

    def foo():
        for elem, attr, link, pos in t.iterlinks(): 
            yield link
    
    models.URLs.bulkparse(foo())


@celery.task
def fetch():
    u = models.URLs.head()
    assert u
    print(u.unparse())
    print(u.status)
    if u.authority_id == 1:
        get.delay(u.unparse())
        u.mark('Requested')
    else:
        u.mark('Ignored')


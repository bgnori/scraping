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

if __name__ == '__main__':
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.orm import scoped_session
    from sqlalchemy.orm import sessionmaker

    engine = create_engine('sqlite:///./moebius.sqlite', poolclass=QueuePool)
    conn = engine.connect()
    models.get_session = scoped_session(sessionmaker(bind=conn))
    get('http://jp.python-requests.org/en/latest/')


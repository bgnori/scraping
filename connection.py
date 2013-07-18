#!/usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import models

class ProcessSession(object):
    def __init__(self, uri=None):
        if uri is None:
            uri = 'sqlite:///./moebius.sqlite'
        self.engine = create_engine(uri, poolclass=QueuePool)
        self.conn = self.engine.connect()
        self.Session = sessionmaker(bind=self.conn, autocommit=False)

    def start(self):
        self.session = self.Session()

    def end(self):
        self.session.commit()
        self.session = None

    def get(self):
        assert self.session
        return self.session

_ps = ProcessSession()

start = _ps.start
end = _ps.end

models.get_session = _ps.get


#!/usr/bin/env python

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
import models

class Hub(object):
    _single = None
    def __new__(cls):
        if cls._single is None:
            cls._single = super(Hub, cls).__new__(cls)
        return cls._single

    def connect(self, uri=None):
        if uri is None:
            uri = 'sqlite:///./moebius.sqlite'
        self.engine = create_engine(uri, poolclass=QueuePool)
        self.conn = self.engine.connect()
        self.Session = sessionmaker(bind=self.conn, autocommit=False)
        self.session = None
        return self.conn

    @contextmanager
    def transaction(self):
        session = self.start()
        yield session
        self.end(session)

    def start(self):
        self.session = self.Session()
        return self.session

    def end(self, s):
        assert self.session == s
        self.session.commit()
        self.session = None

    def get(self):
        assert self.session
        return self.session

hub = Hub()

models.get_session = hub.get


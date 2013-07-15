#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relation, backref

get_session = None
"""
    User of this module MUST supply this.

    Most cases, it should be scoped session.
    http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#thread-local-scope
"""

class Schemes(Base):
    '''
        http://tools.ietf.org/html/rfc3986#section-3

         foo://example.com:8042/over/there?name=ferret#nose
         \_/   \______________/\_________/ \_________/ \__/
          |           |            |            |        |
       scheme     authority       path        query   fragment
    '''
    __tablename__ = 'Schemes'
    id = Column(Integer, primary_key=True)
    scheme = Column(String, nullable=False)

    @classmethod
    def add(cls, **kw):
        session = get_session()
        obj = cls(**kw)
        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(Schemes).get(made)

    @classmethod
    def get(self, s):
        session = get_session()
        return session.query(Schemes).get(s)

    @classmethod
    def have(cls, s):
        r = cls.get(s)
        if r is None:
            r = cls.add(scheme=s)
        return r


class Authorities(Base):
    '''
        http://tools.ietf.org/html/rfc3986#section-3

         foo://example.com:8042/over/there?name=ferret#nose
         \_/   \______________/\_________/ \_________/ \__/
          |           |            |            |        |
       scheme     authority       path        query   fragment
    '''
    __tablename__ = 'Authorities'
    id = Column(Integer, primary_key=True)
    host = Column(String, nullable=False)
    port = Column(String, nullable=True)

    @classmethod
    def add(cls, **kw):
        session = get_session()
        obj = cls(**kw)
        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(Authorities).get(made)

    @classmethod
    def get(cls, host, port):
        session = get_session()
        return session.query(Authorities).\
                filter(Authorities.host == host).\
                filter(Authorities.port == port).\
                scalar()

    @classmethod
    def have(cls, host, port):
        r = cls.get(host, port)
        if r is None:
            r = cls.add(host=host, port=port)
        return r


class URLs(Base):
    '''
        http://tools.ietf.org/html/rfc3986#section-3

         foo://example.com:8042/over/there?name=ferret#nose
         \_/   \______________/\_________/ \_________/ \__/
          |           |            |            |        |
       scheme     authority       path        query   fragment
    '''
    __tablename__ = 'URLs'
    id = Column(Integer, primary_key=True)
    authority = Column(Integer, ForeignKey(Authorities.id), nullable=False)
    scheme = Column(Integer, ForeignKey(Schemes.id), nullable=False)

    path = Column(String, nullable=False)
    query = Column(String, nullable=False)
    fragment = Column(String, nullable=False)
    
    @classmethod
    def add(cls, **kw):
        session = get_session()
        kw['scheme'] = Schemes.have(kw['scheme']).id
        kw['authority'] = Authorities.have(kw['host'], kw['port']).id
        del kw['host']
        del kw['port']

        obj = cls(**kw)
        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(URLs).get(made)


class Pages(Base):
    __tablename__ = 'Pages'
    id = Column(Integer, primary_key=True)
    url = Column(Integer, ForeignKey(URLs.id))
    got_at = Column(DateTime)
    content = Column(String)
    encoding = Column(String)


def create_all(conn):
    Base.metadata.create_all(conn)

def drop_all(conn):
    Base.metadata.drop_all(conn)


if __name__ == '__main__':
    import sys
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.orm import sessionmaker
    engine = create_engine('sqlite:///./moebius.sqlite', poolclass=QueuePool)
    Session = sessionmaker(bind=engine)

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
    else:
        cmd = 'show'
    s = Session()
    try:
        if cmd=='show':
            pass
        elif cmd=='create':
            create_all(s.connection())
        elif cmd=='drop':
            drop_all(s.connection())
        else:
            pass
    finally:
        s.close()



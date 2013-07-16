#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relation, backref, relationship


from urllib.parse import urlparse, urlunparse


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
        return session.query(Schemes).filter(Schemes.scheme==s).scalar()

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
    port = Column(Integer, nullable=True)

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
        if not host:
            return None
        r = cls.get(host, port)
        if r is None:
            r = cls.add(host=host, port=port)
        return r

    @property
    def as_netloc(self):
        if self.port:
            return '{}:{}'.format(self.host, self.port)
        return self.host


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
    scheme_id = Column(Integer, ForeignKey(Schemes.id), nullable=False)
    scheme_obj = relationship(Schemes, primaryjoin=scheme_id==Schemes.id)

    authority_id = Column(Integer, ForeignKey(Authorities.id), nullable=True)
    authority_obj = relationship(Authorities, primaryjoin=authority_id==Authorities.id)


    path = Column(String, nullable=False)
    params = Column(String, nullable=False) #not for http.
    query = Column(String, nullable=False)
    fragment = Column(String, nullable=False)

    status = Column(Integer, nullable=False)
    
    @classmethod
    def add(cls, scheme, host, port, path, params, query, fragment):
        session = get_session()
        kw = dict(path=path, params=params, query=query, fragment=fragment)
        kw['scheme_id'] = Schemes.have(scheme).id

        au = Authorities.have(host, port)
        if au is not None:
            kw['authority_id'] = au.id
        kw['status'] = 0

        obj = cls(**kw)
        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(URLs).get(made)

    @classmethod
    def get(cls, scheme, host, port, path, params, query, fragment):
        session = get_session()
        scheme = Schemes.get(scheme)
        au = Authorities.get(host, port)

        return session.query(URLs).\
                filter(URLs.scheme == scheme).\
                filter(URLs.authority == au).\
                filter(URLs.path == path).\
                filter(URLs.params == params).\
                filter(URLs.query == query).\
                filter(URLs.fragment == fragment).\
                scalar()

    @classmethod
    def have(cls, scheme, host, port, path, params, query, fragment):
        r = cls.get(scheme, host, port, path, params, query, fragment)
        if r is None:
            r = cls.add(scheme, host, port, path, params, query, fragment)
        return r

    @property
    def authority(self):
        return self.authority_obj.as_netloc
    
    @property
    def scheme(self):
        return self.scheme_obj.scheme

    @classmethod
    def parse(cls, s):
        ''' we do not expect encoding, that means byte, not string'''
        r = urlparse(s)
        return cls.have(r.scheme, r.hostname, r.port, r.path, r.params, r.query, r.fragment)

    def unparse(self):
        ''' we do not expect encoding, that means byte, not string'''
        return urlunparse((self.scheme, self.authority, self.path, self.params, self.query, self.fragment))

    @classmethod
    def head(self):
        session = get_session()
        q = session.query(URLs).\
                filter_by(status=0).\
                filter(URLs.authority_id != None).\
                filter(URLs.scheme_id != None).\
                filter(URLs.scheme_id == 1).\
                limit(1)
        return q.scalar()

    def mark(self, n):
        session = get_session()
        self.status = n
        session.add(self)
        session.commit()


class Pages(Base):
    __tablename__ = 'Pages'
    id = Column(Integer, primary_key=True)
    url_id = Column(Integer, ForeignKey(URLs.id))
    url_obj = relationship(URLs, primaryjoin=url_id==URLs.id)

    got_at = Column(DateTime)
    content = Column(String)
    encoding = Column(String)

    @classmethod
    def add(cls, **kw):
        session = get_session()
        url = URLs.parse(kw['url'])
        del kw['url']
        kw['url_obj'] = url

        obj = cls(**kw)
        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(Pages).get(made)

    @classmethod
    def from_id(cls, id):
        session = get_session()
        return session.query(Pages).get(id)

    @property
    def url(self):
        return self.url_obj.unparse()



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



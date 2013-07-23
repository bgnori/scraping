#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relation, backref, relationship


from urllib.parse import urlparse, urlunparse
import hashlib


get_session = None
"""
    User of this module MUST supply this.

    Most cases, it should be scoped session.
    http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#thread-local-scope
"""

"""
Read
    http://docs.sqlalchemy.org/en/rel_0_8/orm/session.html#unitofwork-cascades
    to understand cascades
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
        return obj

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
        return obj

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


class URLStatus(Base):
    __tablename__ = 'URLStatus'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)

    kind = (
        dict(name='New', 
            description='Newly got from parsed page or specified by invoker.'),
        dict(name='Requested',
            description='sent to get, subject of retrieval.'),
        dict(name='Got',
            description='Got Page/Resource'),
        dict(name='Ignored',
            description='Igonred by scraping project'),
    )

    @classmethod
    def insert_predefines(cls, session):
        for x in cls.kind:
            obj = cls(**x)
            session.add(obj)

    @classmethod
    def resolve(cls, name):
        session = get_session()
        return session.query(URLStatus).filter_by(name=name).scalar()


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
    scheme_id = Column(Integer, 
            ForeignKey(Schemes.id, onupdate='cascade'), nullable=False)
    scheme = relationship(Schemes, 
            primaryjoin="URLs.scheme_id==Schemes.id",
            cascade='all')

    authority_id = Column(Integer, 
            ForeignKey(Authorities.id, onupdate='cascade'), nullable=True)
    authority = relationship(Authorities, 
            primaryjoin="URLs.authority_id==Authorities.id",
            cascade='all')


    path = Column(String, nullable=False)
    params = Column(String, nullable=False) #not for http.
    query = Column(String, nullable=False)
    fragment = Column(String, nullable=False)

    status_id = Column(Integer,
            ForeignKey(URLStatus.id, onupdate='cascade'), 
            default=lambda: URLStatus.resolve('New'),
            nullable=False)
    status = relationship(URLStatus,
            primaryjoin="URLs.status_id==URLStatus.id",
            cascade='all')
    

    _cache = {}

    @classmethod
    def status_by_name(cls, name):
        r = URLStatus.resolve(name)
        return r

    def obtained(self):
        session = get_session()
        self.status = self.status_by_name('Got')
        session.add(self)

    @classmethod
    def add(cls, scheme, authority, path, params, query, fragment):
        session = get_session()
        obj = cls(
            scheme=scheme,
            authority=authority if authority else None,
            status=cls.status_by_name('New'),
            path=path, params=params, query=query, fragment=fragment)
        session.add(obj)
        return obj


    @classmethod
    def get(cls, scheme, authority, path, params, query, fragment):
        assert scheme

        session = get_session()
        query = session.query(URLs).\
                filter(URLs.scheme_id == scheme.id).\
                filter(URLs.path == path).\
                filter(URLs.params == params).\
                filter(URLs.query == query).\
                filter(URLs.fragment == fragment)

        if authority:
            query = query.filter(URLs.authority_id == authority.id)

        found = query.scalar()
        return found

    @classmethod
    def have(cls, scheme, host, port, path, params, query, fragment):
        scheme = Schemes.have(scheme)
        authority = Authorities.have(host, port)

        r = cls.get(scheme, authority, path, params, query, fragment)
        if not r:
            r = cls.add(scheme, authority, path, params, query, fragment)
        return r

    
    @classmethod
    def bulkparse(cls, xs):
        ''' we do not expect encoding, that means byte, not string'''
        session = get_session()
        for s in xs:
            r = urlparse(s)
        
            scheme = session.query(Schemes).filter(Schemes.scheme==r.scheme).scalar()
            if scheme is None:
                scheme = Schemes(scheme=r.scheme)
                session.add(scheme)

            authority = session.query(Authorities).\
                filter(Authorities.host == r.hostname).\
                filter(Authorities.port == r.port).\
                scalar()
            if authority is None and r.hostname is not None:
                authority = Authorities(host=r.hostname, port=r.port)
                session.add(authority)

            query = session.query(URLs).\
                filter(URLs.scheme_id == scheme.id).\
                filter(URLs.path == r.path).\
                filter(URLs.params == r.params).\
                filter(URLs.query == r.query).\
                filter(URLs.fragment == r.fragment)

            if authority:
                query = query.filter(URLs.authority_id == authority.id)

            found = query.scalar()

            if found is None:
                found = cls(scheme=scheme,
                        authority=authority if authority else None,
                        path=r.path, 
                        params=r.params,
                        query=r.query,
                        fragment=r.fragment,
                        status=cls.status_by_name('New'))
                session.add(found)



    @classmethod
    def parse(cls, s):
        ''' we do not expect encoding, that means byte, not string'''
        r = urlparse(s)
        return cls.have(r.scheme, r.hostname, r.port, r.path, r.params, r.query, r.fragment)

    def unparse(self):
        ''' we do not expect encoding, that means byte, not string'''
        return urlunparse((self.scheme.scheme, 
            self.authority.as_netloc, self.path, self.params, self.query, self.fragment))

    @classmethod
    def head(cls):
        session = get_session()
        q = session.query(URLs).\
                filter_by(status=cls.status_by_name('New')).\
                filter(URLs.authority != None).\
                filter(URLs.scheme != None).\
                filter(Schemes.scheme == 'http').\
                limit(1)
        return q.scalar()

    def mark(self, name):
        session = get_session()
        self.status = self.status_by_name(name)
        session.add(self)


class Pages(Base):
    __tablename__ = 'Pages'
    id = Column(Integer, primary_key=True)
    url_id = Column(Integer, ForeignKey(URLs.id))
    url_obj = relationship(URLs, primaryjoin=url_id==URLs.id)

    got_at = Column(DateTime)
    content = Column(String)
    http_encoding = Column(String)
    sha1hash = Column(String)

    @classmethod
    def add(cls, **kw):
        session = get_session()
        url = URLs.parse(kw['url'])
        del kw['url']
        kw['url_obj'] = url

        m = hashlib.sha1()
        m.update(kw['content'])
        kw['sha1hash'] = m.digest()

        obj = cls(**kw)
        session.add(obj)
        return obj

    @property
    def url(self):
        return self.url_obj.unparse()

    @classmethod
    def from_id(cls, page_id):
        session = get_session()
        return session.query(Pages).filter(Pages.id == page_id).scalar()


def create_all(session):
    Base.metadata.create_all(s.connection())
    URLStatus.insert_predefines(session)
    session.commit()

def drop_all(session):
    Base.metadata.drop_all(s.connection())


if __name__ == '__main__':
    import sys
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool
    from sqlalchemy.orm import sessionmaker
    engine = create_engine('sqlite:///./moebius.sqlite', poolclass=QueuePool)
    Session = sessionmaker(bind=engine)
    
    get_session = Session

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
    else:
        cmd = 'show'
    s = Session()
    try:
        if cmd=='show':
            pass
        elif cmd=='create':
            create_all(s)
        elif cmd=='drop':
            drop_all(s)
        else:
            pass
    finally:
        s.close()



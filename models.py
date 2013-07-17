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
    def insert_predefines(cls):
        session = get_session()
        for x in cls.kind:
            obj = cls(**x)
            session.add(obj)
        session.commit()

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
    scheme_id = Column(Integer, ForeignKey(Schemes.id), nullable=False)
    scheme_obj = relationship(Schemes, primaryjoin=scheme_id==Schemes.id)

    authority_id = Column(Integer, ForeignKey(Authorities.id), nullable=True)
    authority_obj = relationship(Authorities, primaryjoin=authority_id==Authorities.id)


    path = Column(String, nullable=False)
    params = Column(String, nullable=False) #not for http.
    query = Column(String, nullable=False)
    fragment = Column(String, nullable=False)

    status_id = Column(Integer, ForeignKey(URLStatus.id), nullable=False)
    status_obj = relationship(URLStatus, primaryjoin=status_id==URLStatus.id)
    

    _cache = {}

    @classmethod
    def status_id_by_name(cls, name):
        r = cls._cache.get(name, None)
        if r is None:
            r = URLStatus.resolve(name).id
            cls._cache[name] = r
        return r

    def obtained(self):
        session = get_session()
        self.status_id = self.status_id_by_name('Got')
        session.add(self)
        session.commit()

    @classmethod
    def add(cls, scheme, authority, path, params, query, fragment):
        session = get_session()
        obj = cls(
            scheme_id=scheme.id,
            authority_id= authority.id if authority else None,
            status_id=cls.status_id_by_name('New'),
            path=path, params=params, query=query, fragment=fragment)


        session.add(obj)
        session.commit()
        made = obj.id
        return session.query(URLs).get(made)



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

    @property
    def authority(self):
        return self.authority_obj.as_netloc
    
    @property
    def scheme(self):
        return self.scheme_obj.scheme

    @property
    def status(self):
        return self.status_obj.name

    @classmethod
    def parse(cls, s):
        ''' we do not expect encoding, that means byte, not string'''
        r = urlparse(s)
        return cls.have(r.scheme, r.hostname, r.port, r.path, r.params, r.query, r.fragment)

    def unparse(self):
        ''' we do not expect encoding, that means byte, not string'''
        return urlunparse((self.scheme, self.authority, self.path, self.params, self.query, self.fragment))

    @classmethod
    def head(cls):
        session = get_session()
        q = session.query(URLs).\
                filter_by(status_id=cls.status_id_by_name('New')).\
                filter(URLs.authority_id != None).\
                filter(URLs.scheme_id != None).\
                filter(URLs.scheme_id == 1).\
                limit(1)
        return q.scalar()

    def mark(self, name):
        session = get_session()
        self.status_id = self.status_id_by_name(name)
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
    sha1hash = Column(String)

    @classmethod
    def add(cls, **kw):
        session = get_session()
        url = URLs.parse(kw['url'])
        del kw['url']
        kw['url_obj'] = url

        m = hashlib.sha1()
        m.update(bytes(kw['content'], encoding=kw['encoding']))
        kw['sha1hash'] = m.digest()

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
    URLStatus.insert_predefines()

def drop_all(conn):
    Base.metadata.drop_all(conn)


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
            create_all(s.connection())
        elif cmd=='drop':
            drop_all(s.connection())
        else:
            pass
    finally:
        s.close()



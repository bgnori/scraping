#!/usr/bin/env python


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
    '''
    __tablename__ = 'Schemes'
    id = Column(Integer, primary_key=True)
    scheme = Column(String, nullable=False)


class Authorities(Base):
    '''
        http://tools.ietf.org/html/rfc3986#section-3
    '''
    __tablename__ = 'Authorities'
    id = Column(Integer, primary_key=True)
    authority = Column(String, nullable=False)


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
    


class Pages(Base):
    __tablename__ = 'Pages'
    id = Column(Integer, primary_key=True)
    url = Column(Integer, ForeignKey(URLs.id))
    got_at = Column(DateTime)
    content = Column(String)
    encoding = Column(String)


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
            Base.metadata.create_all(s.connection())
        elif cmd=='drop':
            Base.metadata.drop_all(s.connection())
        else:
            pass
    finally:
        s.close()




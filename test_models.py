#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from connection import hub
import models

class BasicTestCase(unittest.TestCase):
    def setUp(self):
        conn = hub.connect('sqlite:///:memory:')
        with hub.transaction():
            models.create_all(conn)

    def test_scheme_add_get(self):
        with hub.transaction():
            c = models.Schemes.add(scheme='http')

        self.assertEqual('http', c.scheme)
        
        with hub.transaction() as s:
            self.assertEqual(1, len(list(s.query(models.Schemes))))


    def test_authorities_add(self):
        with hub.transaction() as s:
            a = models.Authorities.add(host='example.com', port='8042')

        with hub.transaction() as s:
            self.assertEqual(1, len(list(s.query(models.Authorities))))

    def test_urls_add(self):
        with hub.transaction() as s:
            c = models.Schemes.add(scheme='foo')
            a = models.Authorities.add(host='example.com', port='8042')
            u = models.URLs.add(scheme=c, authority=a,
                path='/over/there', params='', query='name=ferret', fragment='nose')

        with hub.transaction() as s:
            self.assertEqual(1, len(list(s.query(models.Schemes))))
            self.assertEqual(1, len(list(s.query(models.Authorities))))
            self.assertEqual(1, len(list(s.query(models.URLs))))
            self.assertEqual('foo://example.com:8042/over/there?name=ferret#nose', u.unparse())

    def test_pages_add(self):
        with hub.transaction() as s:
            a = models.Pages.add(
                url="foo://example.com:8042/over/there?name=ferret#nose",
                http_encoding='utf-8',
                content=b'foobarbuzz')

        with hub.transaction() as s:
            self.assertEqual(1, len(list(s.query(models.Schemes))))
            self.assertEqual(1, len(list(s.query(models.Authorities))))
            self.assertEqual(1, len(list(s.query(models.URLs))))
            self.assertEqual(1, len(list(s.query(models.Pages))))
            self.assertEqual(b'foobarbuzz', a.content)
            self.assertEqual('utf-8', a.http_encoding)



if __name__ == '__main__':
    unittest.main()

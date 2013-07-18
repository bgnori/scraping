#!/usr/bin/env python


from tasks import get
import connection

connection.start()
get('http://jp.python-requests.org/en/latest/')
connection.end()



#!/usr/bin/env python

from tasks import parse
import connection


connection.start()
parse(1)
connection.end()


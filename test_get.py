#!/usr/bin/env python

from tasks import get
from connection import hub
import models

hub.connect()

loc = 'http://jp.python-requests.org/en/latest/'
with hub.transaction():
    models.URLs.parse(loc)
get(loc)


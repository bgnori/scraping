#!/usr/bin/env python

from tasks import parse
from connection import hub

hub.connect()
parse(1)


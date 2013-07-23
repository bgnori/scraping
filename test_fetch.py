#!/usr/bin/env python

from tasks import fetch 
from connection import hub

hub.connect()

fetch()


#!/usr/bin/env python


from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker

from tasks import parse
import models

engine = create_engine('sqlite:///./moebius.sqlite', poolclass=QueuePool)
conn = engine.connect()
models.get_session = scoped_session(sessionmaker(bind=conn))
parse(1)


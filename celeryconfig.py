#!/usr/bin/env python


from datetime import timedelta

CELERY_TIMEZONE = 'UTC'

## Broker settings.
BROKER_URL = "amqp://guest:guest@localhost:5672//"

# List of modules to import when celery starts.
CELERY_IMPORTS = ("worker", )

## Using the database to store task state and results.
CELERY_RESULT_BACKEND = "database"
CELERY_RESULT_DBURI = "sqlite:///./celery.db"

CELERY_ANNOTATIONS = {"tasks.add": {"rate_limit": "10/s"}}

CELERYBEAT_SCHEDULE = {
    'get-every-30-seconds': {
        'task': 'worker.fetch',
        'schedule': timedelta(seconds=30),
        'args': ()
    },
}


import os
import logging
from celery import Celery

backend = os.getenv('CELERY_BACKEND_URL', 'amqp')
celery = Celery('tasks', backend=backend)

celery.conf.update(
    CELERY_RESULT_SERIALIZER='json',
    CELERY_ENABLE_UTC=True
)


@celery.task
def add(x, y):
    return x + y


@celery.task
def sleep(x):
    time.sleep(x)
    return x


@celery.task
def time():
    import time
    return time.time()


@celery.task
def error(msg):
    raise Exception(msg)


@celery.task
def echo(msg):
    return msg


@celery.task
def send_email(to='me@example.com', title='hi'):
    logging.info("Sending email to '%s' with title '%s'" % (to, title))

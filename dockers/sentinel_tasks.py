import os
import logging
from celery import Celery

celery = Celery('tasks')

celery.conf.broker_url = os.environ.get('BROKER_URL')
celery.conf.broker_transport_options = {
    'master_name': os.environ.get('MASTER_NAME')
}
celery.conf.result_backend = os.environ.get('BACKEND_URL')

celery.conf.update(
    result_serializer='json',
    enable_utc=True
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

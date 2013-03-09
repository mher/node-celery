from celery import Celery


celery = Celery('tasks', broker='amqp://')

celery.conf.update(
        CELERY_RESULT_BACKEND = "amqp",
        CELERY_RESULT_SERIALIZER='json',
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


if __name__ == "__main__":
    celery.start()

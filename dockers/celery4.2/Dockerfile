FROM python:3.5-slim

RUN groupadd user && useradd --create-home --home-dir /home/user -g user user
WORKDIR /home/user

RUN pip install redis

ENV CELERY_VERSION 4.1

RUN pip install celery=="$CELERY_VERSION"

RUN { \
	echo 'import os'; \
	echo "broker_url = os.environ.get('BROKER_URL', 'sentinel://sentinel:26379/')"; \
  echo "broker_transport_options = { 'master_name': os.environ.get('MASTER_NAME', 'sentinelTest') }"; \
	echo "backend_url = os.environ.get('BACKEND_URL', 'redis://redis/0')"; \
} > celeryconfig.py

# --link some-rabbit:rabbit "just works"
# ENV BROKER_URL amqp://guest@rabbit

USER user
CMD ["celery", "worker"]

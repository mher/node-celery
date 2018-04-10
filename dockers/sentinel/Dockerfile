FROM redis

EXPOSE 26379
COPY sentinel.conf /etc/redis/sentinel.conf
RUN chown redis:redis /etc/redis/sentinel.conf
ENV SENTINEL_QUORUM 2
ENV SENTINEL_NAME sentinelTest
ENV SENTINEL_DOWN_AFTER 30000
ENV SENTINEL_FAILOVER 180000
COPY sentinel-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["sentinel-entrypoint.sh"]

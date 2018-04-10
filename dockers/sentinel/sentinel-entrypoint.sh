#!/bin/sh

sed -i "s/\$SENTINEL_QUORUM/$SENTINEL_QUORUM/g" /etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_DOWN_AFTER/$SENTINEL_DOWN_AFTER/g" /etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_FAILOVER/$SENTINEL_FAILOVER/g" /etc/redis/sentinel.conf
sed -i "s/\$SENTINEL_NAME/$SENTINEL_NAME/g" /etc/redis/sentinel.conf
sed -i "s/\$HOST_IP/$HOST_IP/g" /etc/redis/sentinel.conf

exec docker-entrypoint.sh redis-server /etc/redis/sentinel.conf --sentinel

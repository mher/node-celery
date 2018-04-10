#!/bin/bash
set -ev

pushd `dirname $0`

# Get external IP of localhost
IP=`ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -Eo '([0-9]*\.){3}[0-9]*' | grep -v '127.0.0.1'`
echo $IP

echo "EXTERNAL_HOST=$IP" > .env

# Start the services.
docker-compose up -d --build
docker-compose ps

popd

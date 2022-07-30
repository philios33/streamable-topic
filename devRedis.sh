#!/bin/bash

cd "$(dirname "$0")"

docker run --rm --name test-redis -p 127.0.0.1:6379:6379 -v "$(pwd)"/redis-data:/data redis

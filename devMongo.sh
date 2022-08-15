#!/bin/bash

cd "$(dirname "$0")"

docker run --rm --name test-mongo -p 127.0.0.1:27017:27017 -v "$(pwd)"/mongo-data/db:/data/db mongo:4.4

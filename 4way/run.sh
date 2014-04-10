#!/bin/bash

PORT=$1
if [ "$PORT" == "" ]; then
  echo "Missing port argument, can not run"
  exit 1
fi

java -Xmx512M -Xms512M -XX:-UseParallelOldGC \
  -Ddw.http.port=$PORT \
  -Ddw.http.adminPort=$PORT \
  -jar ../../ts-server/target/transistore-server-0.11.0-SNAPSHOT.jar \
  server ../../sample/four-nodes-4copies.yml

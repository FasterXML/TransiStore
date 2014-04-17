#!/bin/bash

PORT=$1
if [ "$PORT" == "" ]; then
  echo "Missing port argument, can not run"
  exit 1
fi

java -Xmx1024M -Xms1024M -XX:-UseParallelOldGC \
  -Ddw.server.connector.port=$PORT \
  -jar ../../ts-server/target/transistore-server-0.11.0-SNAPSHOT.jar \
  server ../../sample/two-nodes-7070-9090.yml

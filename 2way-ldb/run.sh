#!/bin/bash

PORT=$1
if [ "$PORT" == "" ]; then
  echo "Missing port argument, can not run"
  exit 1
fi

java -Xmx1024M -Xms1024M -XX:-UseParallelOldGC \
  -Ddw.http.port=$PORT \
  -Ddw.http.adminPort=$PORT \
  -jar ../../ts-server/target/transistore-server-0.9.3-SNAPSHOT.jar \
  server ../../sample/two-leveldb-7070-9090.yml

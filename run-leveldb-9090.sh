#!/bin/sh
java -Xmx1024M -Xms1024M -XX:-UseParallelOldGC \
  -Ddw.http.port=9090 \
  -Ddw.http.adminPort=9090 \
  -jar ts-server/target/transistore-server-0.9.3-SNAPSHOT.jar \
  server ./sample/single-leveldb-9090.yml

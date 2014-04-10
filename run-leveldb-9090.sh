#!/bin/sh
java -Xmx1024M -Xms1024M -Xmn512M -XX:-UseParallelOldGC \
  -Ddw.server.connector.port=9090 \
  -jar ts-server/target/transistore-server-0.11.0-SNAPSHOT.jar \
  server ./sample/single-leveldb-9090.yml

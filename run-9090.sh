#!/bin/sh
java -Xmx1024M -Xms1024M -XX:-UseParallelOldGC \
  -Ddw.http.port=9090 \
  -Ddw.http.adminPort=9090 \
  -jar ts-server/target/transistore-server-0.6.0-SNAPSHOT.jar \
  server ./sample/single-node-8080.yml

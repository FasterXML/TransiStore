#!/bin/sh
java -Xmx1024M -Xms1024M -Xmn512M -XX:-UseParallelOldGC \
  -Xrunhprof:cpu=samples,depth=15,verbose=n,interval=2 \
  -Ddw.http.port=9090 \
  -Ddw.http.adminPort=9090 \
  -jar ts-server/target/transistore-server-0.9.8-SNAPSHOT.jar \
  server ./sample/single-node-9090.yml

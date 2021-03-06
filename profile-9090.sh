#!/bin/sh
java -Xmx1024M -Xms1024M -Xmn512M -XX:-UseParallelOldGC \
  -Xrunhprof:cpu=samples,depth=15,verbose=n,interval=2 \
  -Ddw.server.connector.port=9090 \
  -jar ts-server/target/transistore-server-0.11.0-SNAPSHOT.jar \
  server ./sample/single-node-9090.yml

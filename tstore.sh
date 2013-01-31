#!/bin/sh

# Note: single-node config is fine, since client bootstraps to find
# all configs there may be assuming 9090 is one of hosts!

CONFIG="sample/single-node-8080.yml"
java -cp ts-commands/target/transistore-commands-0.5.0-SNAPSHOT.jar \
  com.fasterxml.transistore.cmd.TStoreMain  $CONFIG $@


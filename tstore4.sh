#!/bin/sh

# NOTE: defines 4-node config just to allow bootstrapping regardless
# of which node is down

CONFIG=sample/four-nodes-4copies.yml

./ts-commands/target/transistore-commands-0.11.0-SNAPSHOT.jar \
 --config-file $CONFIG $@


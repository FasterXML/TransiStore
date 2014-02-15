#!/bin/sh

# Note: single-node config is fine, since client bootstraps to find
# all configs there may be assuming 9090 is one of hosts!
# Also note that jar SHOULD be real executable (on unix anyway)

# let's actually use 4-node config, even for other clusters; more likely to succeed
# bootstrapping

CONFIG=sample/single-node-9090.yml
#CONFIG=sample/four-nodes-4copies.yml

./ts-commands/target/transistore-commands-0.9.9-SNAPSHOT.jar \
 --config-file $CONFIG $@


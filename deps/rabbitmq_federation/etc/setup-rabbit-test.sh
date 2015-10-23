#!/bin/sh -e
sh -e `dirname $0`/rabbit-test.sh "$DEPS_DIR/rabbit/scripts/rabbitmqctl -n $RABBITMQ_NODENAME"

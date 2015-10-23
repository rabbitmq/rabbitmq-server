#!/bin/sh -e
sh -e `dirname $0`/rabbit-test.sh "$RABBITMQCTL -n $RABBITMQ_NODENAME"

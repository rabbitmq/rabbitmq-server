#!/bin/sh -e
sh -e `dirname $0`/rabbit-test.sh "`dirname $0`/../../rabbitmq-server/scripts/rabbitmqctl -n rabbit-test"

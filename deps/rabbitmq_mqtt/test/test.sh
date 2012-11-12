#!/bin/sh
make -C `dirname $0` build_java_amqp
make -C `dirname $0` test

#!/bin/sh
$MAKE -C `dirname $0` build_java_amqp
$MAKE -C `dirname $0` test

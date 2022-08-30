#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$SCRIPT/../../../bin/stop-rabbitmq.sh
$SCRIPT/stop-uaa.sh

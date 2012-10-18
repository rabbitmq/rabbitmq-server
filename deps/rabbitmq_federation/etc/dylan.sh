#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream jessica '{"uri": "amqp://localhost:5676"}'
$CTL set_parameter policy ring '{"pattern": "^x$", "policy": {"federation-upstream-set": "all"}}'

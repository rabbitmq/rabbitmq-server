#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream jessica '{"uri": "amqp://localhost:5676"}'
$CTL set_policy ring "^x$" '{"federation-upstream-set": "all"}'

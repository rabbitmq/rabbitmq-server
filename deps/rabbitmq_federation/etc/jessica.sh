#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream dylan '{"uri": "amqp://localhost:5674"}'
$CTL set_parameter federation-upstream bugs '{"uri": "amqp://localhost:5675", "max-hops":2}'
$CTL set_policy ring "^x$" '{"federation-upstream-set": "all"}'

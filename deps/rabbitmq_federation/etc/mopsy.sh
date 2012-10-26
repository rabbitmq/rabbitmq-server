#!/bin/sh -e
CTL=$1

$CTL set_parameter federation-upstream cottontail '{"uri": "amqp://localhost:5676"}'
$CTL set_parameter federation-upstream-set ring '[{"upstream": "cottontail", "max-hops": 2}]'
$CTL set_policy ring "^ring$" '{"federation-upstream-set": "ring"}'


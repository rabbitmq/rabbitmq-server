#!/bin/sh -e
CTL=$1

$CTL set_parameter federation-upstream cottontail '{"uri": "amqp://localhost:5676", "max-hops": 2}'
$CTL set_policy ring "^ring$" '{"federation-upstream": "cottontail"}'


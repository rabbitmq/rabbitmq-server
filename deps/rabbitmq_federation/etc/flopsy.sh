#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream mopsy '{"uri": "amqp://localhost:5675"}'
$CTL set_parameter federation-upstream-set ring '[{"upstream": "mopsy", "max-hops": 2}]'
$CTL set_policy ring "^ring$" '{"federation-upstream-set": "ring"}'

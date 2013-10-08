#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream flopsy '{"uri": "amqp://localhost:5674", "max-hops": 2}'
$CTL set_policy ring "^ring$" '{"federation-upstream": "flopsy"}'

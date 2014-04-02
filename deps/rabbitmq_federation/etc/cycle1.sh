#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream cycle2 '{"uri": "amqp://localhost:5675", "max-hops": 99}'
$CTL set_policy cycle "^cycle$" '{"federation-upstream-set": "all"}'

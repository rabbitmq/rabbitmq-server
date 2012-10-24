#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream hare '{"uri": "amqp://localhost:5673"}'
$CTL set_parameter federation-upstream-set upstream '[{"upstream": "hare", "exchange": "upstream"}, {"upstream": "hare", "exchange": "upstream2"}]'
$CTL set_policy fed "^fed\." '{"federation-upstream-set": "upstream"}'


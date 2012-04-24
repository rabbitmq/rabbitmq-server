#!/bin/sh -e
CTL=$1

$CTL set_parameter federation_connection cottontail '[{<<"uri">>,<<"amqp://localhost:5676">>}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"cottontail">>},{<<"max_hops">>,2}]]'

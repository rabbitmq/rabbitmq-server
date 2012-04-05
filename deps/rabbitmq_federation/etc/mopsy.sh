#!/bin/sh -e
CTL=$1

$CTL set_parameter federation_connection cottontail '[{<<"host">>,<<"localhost">>},{<<"port">>,5676}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"cottontail">>},{<<"max_hops">>,2}]]'

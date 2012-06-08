#!/bin/sh -e
CTL=$1

$CTL set_parameter federation_connection cottontail '[{<<"uri">>,<<"amqp://localhost:5676">>}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"cottontail">>},{<<"max_hops">>,2}]]'
$CTL set_parameter policy ring '[{<<"prefix">>, <<"ring">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"ring">>}]}].'


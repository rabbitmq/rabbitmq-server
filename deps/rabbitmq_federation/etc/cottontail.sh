#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection flopsy '[{<<"uri">>,<<"amqp://localhost:5674">>}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"flopsy">>},{<<"max_hops">>,2}]]'
$CTL set_parameter policy ring '[{<<"prefix">>, <<"ring">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"ring">>}]}].'

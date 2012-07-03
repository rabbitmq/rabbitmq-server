#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection mopsy '[{<<"uri">>,<<"amqp://localhost:5675">>}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"mopsy">>},{<<"max_hops">>,2}]]'
$CTL set_parameter policy ring '[{<<"prefix">>, <<"ring">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"ring">>}]}].'

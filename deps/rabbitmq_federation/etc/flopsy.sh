#!/bin/sh -e
CTL=$1

$CTL set_parameter federation_connection mopsy '[{<<"host">>,<<"localhost">>},{<<"port">>,5675}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"mopsy">>},{<<"max_hops">>,2}]]'

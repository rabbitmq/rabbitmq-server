#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection flopsy '[{<<"host">>,<<"localhost">>},{<<"port">>,5674}]'
$CTL set_parameter federation_upstream_set ring '[[{<<"connection">>,<<"flopsy">>},{<<"max_hops">>,2}]]'

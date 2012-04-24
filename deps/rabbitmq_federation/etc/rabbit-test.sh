#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection localhost '[{<<"uri">>,<<"amqp://">>}]' # Test direct connections
$CTL set_parameter federation_connection local5673 '[{<<"uri">>,<<"amqp://localhost:5673">>}]'

$CTL set_parameter federation_upstream_set upstream '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream">>}]]'
$CTL set_parameter federation_upstream_set upstream12 '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream">>}],[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream2">>}]]'
$CTL set_parameter federation_upstream_set one '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"one">>}]]'
$CTL set_parameter federation_upstream_set two '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"two">>}]]'
$CTL set_parameter federation_upstream_set upstream5673 '[[{<<"connection">>,<<"local5673">>},{<<"exchange">>,<<"upstream">>}]]'

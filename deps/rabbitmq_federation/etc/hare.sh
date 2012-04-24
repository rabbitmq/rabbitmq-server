#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection hare '[{<<"uri">>,<<"amqp://localhost:5673">>}]'
$CTL set_parameter federation_upstream_set upstream '[[{<<"connection">>,<<"hare">>},{<<"exchange">>,<<"upstream">>}]]'

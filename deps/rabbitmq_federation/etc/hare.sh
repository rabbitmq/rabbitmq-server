#!/bin/sh
CTL=$1

$CTL set_parameter federation-upstream hare '[{<<"uri">>,<<"amqp://localhost:5673">>}]'
$CTL set_parameter federation-upstream-set upstream '[[{<<"upstream">>,<<"hare">>},{<<"exchange">>,<<"upstream">>}]]'
$CTL set_parameter policy fed '[{<<"prefix">>, <<"fed.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"upstream">>}]}].'

#!/bin/sh
CTL=$1

$CTL set_parameter federation_connection localhost '[{<<"uri">>,<<"amqp://">>}]' # Test direct connections
$CTL set_parameter federation_connection local5673 '[{<<"uri">>,<<"amqp://localhost:5673">>}]'

$CTL set_parameter federation_upstream_set upstream '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream">>}]]'
$CTL set_parameter federation_upstream_set upstream12 '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream">>}],[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"upstream2">>}]]'
$CTL set_parameter federation_upstream_set one '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"one">>}]]'
$CTL set_parameter federation_upstream_set two '[[{<<"connection">>,<<"localhost">>},{<<"exchange">>,<<"two">>}]]'
$CTL set_parameter federation_upstream_set upstream5673 '[[{<<"connection">>,<<"local5673">>},{<<"exchange">>,<<"upstream">>}]]'
$CTL set_parameter policy fed '[{<<"prefix">>, <<"fed.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"upstream">>}]}].'
$CTL set_parameter policy fed12 '[{<<"prefix">>, <<"fed12.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"upstream12">>}]}].'
$CTL set_parameter policy one '[{<<"prefix">>, <<"two">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"one">>}]}].'
$CTL set_parameter policy two '[{<<"prefix">>, <<"one">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"two">>}]}].'
$CTL set_parameter policy hare '[{<<"prefix">>, <<"hare.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"upstream5673">>}]}].'
$CTL set_parameter policy all '[{<<"prefix">>, <<"all.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"all">>}]}].'
$CTL set_parameter policy new '[{<<"prefix">>, <<"new.">>}, {<<"policy">>, [{<<"federation-upstream-set">>, <<"new-set">>}]}].'

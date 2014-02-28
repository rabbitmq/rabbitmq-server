#!/bin/sh
CTL=$1

curl -i -u guest:guest -H "content-type:application/json" \
    -XPUT -d'{"type":"x-consistent-hash","durable":true}' \
    http://localhost:15672/api/exchanges/%2f/sharding.test

$CTL set_parameter sharding-definition spn_test '{"routing-key": "1234"}'
$CTL set_parameter sharding shards-per-node "3"
$CTL set_policy spn_test   "^sharding\."   '{"sharding-definition": "spn_test"}'
$CTL clear_parameter sharding shards-per-node
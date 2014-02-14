#!/bin/sh
CTL=$1

curl -i -u guest:guest -H "content-type:application/json" \
    -XPUT -d'{"type":"x-consistent-hash","durable":true}' \
    http://localhost:15672/api/exchanges/%2f/three.ex

$CTL set_parameter sharding-definition 3_shard '{"shards-per-node": 3}'
$CTL set_policy 3_shard   "^three\."   '{"sharding-definition": "3_shard"}'

$CTL set_parameter sharding-definition 3_shard '{"shards-per-node": 5}'
$CTL set_policy 3_shard   "^three\."   '{"sharding-definition": "3_shard"}'
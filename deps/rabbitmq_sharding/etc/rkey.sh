#!/bin/sh
CTL=$1

curl -i -u guest:guest -H "content-type:application/json" \
    -XPUT -d'{"type":"x-consistent-hash","durable":true}' \
    http://localhost:15672/api/exchanges/%2f/rkey.ex

$CTL set_policy rkey-shard "^rkey\."   '{"shards-per-node": 2, "routing-key": "1234"}'

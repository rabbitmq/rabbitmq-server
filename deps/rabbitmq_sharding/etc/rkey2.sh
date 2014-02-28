#!/bin/sh
CTL=$1

$CTL set_parameter sharding routing-key '"4321"'
$CTL set_parameter sharding-definition rkey2 '{"guest", "shards-per-node": 2}'
$CTL set_policy rkey2-shard   "^rkey2\."   '{"sharding-definition": "rkey2"}'

#!/bin/sh
CTL=$1

$CTL set_parameter sharding-definition my_shard '{"sharded": true, "shards-per-node": 4}'
$CTL set_policy my_shard   "^shard\."   '{"sharding-definition": "my_shard"}'

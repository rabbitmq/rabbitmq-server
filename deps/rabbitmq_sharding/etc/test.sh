#!/bin/sh
CTL=$1

$CTL set_parameter sharding-definition my_shard '{"guest", "shards-per-node": 4}'
$CTL set_policy my_shard   "^shard\."   '{"sharding-definition": "my_shard"}'

-module(rabbit_mirror_queue_misc_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
  [
    default_max_sync_throughput
  ].

default_max_sync_throughput(_Config) ->
  ?assertEqual(
    0,
    rabbit_mirror_queue_misc:default_max_sync_throughput()),
  application:set_env(rabbit, mirroring_sync_max_throughput, 100),
  ?assertEqual(
    100,
    rabbit_mirror_queue_misc:default_max_sync_throughput()),
  application:set_env(rabbit, mirroring_sync_max_throughput, "100MiB"),
  ?assertEqual(
    100*1024*1024,
    rabbit_mirror_queue_misc:default_max_sync_throughput()),
  application:set_env(rabbit, mirroring_sync_max_throughput, "100MB"),
  ?assertEqual(
    100000000,
    rabbit_mirror_queue_misc:default_max_sync_throughput()),
  ok.

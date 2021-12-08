-module(rabbit_mirror_queue_sync_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
  [
    maybe_master_batch_send,
    get_time_diff
  ].

maybe_master_batch_send(_Config) ->
  SyncBatchSize = 4096,
  SyncThroughput = 2000,
  QueueLen = 10000,
  ?assertEqual(
    true, %% Message reach the last one in the queue
    rabbit_mirror_queue_sync:maybe_master_batch_send({[], 0, {0, 0, SyncThroughput}, {QueueLen, QueueLen}, 0}, SyncBatchSize)),
  ?assertEqual(
    true, %% # messages batched is less than batch size; and total message size has reached the batch size
    rabbit_mirror_queue_sync:maybe_master_batch_send({[], 0, {0, 0, SyncThroughput}, {SyncBatchSize, QueueLen}, 0}, SyncBatchSize)),
  TotalBytes0 = SyncThroughput + 1,
  Curr0 = 1,
  ?assertEqual(
    true,  %% Total batch size exceed max sync throughput
    rabbit_mirror_queue_sync:maybe_master_batch_send({[], 0, {TotalBytes0, 0, SyncThroughput}, {Curr0, QueueLen}, 0}, SyncBatchSize)),
  TotalBytes1 = 1,
  Curr1 = 1,
  ?assertEqual(
    false, %% # messages batched is less than batch size; and total bytes is less than sync throughput
    rabbit_mirror_queue_sync:maybe_master_batch_send({[], 0, {TotalBytes1, 0, SyncThroughput}, {Curr1, QueueLen}, 0}, SyncBatchSize)),
  ok.

get_time_diff(_Config) ->
  TotalBytes0 = 100,
  Interval0 = 1000, %% ms
  MaxSyncThroughput0 = 100,  %% bytes/s
  ?assertEqual(%% Used throughput = 100 / 1000 * 1000 = 100 bytes/s; matched max throughput
    0, %% => no need to pause queue sync
    rabbit_mirror_queue_sync:get_time_diff(TotalBytes0, Interval0, MaxSyncThroughput0)),

  TotalBytes1 = 100,
  Interval1 = 1000, %% ms
  MaxSyncThroughput1 = 200,  %% bytes/s
  ?assertEqual( %% Used throughput = 100 / 1000 * 1000 = 100 bytes/s; less than max throughput
    0, %% => no need to pause queue sync
    rabbit_mirror_queue_sync:get_time_diff(TotalBytes1, Interval1, MaxSyncThroughput1)),

  TotalBytes2 = 100,
  Interval2 = 1000, %% ms
  MaxSyncThroughput2 = 50,  %% bytes/s
  ?assertEqual( %% Used throughput = 100 / 1000 * 1000 = 100 bytes/s; greater than max throughput
    1000, %% => pause queue sync for 1000 ms
    rabbit_mirror_queue_sync:get_time_diff(TotalBytes2, Interval2, MaxSyncThroughput2)),
  ok.

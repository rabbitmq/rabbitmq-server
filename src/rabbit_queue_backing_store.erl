-module(rabbit_queue_backing_store).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [
     {init, 2},
     {delete_queue, 1},
     {fetch, 1},
     {is_empty, 1},
     {ack, 2},
     {publish_delivered, 2},
     {tx_publish, 2},
     {publish, 2},
     {requeue, 2},
     {tx_commit, 3},
     {tx_cancel, 2},
     {storage_mode, 1},
     {len, 1},
     {estimate_queue_memory_and_reset_counters, 1},
     {purge, 1},
     {set_storage_mode, 3},
     {maybe_prefetch, 1}
    ];
behaviour_info(_Other) ->
    undefined.

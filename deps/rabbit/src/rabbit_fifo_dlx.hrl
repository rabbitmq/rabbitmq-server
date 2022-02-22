-record(dlx_consumer,{
          %% We don't require a consumer tag because a consumer tag is a means to distinguish
          %% multiple consumers in the same channel. The rabbit_fifo_dlx_worker channel like process however
          %% creates only a single consumer to this quorum queue's discards queue.
          pid :: pid(),
          prefetch :: non_neg_integer(),
          checked_out = #{} :: #{msg_id() => tuple(
                                               rabbit_dead_letter:reason(),
                                               indexed_msg()
                                              )},
          next_msg_id = 0 :: msg_id()
         }).

-record(rabbit_fifo_dlx,{
          consumer = undefined :: #dlx_consumer{} | undefined,
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue(tuple(
                                                     rabbit_dead_letter:reason(),
                                                     indexed_msg()
                                                    )),
          %% Raft indexes of messages in both discards queue and dlx_consumer's checked_out map
          %% so that we get the smallest ra index in O(1).
          ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).

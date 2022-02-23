-record(dlx_consumer,{
          pid :: pid(),
          prefetch :: non_neg_integer(),
          checked_out = #{} :: #{msg_id() => tuple(rabbit_dead_letter:reason(), msg())},
          next_msg_id = 0 :: msg_id()
         }).

-record(rabbit_fifo_dlx,{
          consumer :: option(#dlx_consumer{}),
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue(tuple(rabbit_dead_letter:reason(), msg())),
          %% Raft indexes of messages in both discards queue and dlx_consumer's checked_out map
          %% so that we get the smallest ra index in O(1).
          ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).

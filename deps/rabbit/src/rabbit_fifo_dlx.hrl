-define(NIL, []).

%% At-least-once dead-lettering does not support reason 'maxlen'.
%% Reason of prefix messages is [] because the message will not be
%% actually delivered and storing 2 bytes in the persisted snapshot
%% is less than the reason atom.
-type reason() :: 'expired' | 'rejected' | delivery_limit | ?NIL.

% See snapshot scenarios in rabbit_fifo_prop_SUITE. Add dlx dehydrate tests.
-record(dlx_consumer,{
          %% We don't require a consumer tag because a consumer tag is a means to distinguish
          %% multiple consumers in the same channel. The rabbit_fifo_dlx_worker channel like process however
          %% creates only a single consumer to this quorum queue's discards queue.
          pid :: pid(),
          prefetch :: non_neg_integer(),
          checked_out = #{} :: #{msg_id() => {reason(), indexed_msg()}},
          next_msg_id = 0 :: msg_id() % part of snapshot data
         }).

-record(rabbit_fifo_dlx,{
          consumer = undefined :: #dlx_consumer{} | undefined,
          %% Queue of dead-lettered messages.
          discards = lqueue:new() :: lqueue:lqueue({reason(), indexed_msg()}),
          msg_bytes = 0 :: non_neg_integer(),
          msg_bytes_checkout = 0 :: non_neg_integer()
         }).

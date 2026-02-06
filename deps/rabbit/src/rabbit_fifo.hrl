%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% macros for memory optimised tuple structures
%% [A|B] saves 1 byte compared to {A,B}
-define(TUPLE(A, B), [A | B]).

%% We only hold Raft index and message header in memory.
%% Raw message data is always stored on disk.
-define(MSG(Index, Header), ?TUPLE(Index, Header)).

-define(C_MSG(Timeout, Msg), {Timeout, Msg}).
-define(C_MSG(Msg), {_, Msg}).
-define(NIL, []).

-define(IS_HEADER(H),
        (is_integer(H) andalso H >= 0) orelse
        is_list(H) orelse
        (is_map(H) andalso is_map_key(size, H))).

-define(DELIVERY_SEND_MSG_OPTS, [local, ra_event]).

%% constants for packed msg references where both the raft index and the size
%% is packed into a single immidate term
%%
%% 59 bytes as immedate ints are signed
-define(PACKED_MAX, 16#7FFF_FFFF_FFFF_FFF).
%% index bits - enough for 2000 days at 100k indexes p/sec
-define(PACKED_IDX_BITS, 44).
-define(PACKED_IDX_MAX, 16#FFFF_FFFF_FFF).
-define(PACKED_SZ_BITS, 15). %% size
-define(PACKED_SZ_MAX, 16#7FFF). %% 15 bits

-define(PACK(Idx, Sz),
        (Idx bxor (Sz bsl ?PACKED_IDX_BITS))).
-define(PACKED_IDX(PackedInt),
        (PackedInt band ?PACKED_IDX_MAX)).
-define(PACKED_SZ(PackedInt),
        ((PackedInt bsr 44) band 16#7FFF)).

-define(IS_PACKED(Int), (Int >= 0 andalso Int =< ?PACKED_MAX)).

-type optimised_tuple(A, B) :: nonempty_improper_list(A, B).

-type option(T) :: undefined | T.

-type raw_msg() :: term().
%% The raw message. It is opaque to rabbit_fifo.

-type msg_id() :: non_neg_integer().
%% A consumer-scoped monotonically incrementing integer included with a
%% {@link delivery/0.}. Used to settle deliveries using
%% {@link rabbit_fifo_client:settle/3.}

-type msg_seqno() :: non_neg_integer().
%% A sender process scoped monotonically incrementing integer included
%% in enqueue messages. Used to ensure ordering of messages send from the
%% same process

-type msg_header() :: msg_size() |
                      optimised_tuple(msg_size(), Expiry :: milliseconds()) |
                      #{size := msg_size(),
                        acquired_count => non_neg_integer(),
                        delivery_count => non_neg_integer(),
                        expiry => milliseconds()}.
%% The message header:
%% size: The size of the message payload in bytes.
%% delivery_count: The number of unsuccessful delivery attempts.
%%                 A non-zero value indicates a previous attempt.
%% return_count: The number of explicit returns.
%% expiry: Epoch time in ms when a message expires. Set during enqueue.
%%         Value is determined by per-queue or per-message message TTL.
%% If it contains only the size it can be condensed to an integer.
%% If it contains only the size and expiry it can be condensed to an improper list.

-type msg_size() :: non_neg_integer().
%% the size in bytes of the msg payload

%% 60 byte integer, immediate
-type packed_msg() :: 0..?PACKED_MAX.

-type msg() :: packed_msg() | optimised_tuple(ra:index(), msg_header()).

%% a consumer message
-type c_msg() :: {LockExpiration :: milliseconds(), msg()}.

-type delivery_msg() :: {msg_id(), {msg_header(), raw_msg()}}.
%% A tuple consisting of the message id, and the headered message.

-type delivery() :: {delivery, rabbit_types:ctag(), [delivery_msg()]}.
%% Represents the delivery of one or more rabbit_fifo messages.

-type consumer_id() :: {rabbit_types:ctag(), pid()}.
%% The entity that receives messages. Uniquely identifies a consumer.

-type consumer_idx() :: ra:index().
%% v4 can reference consumers by the raft index they were added at.
%% The entity that receives messages. Uniquely identifies a consumer.
-type consumer_key() :: consumer_id() | consumer_idx().

-type credit_mode() ::
    {credited, InitialDeliveryCount :: rabbit_queue_type:delivery_count()} |
    %% machine_version 2
    {simple_prefetch, MaxCredit :: non_neg_integer()}.
%% determines how credit is replenished

-type checkout_spec() :: {once | auto,
                          Num :: non_neg_integer(),
                          credited | simple_prefetch} |

                         {dequeue, settled | unsettled} |
                         cancel | remove |
                         %% new v4 format
                         {once | auto, credit_mode()}.

-type consumer_meta() :: #{ack => boolean(),
                           username => binary(),
                           prefetch => non_neg_integer(),
                           args => list(),
                           priority => 0..255,
                           timeout => milliseconds()
                          }.
%% static meta data associated with a consumer

-type applied_mfa() :: {module(), atom(), list()}.
% represents a partially applied module call

-define(CHECK_MIN_INTERVAL_MS, 1000).
-define(CHECK_MIN_INDEXES, 4096 * 2).
-define(CHECK_MAX_INDEXES, 666_667).
%% once these many bytes have been written since the last checkpoint
%% we request a checkpoint irrespectively
-define(CHECK_MAX_BYTES, 128_000_000).
-define(SNAP_OUT_BYTES, 64_000_000).

-define(USE_AVG_HALF_LIFE, 10000.0).
%% an average QQ without any message uses about 100KB so setting this limit
%% to ~10 times that should be relatively safe.
-define(GC_MEM_LIMIT_B, 2_000_000).

-define(MB, 1_048_576).
-define(LOW_LIMIT, 0.8).
-define(DELIVERY_CHUNK_LIMIT_B, 128_000).

-type seconds() :: non_neg_integer().
-type milliseconds() :: non_neg_integer().

-record(consumer_cfg,
        {meta = #{} :: consumer_meta(),
         pid :: pid(),
         tag :: rabbit_types:ctag(),
         %% the mode of how credit is incremented
         %% simple_prefetch: credit is re-filled as deliveries are settled
         %% or returned.
         %% credited: credit can only be changed by receiving a consumer_credit
         %% command: `{credit, ReceiverDeliveryCount, Credit}'
         credit_mode :: credited | credit_mode(),
         lifetime = once :: once | auto,
         priority = 0 :: integer(),
         timeout = 1_800_000 :: milliseconds()}).

-type consumer_status() :: up | cancelled | quiescing.

-record(consumer,
        {cfg = #consumer_cfg{},
         status = up :: consumer_status() |
                        {suspected_down, consumer_status()} |
                        %% a message has been pending for longer than the
                        %% consumer timeout
                        {timeout, consumer_status()},
         next_msg_id = 0 :: msg_id(),
         checked_out = #{} :: #{msg_id() => c_msg()},
         %% max number of messages that can be sent
         %% decremented for each delivery
         credit = 0 :: non_neg_integer(),
         %% AMQP 1.0 §2.6.7
         delivery_count :: rabbit_queue_type:delivery_count(),
         timed_out_msg_ids = [] :: [msg_id()],
         drain = false :: boolean()
        }).

-type consumer() :: #consumer{}.

-type consumer_strategy() :: competing | single_active.

-type dead_letter_handler() :: option({at_most_once, applied_mfa()} | at_least_once).

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         unused = ?NIL,
         status = up :: up | suspected_down,
         %% it is useful to have a record of when this was blocked
         %% so that we can retry sending the block effect if
         %% the publisher did not receive the initial one
         blocked :: option(ra:index()),
         unused_1 = ?NIL,
         unused_2 = ?NIL
        }).

-record(cfg,
        {name :: atom(),
         resource :: rabbit_types:r('queue'),
         unused_1 = ?NIL,
         dead_letter_handler :: dead_letter_handler(),
         become_leader_handler :: option(applied_mfa()),
         overflow_strategy = drop_head :: drop_head | reject_publish,
         max_length :: option(non_neg_integer()),
         max_bytes :: option(non_neg_integer()),
         %% whether single active consumer is on or not for this queue
         consumer_strategy = competing :: consumer_strategy(),
         %% the maximum number of unsuccessful delivery attempts permitted
         delivery_limit :: option(non_neg_integer()),
         expires :: option(milliseconds()),
         msg_ttl :: option(milliseconds()),
         %% time to wait before returning messages when consumer's node
         %% becomes unreachable
         consumer_disconnected_timeout = 60_000 :: milliseconds(),
         unused_3 = ?NIL
        }).

-record(messages,
        {
         messages = rabbit_fifo_pq:new() :: rabbit_fifo_pq:state(),
         messages_total = 0 :: non_neg_integer(),
         % queue of returned msg_in_ids - when checking out it picks from
         returns = lqueue:new() :: lqueue:lqueue(term())
        }).

-record(dlx_consumer,
        {pid :: pid(),
         prefetch :: non_neg_integer(),
         checked_out = #{} :: #{msg_id() =>
                                optimised_tuple(rabbit_dead_letter:reason(), msg())},
         next_msg_id = 0 :: msg_id()}).

-record(rabbit_fifo_dlx,
        {consumer :: option(#dlx_consumer{}),
         %% Queue of dead-lettered messages.
         discards = lqueue:new() :: lqueue:lqueue(optimised_tuple(rabbit_dead_letter:reason(), msg())),
         %% Raft indexes of messages in both discards queue and dlx_consumer's checked_out map
         %% so that we get the smallest ra index in O(1).
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         msg_bytes = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer()}).

-record(rabbit_fifo,
        {cfg :: #cfg{},
         % unassigned messages
         messages = rabbit_fifo_pq:new() :: rabbit_fifo_pq:state(),
         messages_total = 0 :: non_neg_integer(),
         % queue of returned msg_in_ids - when checking out it picks from
         returns = lqueue:new() :: lqueue:lqueue(term()),
         % Reclaimable bytes - a counter that is incremented every time a
         % command is processed that does not need to be kept (live indexes).
         % Approximate, used for triggering snapshots.
         % Reset to 0 when release_cursor gets stored.
         reclaimable_bytes = 0,
         % a map containing all the live processes that have ever enqueued
         % a message to this queue
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % index of all messages that have been delivered at least once
         % used to work out the smallest live raft index
         % rabbit_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations as it's backed by a map
         last_command_time = 0,
         next_consumer_timeout = infinity :: infinity | milliseconds(),
         % consumers need to reflect consumer state at time of snapshot
         consumers = #{} :: #{consumer_key() => consumer()},
         % consumers that require further service are queued here
         service_queue = priority_queue:new() :: priority_queue:q(),
         %% state for at-least-once dead-lettering
         dlx = #rabbit_fifo_dlx{} :: #rabbit_fifo_dlx{},
         msg_bytes_enqueue = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer(),
         %% one is picked if active consumer is cancelled or dies
         %% used only when single active consumer is on
         waiting_consumers = [] :: [{consumer_key(), consumer()}],
         %% records the timestamp whenever the queue was last considered
         %% active in terms of consumer activity
         last_active :: option(non_neg_integer()),
         msg_cache :: option({ra:index(), raw_msg()}),
         unused_2 = ?NIL
        }).

-type config() :: #{name := atom(),
                    queue_resource := rabbit_types:r('queue'),
                    dead_letter_handler => dead_letter_handler(),
                    become_leader_handler => applied_mfa(),
                    % checkpoint_min_indexes => non_neg_integer(),
                    % checkpoint_max_indexes => non_neg_integer(),
                    max_length => non_neg_integer(),
                    max_bytes => non_neg_integer(),
                    overflow_strategy => drop_head | reject_publish,
                    single_active_consumer_on => boolean(),
                    delivery_limit => non_neg_integer() | -1,
                    expires => non_neg_integer(),
                    msg_ttl => non_neg_integer(),
                    created => non_neg_integer(),
                    consumer_disconnected_timeout => milliseconds()
                   }.

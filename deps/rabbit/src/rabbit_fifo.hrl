%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% macros for memory optimised tuple structures
%% [A|B] saves 1 byte compared to {A,B}
-define(TUPLE(A, B), [A | B]).

%% We only hold Raft index and message header in memory.
%% Raw message data is always stored on disk.
-define(MSG(Index, Header), ?TUPLE(Index, Header)).

-define(NIL, []).

-define(IS_HEADER(H),
        (is_integer(H) andalso H >= 0) orelse
        is_list(H) orelse
        (is_map(H) andalso is_map_key(size, H))).

-define(DELIVERY_SEND_MSG_OPTS, [local, ra_event]).


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

-type consumer_filter() :: none |
                           {property, rabbit_amqp_filter:filter_expressions()} |
                           {jms, ParsedSelector :: term()}.

-type simple_type() :: atom() | binary() | number().

%% Message metadata held in memory for consumers to filter on.
-type msg_metadata() :: #{atom() | binary() => simple_type()}.

-type msg_header() :: msg_size() |
                      optimised_tuple(msg_size(), Expiry :: milliseconds()) |
                      #{size := msg_size(),
                        acquired_count => non_neg_integer(),
                        delivery_count => non_neg_integer(),
                        expiry => milliseconds(),
                        meta => msg_metadata()}.
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

-type msg() :: optimised_tuple(ra:index(), msg_header()).

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
                           priority => non_neg_integer(),
                           filter => consumer_filter()
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

-define(USE_AVG_HALF_LIFE, 10000.0).
%% an average QQ without any message uses about 100KB so setting this limit
%% to ~10 times that should be relatively safe.
-define(GC_MEM_LIMIT_B, 2_000_000).

-define(MB, 1_048_576).
-define(LOW_LIMIT, 0.8).
-define(DELIVERY_CHUNK_LIMIT_B, 128_000).

-type milliseconds() :: non_neg_integer().
-type timestamp() :: milliseconds().

-type counter() :: non_neg_integer().
-type return_counter() :: counter().

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
         filter = none :: consumer_filter()
        }).

-record(consumer,
        {cfg = #consumer_cfg{},
         status = up :: up | suspected_down | cancelled | quiescing,
         next_msg_id = 0 :: msg_id(),
         checked_out = #{} :: #{msg_id() => msg()},
         %% max number of messages that can be sent
         %% decremented for each delivery
         credit = 0 :: non_neg_integer(),
         %% AMQP 1.0 §2.6.7
         delivery_count :: rabbit_queue_type:delivery_count(),
         scanned_idxs = {0, 0} :: {HighPrio :: ra:index(),
                                   NormalPrio :: ra:index()},
         scanned_returns = 0 :: return_counter()
        }).

-type consumer() :: #consumer{}.

-type consumer_strategy() :: competing | single_active.

-type dead_letter_handler() :: option({at_most_once, applied_mfa()} | at_least_once).

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         % out of order enqueues - sorted list
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
         max_bytes_meta :: option(non_neg_integer()),
         %% whether single active consumer is on or not for this queue
         consumer_strategy = competing :: consumer_strategy(),
         %% the maximum number of unsuccessful delivery attempts permitted
         delivery_limit :: option(non_neg_integer()),
         expires :: option(milliseconds()),
         msg_ttl :: option(milliseconds()),
         filter_enabled :: boolean(),
         unused_2 = ?NIL,
         unused_3 = ?NIL
        }).

-record(rabbit_fifo,
        {cfg :: #cfg{},
         % unassigned messages
         messages = rabbit_fifo_q:new() :: rabbit_fifo_q:state() | rabbit_fifo_filter_q:state(),
         messages_total = 0 :: non_neg_integer(),
         % queue of returned msg_in_ids - when checking out it picks from
         returns = lqueue:new() :: lqueue:lqueue(term()) | gb_trees:tree(
                                                             return_counter(),
                                                             msg()),
         %% * only used if filtering is enabled
         %% * contains messages that are available and have a TTL set
         %%   (we do not expire acquired messages)
         filter_msgs_expiry = gb_trees:empty() :: gb_trees:tree(
                                                    {timestamp(), ra:index()},
                                                    %% reference into where this msg is stored
                                                    hi | no | return_counter()),
         % a counter of enqueues - used to trigger shadow copy points
         % reset to 0 when release_cursor gets stored
         enqueue_count = 0 :: counter(),
         return_count = 0 :: return_counter(),
         % a map containing all the live processes that have ever enqueued
         % a message to this queue
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % index of all messages that have been delivered at least once
         % used to work out the smallest live raft index
         % rabbit_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations as it's backed by a map
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         unused_1 = ?NIL,
         % consumers need to reflect consumer state at time of snapshot
         consumers = #{} :: #{consumer_key() => consumer()},
         %% TODO should only be used for filtering enabled queues.
         %% All active consumers including consumers that may be down or may contain 0 credits.
         %% An auxiliary data structure used as the base service queue after a new message has
         %% been enqueued.
         consumers_q = priority_queue:new() :: priority_queue:q(),
         % consumers that require further service are queued here
         service_queue = priority_queue:new() :: priority_queue:q(),
         %% state for at-least-once dead-lettering
         dlx = rabbit_fifo_dlx:init() :: rabbit_fifo_dlx:state(),
         msg_bytes_enqueue = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer(),
         %% one is picked if active consumer is cancelled or dies
         %% used only when single active consumer is on
         waiting_consumers = [] :: [{consumer_key(), consumer()}],
         last_active :: option(non_neg_integer()),
         msg_cache :: option({ra:index(), raw_msg()}),
         unused_2 = ?NIL
        }).

-type config() :: #{name := atom(),
                    queue_resource := rabbit_types:r('queue'),
                    dead_letter_handler => dead_letter_handler(),
                    become_leader_handler => applied_mfa(),
                    checkpoint_min_indexes => non_neg_integer(),
                    checkpoint_max_indexes => non_neg_integer(),
                    max_length => non_neg_integer(),
                    max_bytes => non_neg_integer(),
                    max_bytes_meta => non_neg_integer(),
                    overflow_strategy => drop_head | reject_publish,
                    single_active_consumer_on => boolean(),
                    filter_enabled => boolean(),
                    delivery_limit => non_neg_integer() | -1,
                    expires => non_neg_integer(),
                    msg_ttl => non_neg_integer(),
                    created => non_neg_integer()
                   }.

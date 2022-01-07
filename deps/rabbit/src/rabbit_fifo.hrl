
%% macros for memory optimised tuple structures
-define(TUPLE(A, B), [A | B]).

%% We want short atoms since their binary representations will get
%% persisted in a snapshot for every message.
%% '$d' stand for 'disk'.
-define(DISK_MSG_TAG, '$d').
%% '$m' stand for 'memory'.
-define(PREFIX_MEM_MSG_TAG, '$m').

% -define(DISK_MSG(Header), [Header | ?DISK_MSG_TAG]).
-define(DISK_MSG(Header), Header).
% -define(MSG(Header, RawMsg), [Header | RawMsg]).
-define(INDEX_MSG(Index, Msg), [Index | Msg]).
% -define(PREFIX_MEM_MSG(Header), [Header | ?PREFIX_MEM_MSG_TAG]).

-define(IS_HEADER(H),
          when is_integer(H) orelse
               (is_map(H) andalso is_map_key(size, H))).

-type option(T) :: undefined | T.

-type raw_msg() :: term().
%% The raw message. It is opaque to rabbit_fifo.

% a queue scoped monotonically incrementing integer used to enforce order
% in the unassigned messages map

-type msg_id() :: non_neg_integer().
%% A consumer-scoped monotonically incrementing integer included with a
%% {@link delivery/0.}. Used to settle deliveries using
%% {@link rabbit_fifo_client:settle/3.}

-type msg_seqno() :: non_neg_integer().
%% A sender process scoped monotonically incrementing integer included
%% in enqueue messages. Used to ensure ordering of messages send from the
%% same process

-type msg_header() :: msg_size() |
                      #{size := msg_size(),
                        delivery_count => non_neg_integer(),
                        expiry => milliseconds()}.
%% The message header:
%% delivery_count: the number of unsuccessful delivery attempts.
%%                 A non-zero value indicates a previous attempt.
%% expiry: Epoch time in ms when a message expires. Set during enqueue.
%%         Value is determined by per-queue or per-message message TTL.
%% If it only contains the size it can be condensed to an integer only

-type msg() :: %%?MSG(msg_header(), raw_msg()) |
               ?DISK_MSG(msg_header()).
               % ?PREFIX_MEM_MSG(msg_header()).
%% message with a header map.

-type msg_size() :: non_neg_integer().
%% the size in bytes of the msg payload

-type indexed_msg() :: ?INDEX_MSG(ra:index(), msg_header()).

% -type prefix_msg() :: {'$prefix_msg', msg_header()}.

-type delivery_msg() :: {msg_id(), {msg_header(), term()}}.
%% A tuple consisting of the message id and the headered message.

-type consumer_tag() :: binary().
%% An arbitrary binary tag used to distinguish between different consumers
%% set up by the same process. See: {@link rabbit_fifo_client:checkout/3.}

-type delivery() :: {delivery, consumer_tag(), [delivery_msg()]}.
%% Represents the delivery of one or more rabbit_fifo messages.

-type consumer_id() :: {consumer_tag(), pid()}.
%% The entity that receives messages. Uniquely identifies a consumer.

-type credit_mode() :: simple_prefetch | credited.
%% determines how credit is replenished

-type checkout_spec() :: {once | auto, Num :: non_neg_integer(),
                          credit_mode()} |
                         {dequeue, settled | unsettled} |
                         cancel.

-type consumer_meta() :: #{ack => boolean(),
                           username => binary(),
                           prefetch => non_neg_integer(),
                           args => list()}.
%% static meta data associated with a consumer


-type applied_mfa() :: {module(), atom(), list()}.
% represents a partially applied module call

-define(RELEASE_CURSOR_EVERY, 2048).
-define(RELEASE_CURSOR_EVERY_MAX, 3200000).
-define(USE_AVG_HALF_LIFE, 10000.0).
%% an average QQ without any message uses about 100KB so setting this limit
%% to ~10 times that should be relatively safe.
-define(GC_MEM_LIMIT_B, 2000000).

-define(MB, 1048576).
-define(LOW_LIMIT, 0.8).

-record(consumer,
        {meta = #{} :: consumer_meta(),
         checked_out = #{} :: #{msg_id() => indexed_msg()},
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         %% max number of messages that can be sent
         %% decremented for each delivery
         credit = 0 : non_neg_integer(),
         %% total number of checked out messages - ever
         %% incremented for each delivery
         delivery_count = 0 :: non_neg_integer(),
         %% the mode of how credit is incremented
         %% simple_prefetch: credit is re-filled as deliveries are settled
         %% or returned.
         %% credited: credit can only be changed by receiving a consumer_credit
         %% command: `{consumer_credit, ReceiverDeliveryCount, Credit}'
         credit_mode = simple_prefetch :: credit_mode(), % part of snapshot data
         lifetime = once :: once | auto,
         status = up :: up | suspected_down | cancelled,
         priority = 0 :: non_neg_integer()
        }).

-type consumer() :: #consumer{}.

-type consumer_strategy() :: competing | single_active.

-type milliseconds() :: non_neg_integer().

-type dead_letter_handler() :: option({at_most_once, applied_mfa()} | at_least_once).

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         % out of order enqueues - sorted list
         unused,
         status = up :: up | suspected_down,
         %% it is useful to have a record of when this was blocked
         %% so that we can retry sending the block effect if
         %% the publisher did not receive the initial one
         blocked :: undefined | ra:index(),
         unused_1,
         unused_2
        }).

-record(cfg,
        {name :: atom(),
         resource :: rabbit_types:r('queue'),
         release_cursor_interval :: option({non_neg_integer(), non_neg_integer()}),
         dead_letter_handler :: dead_letter_handler(),
         become_leader_handler :: option(applied_mfa()),
         overflow_strategy = drop_head :: drop_head | reject_publish,
         max_length :: option(non_neg_integer()),
         max_bytes :: option(non_neg_integer()),
         %% whether single active consumer is on or not for this queue
         consumer_strategy = competing :: consumer_strategy(),
         %% the maximum number of unsuccessful delivery attempts permitted
         delivery_limit :: option(non_neg_integer()),
         max_in_memory_length :: option(non_neg_integer()),
         max_in_memory_bytes :: option(non_neg_integer()),
         expires :: undefined | milliseconds(),
         msg_ttl :: undefined | milliseconds(),
         unused_1,
         unused_2
        }).

-type prefix_msgs() :: {list(), list()} |
                       {non_neg_integer(), list(),
                        non_neg_integer(), list()}.

-record(rabbit_fifo,
        {cfg :: #cfg{},
         % unassigned messages
         messages = lqueue:new() :: lqueue:lqueue(indexed_msg()),
         % defines the next message id
         messages_total = 0 :: non_neg_integer(),
         % queue of returned msg_in_ids - when checking out it picks from
         returns = lqueue:new() :: lqueue:lqueue(term()),
         % a counter of enqueues - used to trigger shadow copy points
         % reset to 0 when release_cursor gets stored
         enqueue_count = 0 :: non_neg_integer(),
         % a map containing all the live processes that have ever enqueued
         % a message to this queue as well as a cached value of the smallest
         % ra_index of all pending enqueues
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % master index of all enqueue raft indexes including pending
         % enqueues
         % rabbit_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations as it's backed by a map
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         %% A release cursor is essentially a snapshot without message bodies
         %% (aka. "dehydrated state") taken at time T in order to truncate
         %% the log at some point in the future when all messages that were enqueued
         %% up to time T have been removed (e.g. consumed, dead-lettered, or dropped).
         %% This concept enables snapshots to not contain any message bodies.
         %% Advantage: Smaller snapshots are sent between Ra nodes.
         %% Working assumption: Messages are consumed in a FIFO-ish order because
         %% the log is truncated only until the oldest message.
         release_cursors = lqueue:new() :: lqueue:lqueue({release_cursor,
                                                          ra:index(), #rabbit_fifo{}}),
         % consumers need to reflect consumer state at time of snapshot
         % needs to be part of snapshot
         consumers = #{} :: #{consumer_id() => consumer()},
         % consumers that require further service are queued here
         % needs to be part of snapshot
         service_queue = priority_queue:new() :: priority_queue:q(),
         %% This is a special field that is only used for snapshots
         %% It represents the queued messages at the time the
         %% dehydrated snapshot state was cached.
         %% As release_cursors are only emitted for raft indexes where all
         %% prior messages no longer contribute to the current state we can
         %% replace all message payloads with their sizes (to be used for
         %% overflow calculations).
         %% This is done so that consumers are still served in a deterministic
         %% order on recovery.
         %% TODO Remove this field and store prefix messages in-place. This will
         %% simplify the checkout logic.
         prefix_msgs = {0, [], 0, []} :: prefix_msgs(),
         %% state for at-least-once dead-lettering
         dlx = rabbit_fifo_dlx:init() :: rabbit_fifo_dlx:state(),
         msg_bytes_enqueue = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer(),
         %% waiting consumers, one is picked active consumer is cancelled or dies
         %% used only when single active consumer is on
         waiting_consumers = [] :: [{consumer_id(), consumer()}],
         msg_bytes_in_memory = 0 :: non_neg_integer(),
         msgs_ready_in_memory = 0 :: non_neg_integer(),
         last_active :: undefined | non_neg_integer(),
         unused_1,
         unused_2
        }).

-type config() :: #{name := atom(),
                    queue_resource := rabbit_types:r('queue'),
                    dead_letter_handler => dead_letter_handler(),
                    become_leader_handler => applied_mfa(),
                    release_cursor_interval => non_neg_integer(),
                    max_length => non_neg_integer(),
                    max_bytes => non_neg_integer(),
                    max_in_memory_length => non_neg_integer(),
                    max_in_memory_bytes => non_neg_integer(),
                    overflow_strategy => drop_head | reject_publish,
                    single_active_consumer_on => boolean(),
                    delivery_limit => non_neg_integer(),
                    expires => non_neg_integer(),
                    msg_ttl => non_neg_integer(),
                    created => non_neg_integer()
                   }.

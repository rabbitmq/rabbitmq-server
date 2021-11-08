%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_variable_queue).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/6, publish_delivered/5,
         batch_publish/4, batch_publish_delivered/4,
         discard/4, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         set_ram_duration_target/2, ram_duration/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2, set_queue_mode/2,
         set_queue_version/2,
         zip_msgs_and_acks/4,  multiple_routing_keys/0, handle_info/2]).

-export([start/2, stop/1]).

%% exported for testing only
-export([start_msg_store/3, stop_msg_store/1, init/6]).

-export([move_messages_to_vhost_store/0]).

-export([migrate_queue/3, migrate_message/3, get_per_vhost_store_client/2,
         get_global_store_client/1, log_upgrade_verbose/1,
         log_upgrade_verbose/2]).

-include_lib("stdlib/include/qlc.hrl").

-define(QUEUE_MIGRATION_BATCH_SIZE, 100).
-define(EMPTY_START_FUN_STATE, {fun (ok) -> finished end, ok}).

%%----------------------------------------------------------------------------
%% Messages, and their position in the queue, can be in memory or on
%% disk, or both. Persistent messages will have both message and
%% position pushed to disk as soon as they arrive; transient messages
%% can be written to disk (and thus both types can be evicted from
%% memory) under memory pressure. The question of whether a message is
%% in RAM and whether it is persistent are orthogonal.
%%
%% Messages are persisted using the queue index and the message
%% store. Normally the queue index holds the position of the message
%% *within this queue* along with a couple of small bits of metadata,
%% while the message store holds the message itself (including headers
%% and other properties).
%%
%% However, as an optimisation, small messages can be embedded
%% directly in the queue index and bypass the message store
%% altogether.
%%
%% Definitions:
%%
%% alpha: this is a message where both the message itself, and its
%%        position within the queue are held in RAM
%%
%% beta:  this is a message where the message itself is only held on
%%        disk (if persisted to the message store) but its position
%%        within the queue is held in RAM.
%%
%% gamma: this is a message where the message itself is only held on
%%        disk, but its position is both in RAM and on disk.
%%
%% delta: this is a collection of messages, represented by a single
%%        term, where the messages and their position are only held on
%%        disk.
%%
%% Note that for persistent messages, the message and its position
%% within the queue are always held on disk, *in addition* to being in
%% one of the above classifications.
%%
%% Also note that within this code, the term gamma seldom
%% appears. It's frequently the case that gammas are defined by betas
%% who have had their queue position recorded on disk.
%%
%% In general, messages move q1 -> q2 -> delta -> q3 -> q4, though
%% many of these steps are frequently skipped. q1 and q4 only hold
%% alphas, q2 and q3 hold both betas and gammas. When a message
%% arrives, its classification is determined. It is then added to the
%% rightmost appropriate queue.
%%
%% If a new message is determined to be a beta or gamma, q1 is
%% empty. If a new message is determined to be a delta, q1 and q2 are
%% empty (and actually q4 too).
%%
%% When removing messages from a queue, if q4 is empty then q3 is read
%% directly. If q3 becomes empty then the next segment's worth of
%% messages from delta are read into q3, reducing the size of
%% delta. If the queue is non empty, either q4 or q3 contain
%% entries. It is never permitted for delta to hold all the messages
%% in the queue.
%%
%% The duration indicated to us by the memory_monitor is used to
%% calculate, given our current ingress and egress rates, how many
%% messages we should hold in RAM (i.e. as alphas). We track the
%% ingress and egress rates for both messages and pending acks and
%% rates for both are considered when calculating the number of
%% messages to hold in RAM. When we need to push alphas to betas or
%% betas to gammas, we favour writing out messages that are further
%% from the head of the queue. This minimises writes to disk, as the
%% messages closer to the tail of the queue stay in the queue for
%% longer, thus do not need to be replaced as quickly by sending other
%% messages to disk.
%%
%% Whilst messages are pushed to disk and forgotten from RAM as soon
%% as requested by a new setting of the queue RAM duration, the
%% inverse is not true: we only load messages back into RAM as
%% demanded as the queue is read from. Thus only publishes to the
%% queue will take up available spare capacity.
%%
%% When we report our duration to the memory monitor, we calculate
%% average ingress and egress rates over the last two samples, and
%% then calculate our duration based on the sum of the ingress and
%% egress rates. More than two samples could be used, but it's a
%% balance between responding quickly enough to changes in
%% producers/consumers versus ignoring temporary blips. The problem
%% with temporary blips is that with just a few queues, they can have
%% substantial impact on the calculation of the average duration and
%% hence cause unnecessary I/O. Another alternative is to increase the
%% amqqueue_process:RAM_DURATION_UPDATE_PERIOD to beyond 5
%% seconds. However, that then runs the risk of being too slow to
%% inform the memory monitor of changes. Thus a 5 second interval,
%% plus a rolling average over the last two samples seems to work
%% well in practice.
%%
%% The sum of the ingress and egress rates is used because the egress
%% rate alone is not sufficient. Adding in the ingress rate means that
%% queues which are being flooded by messages are given more memory,
%% resulting in them being able to process the messages faster (by
%% doing less I/O, or at least deferring it) and thus helping keep
%% their mailboxes empty and thus the queue as a whole is more
%% responsive. If such a queue also has fast but previously idle
%% consumers, the consumer can then start to be driven as fast as it
%% can go, whereas if only egress rate was being used, the incoming
%% messages may have to be written to disk and then read back in,
%% resulting in the hard disk being a bottleneck in driving the
%% consumers. Generally, we want to give Rabbit every chance of
%% getting rid of messages as fast as possible and remaining
%% responsive, and using only the egress rate impacts that goal.
%%
%% Once the queue has more alphas than the target_ram_count, the
%% surplus must be converted to betas, if not gammas, if not rolled
%% into delta. The conditions under which these transitions occur
%% reflect the conflicting goals of minimising RAM cost per msg, and
%% minimising CPU cost per msg. Once the msg has become a beta, its
%% payload is no longer in RAM, thus a read from the msg_store must
%% occur before the msg can be delivered, but the RAM cost of a beta
%% is the same as a gamma, so converting a beta to gamma will not free
%% up any further RAM. To reduce the RAM cost further, the gamma must
%% be rolled into delta. Whilst recovering a beta or a gamma to an
%% alpha requires only one disk read (from the msg_store), recovering
%% a msg from within delta will require two reads (queue_index and
%% then msg_store). But delta has a near-0 per-msg RAM cost. So the
%% conflict is between using delta more, which will free up more
%% memory, but require additional CPU and disk ops, versus using delta
%% less and gammas and betas more, which will cost more memory, but
%% require fewer disk ops and less CPU overhead.
%%
%% In the case of a persistent msg published to a durable queue, the
%% msg is immediately written to the msg_store and queue_index. If
%% then additionally converted from an alpha, it'll immediately go to
%% a gamma (as it's already in queue_index), and cannot exist as a
%% beta. Thus a durable queue with a mixture of persistent and
%% transient msgs in it which has more messages than permitted by the
%% target_ram_count may contain an interspersed mixture of betas and
%% gammas in q2 and q3.
%%
%% There is then a ratio that controls how many betas and gammas there
%% can be. This is based on the target_ram_count and thus expresses
%% the fact that as the number of permitted alphas in the queue falls,
%% so should the number of betas and gammas fall (i.e. delta
%% grows). If q2 and q3 contain more than the permitted number of
%% betas and gammas, then the surplus are forcibly converted to gammas
%% (as necessary) and then rolled into delta. The ratio is that
%% delta/(betas+gammas+delta) equals
%% (betas+gammas+delta)/(target_ram_count+betas+gammas+delta). I.e. as
%% the target_ram_count shrinks to 0, so must betas and gammas.
%%
%% The conversion of betas to deltas is done if there are at least
%% ?IO_BATCH_SIZE betas in q2 & q3. This value should not be too small,
%% otherwise the frequent operations on the queues of q2 and q3 will not be
%% effectively amortised (switching the direction of queue access defeats
%% amortisation). Note that there is a natural upper bound due to credit_flow
%% limits on the alpha to beta conversion.
%%
%% The conversion from alphas to betas is chunked due to the
%% credit_flow limits of the msg_store. This further smooths the
%% effects of changes to the target_ram_count and ensures the queue
%% remains responsive even when there is a large amount of IO work to
%% do. The 'resume' callback is utilised to ensure that conversions
%% are done as promptly as possible whilst ensuring the queue remains
%% responsive.
%%
%% In the queue we keep track of both messages that are pending
%% delivery and messages that are pending acks. In the event of a
%% queue purge, we only need to load qi segments if the queue has
%% elements in deltas (i.e. it came under significant memory
%% pressure). In the event of a queue deletion, in addition to the
%% preceding, by keeping track of pending acks in RAM, we do not need
%% to search through qi segments looking for messages that are yet to
%% be acknowledged.
%%
%% Pending acks are recorded in memory by storing the message itself.
%% If the message has been sent to disk, we do not store the message
%% content. During memory reduction, pending acks containing message
%% content have that content removed and the corresponding messages
%% are pushed out to disk.
%%
%% Messages from pending acks are returned to q4, q3 and delta during
%% requeue, based on the limits of seq_id contained in each. Requeued
%% messages retain their original seq_id, maintaining order
%% when requeued.
%%
%% The order in which alphas are pushed to betas and pending acks
%% are pushed to disk is determined dynamically. We always prefer to
%% push messages for the source (alphas or acks) that is growing the
%% fastest (with growth measured as avg. ingress - avg. egress).
%%
%% Notes on Clean Shutdown
%% (This documents behaviour in variable_queue, queue_index and
%% msg_store.)
%%
%% In order to try to achieve as fast a start-up as possible, if a
%% clean shutdown occurs, we try to save out state to disk to reduce
%% work on startup. In the msg_store this takes the form of the
%% index_module's state, plus the file_summary ets table, and client
%% refs. In the VQ, this takes the form of the count of persistent
%% messages in the queue and references into the msg_stores. The
%% queue_index adds to these terms the details of its segments and
%% stores the terms in the queue directory.
%%
%% Two message stores are used. One is created for persistent messages
%% to durable queues that must survive restarts, and the other is used
%% for all other messages that just happen to need to be written to
%% disk. On start up we can therefore nuke the transient message
%% store, and be sure that the messages in the persistent store are
%% all that we need.
%%
%% The references to the msg_stores are there so that the msg_store
%% knows to only trust its saved state if all of the queues it was
%% previously talking to come up cleanly. Likewise, the queues
%% themselves (esp queue_index) skips work in init if all the queues
%% and msg_store were shutdown cleanly. This gives both good speed
%% improvements and also robustness so that if anything possibly went
%% wrong in shutdown (or there was subsequent manual tampering), all
%% messages and queues that can be recovered are recovered, safely.
%%
%% To delete transient messages lazily, the variable_queue, on
%% startup, stores the next_seq_id reported by the queue_index as the
%% transient_threshold. From that point on, whenever it's reading a
%% message off disk via the queue_index, if the seq_id is below this
%% threshold and the message is transient then it drops the message
%% (the message itself won't exist on disk because it would have been
%% stored in the transient msg_store which would have had its saved
%% state nuked on startup). This avoids the expensive operation of
%% scanning the entire queue on startup in order to delete transient
%% messages that were only pushed to disk to save memory.
%%
%% v2 UPDATE: The queue is keeping track of delivers via the
%% next_deliver_seq_id variable. This variable gets increased
%% with every (first-time) delivery. When delivering messages
%% the seq_id of the message is checked against this variable
%% to determine whether the message is a redelivery. The variable
%% is stored in the queue terms on graceful shutdown. On dirty
%% recovery the variable becomes the seq_id of the most recent
%% message in the queue (effectively marking all messages as
%% delivered, like the v1 index was doing).
%%
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(vqstate,
        { q1,
          q2,
          delta,
          q3,
          q4,
          next_seq_id,
          %% seq_id() of first undelivered message
          %% everything before this seq_id() was delivered at least once
          next_deliver_seq_id,
          ram_pending_ack,    %% msgs using store, still in RAM
          disk_pending_ack,   %% msgs in store, paged out
          qi_pending_ack,     %% msgs using qi, *can't* be paged out
          index_mod,
          index_state,
          store_state,
          msg_store_clients,
          durable,
          transient_threshold,
          qi_embed_msgs_below,

          len,                %% w/o unacked
          bytes,              %% w/o unacked
          unacked_bytes,
          persistent_count,   %% w   unacked
          persistent_bytes,   %% w   unacked
          delta_transient_bytes,        %%

          target_ram_count,
          ram_msg_count,      %% w/o unacked
          ram_msg_count_prev,
          ram_ack_count_prev,
          ram_bytes,          %% w   unacked
          out_counter,
          in_counter,
          rates,
          msgs_on_disk,
          msg_indices_on_disk,
          unconfirmed,
          confirmed,
          ack_out_counter,
          ack_in_counter,
          %% Unlike the other counters these two do not feed into
          %% #rates{} and get reset
          disk_read_count,
          disk_write_count,

          io_batch_size,

          %% default queue or lazy queue
          mode,
          version = 1,
          %% number of reduce_memory_usage executions, once it
          %% reaches a threshold the queue will manually trigger a runtime GC
          %% see: maybe_execute_gc/1
          memory_reduction_run_count,
          %% Queue data is grouped by VHost. We need to store it
          %% to work with queue index.
          virtual_host,
          waiting_bump = false
        }).

-record(rates, { in, out, ack_in, ack_out, timestamp }).

-type msg_location() :: memory
                      | rabbit_msg_store
                      | rabbit_queue_index
                      | rabbit_classic_queue_store_v2:msg_location().
-export_type([msg_location/0]).

-record(msg_status,
        { seq_id,
          msg_id,
          msg,
          is_persistent,
          is_delivered,
          msg_location, %% ?IN_SHARED_STORE | ?IN_QUEUE_STORE | ?IN_QUEUE_INDEX | ?IN_MEMORY
          index_on_disk,
          persist_to,
          msg_props
        }).

-record(delta,
        { start_seq_id, %% start_seq_id is inclusive
          count,
          transient,
          end_seq_id    %% end_seq_id is exclusive
        }).

-define(HEADER_GUESS_SIZE, 100). %% see determine_persist_to/2
-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).

-define(QUEUE, lqueue).

-define(IN_SHARED_STORE, rabbit_msg_store).
-define(IN_QUEUE_STORE, {rabbit_classic_queue_store_v2, _, _}).
-define(IN_QUEUE_INDEX, rabbit_queue_index).
-define(IN_MEMORY, memory).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("amqqueue.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({multiple_routing_keys, local, []}).
-rabbit_upgrade({move_messages_to_vhost_store, message_store, []}).

-type seq_id()  :: non_neg_integer().
-export_type([seq_id/0]).

-type rates() :: #rates { in        :: float(),
                          out       :: float(),
                          ack_in    :: float(),
                          ack_out   :: float(),
                          timestamp :: rabbit_types:timestamp()}.

-type delta() :: #delta { start_seq_id :: non_neg_integer(),
                          count        :: non_neg_integer(),
                          end_seq_id   :: non_neg_integer() }.

%% The compiler (rightfully) complains that ack() and state() are
%% unused. For this reason we duplicate a -spec from
%% rabbit_backing_queue with the only intent being to remove
%% warnings. The problem here is that we can't parameterise the BQ
%% behaviour by these two types as we would like to. We still leave
%% these here for documentation purposes.
-type ack() :: seq_id().
-type state() :: #vqstate {
             q1                    :: ?QUEUE:?QUEUE(),
             q2                    :: ?QUEUE:?QUEUE(),
             delta                 :: delta(),
             q3                    :: ?QUEUE:?QUEUE(),
             q4                    :: ?QUEUE:?QUEUE(),
             next_seq_id           :: seq_id(),
             next_deliver_seq_id   :: seq_id(),
             ram_pending_ack       :: gb_trees:tree(),
             disk_pending_ack      :: gb_trees:tree(),
             qi_pending_ack        :: gb_trees:tree(),
             index_mod             :: rabbit_queue_index | rabbit_classic_queue_index_v2,
             index_state           :: any(),
             store_state           :: any(),
             msg_store_clients     :: 'undefined' | {{any(), binary()},
                                                    {any(), binary()}},
             durable               :: boolean(),
             transient_threshold   :: non_neg_integer(),
             qi_embed_msgs_below   :: non_neg_integer(),

             len                   :: non_neg_integer(),
             bytes                 :: non_neg_integer(),
             unacked_bytes         :: non_neg_integer(),

             persistent_count      :: non_neg_integer(),
             persistent_bytes      :: non_neg_integer(),

             target_ram_count      :: non_neg_integer() | 'infinity',
             ram_msg_count         :: non_neg_integer(),
             ram_msg_count_prev    :: non_neg_integer(),
             ram_ack_count_prev    :: non_neg_integer(),
             ram_bytes             :: non_neg_integer(),
             out_counter           :: non_neg_integer(),
             in_counter            :: non_neg_integer(),
             rates                 :: rates(),
             msgs_on_disk          :: gb_sets:set(),
             msg_indices_on_disk   :: gb_sets:set(),
             unconfirmed           :: gb_sets:set(),
             confirmed             :: gb_sets:set(),
             ack_out_counter       :: non_neg_integer(),
             ack_in_counter        :: non_neg_integer(),
             disk_read_count       :: non_neg_integer(),
             disk_write_count      :: non_neg_integer(),

             io_batch_size         :: pos_integer(),
             mode                  :: 'default' | 'lazy',
             version               :: 1 | 2,
             memory_reduction_run_count :: non_neg_integer()}.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count        = 0,
                              transient    = 0,
                              end_seq_id   = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
                                         count        = 0,
                                         transient    = 0,
                                         end_seq_id   = Z }).

-define(MICROS_PER_SECOND, 1000000.0).

%% We're sampling every 5s for RAM duration; a half life that is of
%% the same order of magnitude is probably about right.
-define(RATE_AVG_HALF_LIFE, 5.0).

%% We will recalculate the #rates{} every time we get asked for our
%% RAM duration, or every N messages published, whichever is
%% sooner. We do this since the priority calculations in
%% rabbit_amqqueue_process need fairly fresh rates.
-define(MSGS_PER_RATE_CALC, 100).

%% we define the garbage collector threshold
%% it needs to tune the `reduce_memory_use` calls. Thus, the garbage collection.
%% see: rabbitmq-server-973 and rabbitmq-server-964
-define(DEFAULT_EXPLICIT_GC_RUN_OP_THRESHOLD, 1000).
-define(EXPLICIT_GC_RUN_OP_THRESHOLD(Mode),
    case get(explicit_gc_run_operation_threshold) of
        undefined ->
            Val = explicit_gc_run_operation_threshold_for_mode(Mode),
            put(explicit_gc_run_operation_threshold, Val),
            Val;
        Val       -> Val
    end).

explicit_gc_run_operation_threshold_for_mode(Mode) ->
    {Key, Fallback} = case Mode of
        lazy -> {lazy_queue_explicit_gc_run_operation_threshold,
                 ?DEFAULT_EXPLICIT_GC_RUN_OP_THRESHOLD};
        _    -> {queue_explicit_gc_run_operation_threshold,
                 ?DEFAULT_EXPLICIT_GC_RUN_OP_THRESHOLD}
    end,
    rabbit_misc:get_env(rabbit, Key, Fallback).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(VHost, DurableQueues) ->
    %% The v2 index walker function covers both v1 and v2 index files.
    {AllTerms, StartFunState} = rabbit_classic_queue_index_v2:start(VHost, DurableQueues),
    %% Group recovery terms by vhost.
    ClientRefs = [Ref || Terms <- AllTerms,
                      Terms /= non_clean_shutdown,
                      begin
                          Ref = proplists:get_value(persistent_ref, Terms),
                          Ref =/= undefined
                      end],
    start_msg_store(VHost, ClientRefs, StartFunState),
    {ok, AllTerms}.

stop(VHost) ->
    ok = stop_msg_store(VHost),
    ok = rabbit_classic_queue_index_v2:stop(VHost).

start_msg_store(VHost, Refs, StartFunState) when is_list(Refs); Refs == undefined ->
    rabbit_log:info("Starting message stores for vhost '~s'", [VHost]),
    do_start_msg_store(VHost, ?TRANSIENT_MSG_STORE, undefined, ?EMPTY_START_FUN_STATE),
    do_start_msg_store(VHost, ?PERSISTENT_MSG_STORE, Refs, StartFunState),
    ok.

do_start_msg_store(VHost, Type, Refs, StartFunState) ->
    case rabbit_vhost_msg_store:start(VHost, Type, Refs, StartFunState) of
        {ok, _} ->
            rabbit_log:info("Started message store of type ~s for vhost '~s'", [abbreviated_type(Type), VHost]);
        {error, {no_such_vhost, VHost}} = Err ->
            rabbit_log:error("Failed to start message store of type ~s for vhost '~s': the vhost no longer exists!",
                             [Type, VHost]),
            exit(Err);
        {error, Error} ->
            rabbit_log:error("Failed to start message store of type ~s for vhost '~s': ~p",
                             [Type, VHost, Error]),
            exit({error, Error})
    end.

abbreviated_type(?TRANSIENT_MSG_STORE)  -> transient;
abbreviated_type(?PERSISTENT_MSG_STORE) -> persistent.

stop_msg_store(VHost) ->
    rabbit_vhost_msg_store:stop(VHost, ?TRANSIENT_MSG_STORE),
    rabbit_vhost_msg_store:stop(VHost, ?PERSISTENT_MSG_STORE),
    ok.

init(Queue, Recover, Callback) ->
    init(
      Queue, Recover, Callback,
      fun (MsgIds, ActionTaken) ->
              msgs_written_to_disk(Callback, MsgIds, ActionTaken)
      end,
      fun (MsgIds) -> msg_indices_written_to_disk(Callback, MsgIds) end,
      fun (MsgIds) -> msgs_and_indices_written_to_disk(Callback, MsgIds) end).

init(Q, new, AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) when ?is_amqqueue(Q) ->
    QueueName = amqqueue:get_name(Q),
    IsDurable = amqqueue:is_durable(Q),
    %% We resolve the queue version immediately to avoid converting
    %% between queue versions unnecessarily.
    IndexMod = index_mod(Q),
    IndexState = IndexMod:init(QueueName, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
    StoreState = rabbit_classic_queue_store_v2:init(QueueName, MsgOnDiskFun),
    VHost = QueueName#resource.virtual_host,
    init(queue_version(Q),
         IsDurable, IndexMod, IndexState, StoreState, 0, 0, [],
         case IsDurable of
             true  -> msg_store_client_init(?PERSISTENT_MSG_STORE,
                                            MsgOnDiskFun, AsyncCallback, VHost);
             false -> undefined
         end,
         msg_store_client_init(?TRANSIENT_MSG_STORE, undefined,
                               AsyncCallback, VHost), VHost);

%% We can be recovering a transient queue if it crashed
init(Q, Terms, AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) when ?is_amqqueue(Q) ->
    QueueName = amqqueue:get_name(Q),
    IsDurable = amqqueue:is_durable(Q),
    {PRef, RecoveryTerms} = process_recovery_terms(Terms),
    VHost = QueueName#resource.virtual_host,
    {PersistentClient, ContainsCheckFun} =
        case IsDurable of
            true  -> C = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef,
                                               MsgOnDiskFun, AsyncCallback,
                                               VHost),
                     {C, fun (MsgId) when is_binary(MsgId) ->
                                 rabbit_msg_store:contains(MsgId, C);
                             (#basic_message{is_persistent = Persistent}) ->
                                 Persistent
                         end};
            false -> {undefined, fun(_MsgId) -> false end}
        end,
    TransientClient  = msg_store_client_init(?TRANSIENT_MSG_STORE,
                                             undefined, AsyncCallback,
                                             VHost),
    %% We MUST resolve the queue version immediately in order to recover.
    IndexMod = index_mod(Q),
    {DeltaCount, DeltaBytes, IndexState} =
        IndexMod:recover(
          QueueName, RecoveryTerms,
          rabbit_vhost_msg_store:successfully_recovered_state(
              VHost,
              ?PERSISTENT_MSG_STORE),
          ContainsCheckFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
    StoreState = rabbit_classic_queue_store_v2:init(QueueName, MsgOnDiskFun),
    init(queue_version(Q),
         IsDurable, IndexMod, IndexState, StoreState, DeltaCount, DeltaBytes, RecoveryTerms,
         PersistentClient, TransientClient, VHost).

process_recovery_terms(Terms=non_clean_shutdown) ->
    {rabbit_guid:gen(), Terms};
process_recovery_terms(Terms) ->
    case proplists:get_value(persistent_ref, Terms) of
        undefined -> {rabbit_guid:gen(), []};
        PRef      -> {PRef, Terms}
    end.

queue_version(Q) ->
    Resolve = fun(_, ArgVal) -> ArgVal end,
    case rabbit_queue_type_util:args_policy_lookup(<<"queue-version">>, Resolve, Q) of
        undefined -> rabbit_misc:get_env(rabbit, variable_queue_default_version, 1);
        Vsn when is_integer(Vsn) -> Vsn;
        Vsn -> binary_to_integer(Vsn)
    end.

index_mod(Q) ->
    case queue_version(Q) of
        1 -> rabbit_queue_index;
        2 -> rabbit_classic_queue_index_v2
    end.

terminate(_Reason, State) ->
    State1 = #vqstate { virtual_host        = VHost,
                        next_seq_id         = NextSeqId,
                        next_deliver_seq_id = NextDeliverSeqId,
                        persistent_count    = PCount,
                        persistent_bytes    = PBytes,
                        index_mod           = IndexMod,
                        index_state         = IndexState,
                        store_state         = StoreState,
                        msg_store_clients   = {MSCStateP, MSCStateT} } =
        purge_pending_ack(true, State),
    PRef = case MSCStateP of
               undefined -> undefined;
               _         -> ok = maybe_client_terminate(MSCStateP),
                            rabbit_msg_store:client_ref(MSCStateP)
           end,
    ok = rabbit_msg_store:client_delete_and_terminate(MSCStateT),
    Terms = [{next_seq_id,         NextSeqId},
             {next_deliver_seq_id, NextDeliverSeqId},
             {persistent_ref,      PRef},
             {persistent_count,    PCount},
             {persistent_bytes,    PBytes}],
    a(State1#vqstate {
        index_state = IndexMod:terminate(VHost, Terms, IndexState),
        store_state = rabbit_classic_queue_store_v2:terminate(StoreState),
        msg_store_clients = undefined }).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete_and_terminate(_Reason, State) ->
    %% Normally when we purge messages we interact with the qi by
    %% issues delivers and acks for every purged message. In this case
    %% we don't need to do that, so we just delete the qi.
    State1 = purge_and_index_reset(State),
    State2 = #vqstate { msg_store_clients = {MSCStateP, MSCStateT} } =
        purge_pending_ack_delete_and_terminate(State1),
    case MSCStateP of
        undefined -> ok;
        _         -> rabbit_msg_store:client_delete_and_terminate(MSCStateP)
    end,
    rabbit_msg_store:client_delete_and_terminate(MSCStateT),
    a(State2 #vqstate { msg_store_clients = undefined }).

delete_crashed(Q) when ?is_amqqueue(Q) ->
    QName = amqqueue:get_name(Q),
    ok = rabbit_classic_queue_index_v2:erase(QName).

purge(State = #vqstate { len = Len }) ->
    case is_pending_ack_empty(State) and is_unconfirmed_empty(State) of
        true ->
            {Len, purge_and_index_reset(State)};
        false ->
            {Len, purge_when_pending_acks(State)}
    end.

purge_acks(State) -> a(purge_pending_ack(false, State)).

publish(Msg, MsgProps, IsDelivered, ChPid, Flow, State) ->
    State1 =
        publish1(Msg, MsgProps, IsDelivered, ChPid, Flow,
                 fun maybe_write_to_disk/4,
                 State),
    a(maybe_reduce_memory_use(maybe_update_rates(State1))).

batch_publish(Publishes, ChPid, Flow, State) ->
    {ChPid, Flow, State1} =
        lists:foldl(fun batch_publish1/2, {ChPid, Flow, State}, Publishes),
    State2 = ui(State1),
    a(maybe_reduce_memory_use(maybe_update_rates(State2))).

publish_delivered(Msg, MsgProps, ChPid, Flow, State) ->
    {SeqId, State1} =
        publish_delivered1(Msg, MsgProps, ChPid, Flow,
                           fun maybe_write_to_disk/4,
                           State),
    {SeqId, a(maybe_reduce_memory_use(maybe_update_rates(State1)))}.

batch_publish_delivered(Publishes, ChPid, Flow, State) ->
    {ChPid, Flow, SeqIds, State1} =
        lists:foldl(fun batch_publish_delivered1/2,
                    {ChPid, Flow, [], State}, Publishes),
    State2 = ui(State1),
    {lists:reverse(SeqIds), a(maybe_reduce_memory_use(maybe_update_rates(State2)))}.

discard(_MsgId, _ChPid, _Flow, State) -> State.

drain_confirmed(State = #vqstate { confirmed = C }) ->
    case gb_sets:is_empty(C) of
        true  -> {[], State}; %% common case
        false -> {gb_sets:to_list(C), State #vqstate {
                                        confirmed = gb_sets:new() }}
    end.

dropwhile(Pred, State) ->
    {MsgProps, State1} =
        remove_by_predicate(Pred, State),
    {MsgProps, a(State1)}.

fetchwhile(Pred, Fun, Acc, State) ->
    {MsgProps, Acc1, State1} =
         fetch_by_predicate(Pred, Fun, Acc, State),
    {MsgProps, Acc1, a(State1)}.

fetch(AckRequired, State) ->
    case queue_out(State) of
        {empty, State1} ->
            {empty, a(State1)};
        {{value, MsgStatus}, State1} ->
            %% it is possible that the message wasn't read from disk
            %% at this point, so read it in.
            {Msg, State2} = read_msg(MsgStatus, State1),
            {AckTag, State3} = remove(AckRequired, MsgStatus, State2),
            {{Msg, MsgStatus#msg_status.is_delivered, AckTag}, a(State3)}
    end.

drop(AckRequired, State) ->
    case queue_out(State) of
        {empty, State1} ->
            {empty, a(State1)};
        {{value, MsgStatus}, State1} ->
            {AckTag, State2} = remove(AckRequired, MsgStatus, State1),
            {{MsgStatus#msg_status.msg_id, AckTag}, a(State2)}
    end.

%% Duplicated from rabbit_backing_queue
-spec ack([ack()], state()) -> {[rabbit_guid:guid()], state()}.

ack([], State) ->
    {[], State};
%% optimisation: this head is essentially a partial evaluation of the
%% general case below, for the single-ack case.
ack([SeqId], State) ->
    case remove_pending_ack(true, SeqId, State) of
        {none, _} ->
            {[], State};
        {#msg_status { msg_id        = MsgId,
                       is_persistent = IsPersistent,
                       msg_location  = MsgLocation,
                       index_on_disk = IndexOnDisk },
         State1 = #vqstate { index_mod         = IndexMod,
                             index_state       = IndexState,
                             store_state       = StoreState0,
                             msg_store_clients = MSCState,
                             ack_out_counter   = AckOutCount }} ->
            {DeletedSegments, IndexState1} = case IndexOnDisk of
                              true  -> IndexMod:ack([SeqId], IndexState);
                              false -> {[], IndexState}
                          end,
            StoreState1 = case MsgLocation of
                ?IN_SHARED_STORE  -> ok = msg_store_remove(MSCState, IsPersistent, [MsgId]), StoreState0;
                ?IN_QUEUE_STORE   -> rabbit_classic_queue_store_v2:remove(SeqId, StoreState0);
                ?IN_QUEUE_INDEX   -> StoreState0;
                ?IN_MEMORY        -> StoreState0
            end,
            StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState1),
            {[MsgId],
             a(State1 #vqstate { index_state      = IndexState1,
                                 store_state      = StoreState,
                                 ack_out_counter  = AckOutCount + 1 })}
    end;
ack(AckTags, State) ->
    {{IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, AllMsgIds},
     State1 = #vqstate { index_mod         = IndexMod,
                         index_state       = IndexState,
                         store_state       = StoreState0,
                         msg_store_clients = MSCState,
                         ack_out_counter   = AckOutCount }} =
        lists:foldl(
          fun (SeqId, {Acc, State2}) ->
                  case remove_pending_ack(true, SeqId, State2) of
                      {none, _} ->
                          {Acc, State2};
                      {MsgStatus, State3} ->
                          {accumulate_ack(MsgStatus, Acc), State3}
                  end
          end, {accumulate_ack_init(), State}, AckTags),
    {DeletedSegments, IndexState1} = IndexMod:ack(IndexOnDiskSeqIds, IndexState),
    StoreState1 = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState0),
    StoreState = lists:foldl(fun rabbit_classic_queue_store_v2:remove/2, StoreState1, SeqIdsInStore),
    remove_vhost_msgs_by_id(MsgIdsByStore, MSCState),
    {lists:reverse(AllMsgIds),
     a(State1 #vqstate { index_state      = IndexState1,
                         store_state      = StoreState,
                         ack_out_counter  = AckOutCount + length(AckTags) })}.

requeue(AckTags, #vqstate { mode       = default,
                            delta      = Delta,
                            q3         = Q3,
                            q4         = Q4,
                            in_counter = InCounter,
                            len        = Len } = State) ->
    {SeqIds,  Q4a, MsgIds,  State1} = queue_merge(lists:sort(AckTags), Q4, [],
                                                  beta_limit(Q3),
                                                  fun publish_alpha/2, State),
    {SeqIds1, Q3a, MsgIds1, State2} = queue_merge(SeqIds, Q3, MsgIds,
                                                  delta_limit(Delta),
                                                  fun publish_beta/2, State1),
    {Delta1, MsgIds2, State3}       = delta_merge(SeqIds1, Delta, MsgIds1,
                                                  State2),
    MsgCount = length(MsgIds2),
    {MsgIds2, a(maybe_reduce_memory_use(
                  maybe_update_rates(ui(
                    State3 #vqstate { delta      = Delta1,
                                      q3         = Q3a,
                                      q4         = Q4a,
                                      in_counter = InCounter + MsgCount,
                                      len        = Len + MsgCount }))))};
requeue(AckTags, #vqstate { mode       = lazy,
                            delta      = Delta,
                            q3         = Q3,
                            in_counter = InCounter,
                            len        = Len } = State) ->
    {SeqIds, Q3a, MsgIds, State1} = queue_merge(lists:sort(AckTags), Q3, [],
                                                delta_limit(Delta),
                                                fun publish_beta/2, State),
    {Delta1, MsgIds1, State2}     = delta_merge(SeqIds, Delta, MsgIds,
                                                State1),
    MsgCount = length(MsgIds1),
    {MsgIds1, a(maybe_reduce_memory_use(
                  maybe_update_rates(ui(
                    State2 #vqstate { delta      = Delta1,
                                      q3         = Q3a,
                                      in_counter = InCounter + MsgCount,
                                      len        = Len + MsgCount }))))}.

ackfold(MsgFun, Acc, State, AckTags) ->
    {AccN, StateN} =
        lists:foldl(fun(SeqId, {Acc0, State0}) ->
                            MsgStatus = lookup_pending_ack(SeqId, State0),
                            {Msg, State1} = read_msg(MsgStatus, State0),
                            {MsgFun(Msg, SeqId, Acc0), State1}
                    end, {Acc, State}, AckTags),
    {AccN, a(StateN)}.

fold(Fun, Acc, State = #vqstate{index_state = IndexState}) ->
    {Its, IndexState1} = lists:foldl(fun inext/2, {[], IndexState},
                                     [msg_iterator(State),
                                      disk_ack_iterator(State),
                                      ram_ack_iterator(State),
                                      qi_ack_iterator(State)]),
    ifold(Fun, Acc, Its, State#vqstate{index_state = IndexState1}).

len(#vqstate { len = Len }) -> Len.

is_empty(State) -> 0 == len(State).

depth(State) ->
    len(State) + count_pending_acks(State).

set_ram_duration_target(
  DurationTarget, State = #vqstate {
                    rates = #rates { in      = AvgIngressRate,
                                     out     = AvgEgressRate,
                                     ack_in  = AvgAckIngressRate,
                                     ack_out = AvgAckEgressRate },
                    target_ram_count = TargetRamCount }) ->
    Rate =
        AvgEgressRate + AvgIngressRate + AvgAckEgressRate + AvgAckIngressRate,
    TargetRamCount1 =
        case DurationTarget of
            infinity  -> infinity;
            _         -> trunc(DurationTarget * Rate) %% msgs = sec * msgs/sec
        end,
    State1 = State #vqstate { target_ram_count = TargetRamCount1 },
    a(case TargetRamCount1 == infinity orelse
          (TargetRamCount =/= infinity andalso
           TargetRamCount1 >= TargetRamCount) of
          true  -> State1;
          false -> reduce_memory_use(State1)
      end).

maybe_update_rates(State = #vqstate{ in_counter  = InCount,
                                     out_counter = OutCount })
  when InCount + OutCount > ?MSGS_PER_RATE_CALC ->
    update_rates(State);
maybe_update_rates(State) ->
    State.

update_rates(State = #vqstate{ in_counter      =     InCount,
                               out_counter     =    OutCount,
                               ack_in_counter  =  AckInCount,
                               ack_out_counter = AckOutCount,
                               rates = #rates{ in        =     InRate,
                                               out       =    OutRate,
                                               ack_in    =  AckInRate,
                                               ack_out   = AckOutRate,
                                               timestamp = TS }}) ->
    Now = erlang:monotonic_time(),

    Rates = #rates { in        = update_rate(Now, TS,     InCount,     InRate),
                     out       = update_rate(Now, TS,    OutCount,    OutRate),
                     ack_in    = update_rate(Now, TS,  AckInCount,  AckInRate),
                     ack_out   = update_rate(Now, TS, AckOutCount, AckOutRate),
                     timestamp = Now },

    State#vqstate{ in_counter      = 0,
                   out_counter     = 0,
                   ack_in_counter  = 0,
                   ack_out_counter = 0,
                   rates           = Rates }.

update_rate(Now, TS, Count, Rate) ->
    Time = erlang:convert_time_unit(Now - TS, native, micro_seconds) /
        ?MICROS_PER_SECOND,
    if
        Time == 0 -> Rate;
        true      -> rabbit_misc:moving_average(Time, ?RATE_AVG_HALF_LIFE,
                                                Count / Time, Rate)
    end.

ram_duration(State) ->
    State1 = #vqstate { rates = #rates { in      = AvgIngressRate,
                                         out     = AvgEgressRate,
                                         ack_in  = AvgAckIngressRate,
                                         ack_out = AvgAckEgressRate },
                        ram_msg_count      = RamMsgCount,
                        ram_msg_count_prev = RamMsgCountPrev,
                        ram_pending_ack    = RPA,
                        qi_pending_ack     = QPA,
                        ram_ack_count_prev = RamAckCountPrev } =
        update_rates(State),

    RamAckCount = gb_trees:size(RPA) + gb_trees:size(QPA),

    Duration = %% msgs+acks / (msgs+acks/sec) == sec
        case lists:all(fun (X) -> X < 0.01 end,
                       [AvgEgressRate, AvgIngressRate,
                        AvgAckEgressRate, AvgAckIngressRate]) of
            true  -> infinity;
            false -> (RamMsgCountPrev + RamMsgCount +
                          RamAckCount + RamAckCountPrev) /
                         (4 * (AvgEgressRate + AvgIngressRate +
                                   AvgAckEgressRate + AvgAckIngressRate))
        end,

    {Duration, State1}.

needs_timeout(#vqstate { index_mod   = IndexMod,
                         index_state = IndexState }) ->
    case IndexMod:needs_sync(IndexState) of
        confirms -> timed;
        other    -> idle;
        false    -> false
    end.

timeout(State = #vqstate { index_mod   = IndexMod,
                           index_state = IndexState0,
                           store_state = StoreState0 }) ->
    StoreState = rabbit_classic_queue_store_v2:sync(StoreState0),
    IndexState = IndexMod:sync(IndexState0),
    State #vqstate { index_state = IndexState,
                     store_state = StoreState }.

handle_pre_hibernate(State = #vqstate { index_mod   = IndexMod,
                                        index_state = IndexState }) ->
    %% @todo Sync the store before the index but only if IndexMod:needs_sync != false.
    State #vqstate { index_state = IndexMod:flush(IndexState) }.

handle_info(bump_reduce_memory_use, State = #vqstate{ waiting_bump = true }) ->
    State#vqstate{ waiting_bump = false };
handle_info(bump_reduce_memory_use, State) ->
    State.

resume(State) -> a(reduce_memory_use(State)).

msg_rates(#vqstate { rates = #rates { in  = AvgIngressRate,
                                      out = AvgEgressRate } }) ->
    {AvgIngressRate, AvgEgressRate}.

info(messages_ready_ram, #vqstate{ram_msg_count = RamMsgCount}) ->
    RamMsgCount;
info(messages_unacknowledged_ram, #vqstate{ram_pending_ack = RPA,
                                           qi_pending_ack  = QPA}) ->
    gb_trees:size(RPA) + gb_trees:size(QPA);
info(messages_ram, State) ->
    info(messages_ready_ram, State) + info(messages_unacknowledged_ram, State);
info(messages_persistent, #vqstate{persistent_count = PersistentCount}) ->
    PersistentCount;
info(messages_paged_out, #vqstate{delta = #delta{transient = Count}}) ->
    Count;
info(message_bytes, #vqstate{bytes         = Bytes,
                             unacked_bytes = UBytes}) ->
    Bytes + UBytes;
info(message_bytes_ready, #vqstate{bytes = Bytes}) ->
    Bytes;
info(message_bytes_unacknowledged, #vqstate{unacked_bytes = UBytes}) ->
    UBytes;
info(message_bytes_ram, #vqstate{ram_bytes = RamBytes}) ->
    RamBytes;
info(message_bytes_persistent, #vqstate{persistent_bytes = PersistentBytes}) ->
    PersistentBytes;
info(message_bytes_paged_out, #vqstate{delta_transient_bytes = PagedOutBytes}) ->
    PagedOutBytes;
info(head_message_timestamp, #vqstate{
          q3               = Q3,
          q4               = Q4,
          ram_pending_ack  = RPA,
          qi_pending_ack   = QPA}) ->
          head_message_timestamp(Q3, Q4, RPA, QPA);
info(disk_reads, #vqstate{disk_read_count = Count}) ->
    Count;
info(disk_writes, #vqstate{disk_write_count = Count}) ->
    Count;
info(backing_queue_status, #vqstate {
          q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
          mode             = Mode,
          version          = Version,
          len              = Len,
          target_ram_count = TargetRamCount,
          next_seq_id      = NextSeqId,
          rates            = #rates { in      = AvgIngressRate,
                                      out     = AvgEgressRate,
                                      ack_in  = AvgAckIngressRate,
                                      ack_out = AvgAckEgressRate }}) ->

    [ {mode                , Mode},
      {version             , Version},
      {q1                  , ?QUEUE:len(Q1)},
      {q2                  , ?QUEUE:len(Q2)},
      {delta               , Delta},
      {q3                  , ?QUEUE:len(Q3)},
      {q4                  , ?QUEUE:len(Q4)},
      {len                 , Len},
      {target_ram_count    , TargetRamCount},
      {next_seq_id         , NextSeqId},
      {avg_ingress_rate    , AvgIngressRate},
      {avg_egress_rate     , AvgEgressRate},
      {avg_ack_ingress_rate, AvgAckIngressRate},
      {avg_ack_egress_rate , AvgAckEgressRate} ];
info(_, _) ->
    ''.

invoke(?MODULE, Fun, State) -> Fun(?MODULE, State);
invoke(      _,   _, State) -> State.

is_duplicate(_Msg, State) -> {false, State}.

set_queue_mode(Mode, State = #vqstate { mode = Mode }) ->
    State;
set_queue_mode(lazy, State = #vqstate {
                                target_ram_count = TargetRamCount }) ->
    %% To become a lazy queue we need to page everything to disk first.
    State1 = convert_to_lazy(State),
    %% restore the original target_ram_count
    a(State1 #vqstate { mode = lazy, target_ram_count = TargetRamCount });
set_queue_mode(default, State) ->
    %% becoming a default queue means loading messages from disk like
    %% when a queue is recovered.
    a(maybe_deltas_to_betas(State #vqstate { mode = default }));
set_queue_mode(_, State) ->
    State.

zip_msgs_and_acks(Msgs, AckTags, Accumulator, _State) ->
    lists:foldl(fun ({{#basic_message{ id = Id }, _Props}, AckTag}, Acc) ->
                        [{Id, AckTag} | Acc]
                end, Accumulator, lists:zip(Msgs, AckTags)).

convert_to_lazy(State) ->
    State1 = #vqstate { delta = Delta, q3 = Q3, len = Len } =
        set_ram_duration_target(0, State),
    case Delta#delta.count + ?QUEUE:len(Q3) == Len of
        true ->
            State1;
        false ->
            %% When pushing messages to disk, we might have been
            %% blocked by the msg_store, so we need to see if we have
            %% to wait for more credit, and then keep paging messages.
            %%
            %% The amqqueue_process could have taken care of this, but
            %% between the time it receives the bump_credit msg and
            %% calls BQ:resume to keep paging messages to disk, some
            %% other request may arrive to the BQ which at this moment
            %% is not in a proper state for a lazy BQ (unless all
            %% messages have been paged to disk already).
            wait_for_msg_store_credit(),
            convert_to_lazy(resume(State1))
    end.

wait_for_msg_store_credit() ->
    case credit_flow:blocked() of
        true  -> receive
                     {bump_credit, Msg} ->
                         credit_flow:handle_bump_msg(Msg)
                 end;
        false -> ok
    end.

%% No change.
set_queue_version(Version, State = #vqstate { version = Version }) ->
    State;
%% v2 -> v1.
set_queue_version(1, State = #vqstate { version = 2 }) ->
    convert_from_v2_to_v1(State #vqstate { version = 1 });
%% v1 -> v2.
set_queue_version(2, State = #vqstate { version = 1 }) ->
    convert_from_v1_to_v2(State #vqstate { version = 2 }).

%% We move messages from the v1 index to the v2 index. The message payload
%% is moved to the v2 store if it was embedded, and left in the per-vhost
%% store otherwise.
convert_from_v1_to_v2(State0 = #vqstate{ index_mod   = rabbit_queue_index,
                                         index_state = V1Index }) ->
    State = convert_from_v1_to_v2_in_memory(State0),
    {QueueName, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun} = rabbit_queue_index:init_args(V1Index),
    V2Index0 = rabbit_classic_queue_index_v2:init_for_conversion(QueueName, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
    V2Store0 = rabbit_classic_queue_store_v2:init(QueueName, fun(_, _) -> ok end),
    {LoSeqId, HiSeqId, _} = rabbit_queue_index:bounds(V1Index),
    {V2Index, V2Store} = convert_from_v1_to_v2_loop(QueueName, V1Index, V2Index0, V2Store0, LoSeqId, HiSeqId),
    %% We have already deleted segments files but not the journal.
    rabbit_queue_index:delete_journal(V1Index),
    State#vqstate{ index_mod   = rabbit_classic_queue_index_v2,
                   index_state = V2Index,
                   store_state = V2Store }.

convert_from_v1_to_v2_in_memory(State = #vqstate{ q1 = Q1b,
                                                  q2 = Q2b,
                                                  q3 = Q3b,
                                                  q4 = Q4b }) ->
    Q1 = convert_from_v1_to_v2_queue(Q1b),
    Q2 = convert_from_v1_to_v2_queue(Q2b),
    Q3 = convert_from_v1_to_v2_queue(Q3b),
    Q4 = convert_from_v1_to_v2_queue(Q4b),
    State#vqstate{ q1 = Q1,
                   q2 = Q2,
                   q3 = Q3,
                   q4 = Q4 }.

%% We change where the message is expected to be persisted to.
%% We do not need to worry about the message location because
%% it will only be in memory or in the per-vhost store.
convert_from_v1_to_v2_queue(Q) ->
    List0 = ?QUEUE:to_list(Q),
    List = lists:map(fun
        (MsgStatus = #msg_status{ persist_to = queue_index }) ->
            MsgStatus#msg_status{ persist_to = queue_store };
        (MsgStatus) ->
            MsgStatus
    end, List0),
    ?QUEUE:from_list(List).

convert_from_v1_to_v2_loop(_, _, V2Index, V2Store, HiSeqId, HiSeqId) ->
    {V2Index, V2Store};
convert_from_v1_to_v2_loop(QueueName, V1Index0, V2Index0, V2Store0, LoSeqId, HiSeqId) ->
    UpSeqId = lists:min([rabbit_queue_index:next_segment_boundary(LoSeqId),
                         HiSeqId]),
    {Messages, V1Index} = rabbit_queue_index:read(LoSeqId, UpSeqId, V1Index0),
    %% We do a garbage collect immediately after the old index read
    %% because that may have created a lot of garbage.
    garbage_collect(),
    {V2Index3, V2Store3} = lists:foldl(fun
        %% Move embedded messages to the per-queue store.
        ({Msg = #basic_message{id = MsgId}, SeqId, rabbit_queue_index, Props, IsPersistent},
         {V2Index1, V2Store1}) ->
            {MsgLocation, V2Store2} = rabbit_classic_queue_store_v2:write(SeqId, Msg, Props, V2Store1),
            V2Index2 = rabbit_classic_queue_index_v2:publish(MsgId, SeqId, MsgLocation, Props, IsPersistent, infinity, V2Index1),
            {V2Index2, V2Store2};
        %% Keep messages in the per-vhost store where they are.
        ({MsgId, SeqId, rabbit_msg_store, Props, IsPersistent},
         {V2Index1, V2Store1}) ->
            V2Index2 = rabbit_classic_queue_index_v2:publish(MsgId, SeqId, rabbit_msg_store, Props, IsPersistent, infinity, V2Index1),
            {V2Index2, V2Store1}
    end, {V2Index0, V2Store0}, Messages),
    %% Flush to disk to avoid keeping too much in memory between segments.
    V2Index = rabbit_classic_queue_index_v2:flush(V2Index3),
    V2Store = rabbit_classic_queue_store_v2:sync(V2Store3),
    %% We have written everything to disk. We can delete the old segment file
    %% to free up much needed space, to avoid doubling disk usage during the upgrade.
    rabbit_queue_index:delete_segment_file_for_seq_id(LoSeqId, V1Index),
    %% Log some progress to keep the user aware of what's going on, as moving
    %% embedded messages can take quite some time.
    #resource{virtual_host = VHost, name = Name} = QueueName,
    rabbit_log:info("Queue ~s on vhost ~s converted ~b messages from v1 to v2",
                    [Name, VHost, length(Messages)]),
    convert_from_v1_to_v2_loop(QueueName, V1Index, V2Index, V2Store, UpSeqId, HiSeqId).

%% We move messages from the v1 index to the v2 index. The message payload
%% is moved to the v2 store if it was embedded, and left in the per-vhost
%% store otherwise.
convert_from_v2_to_v1(State0 = #vqstate{ index_mod   = rabbit_classic_queue_index_v2,
                                         index_state = V2Index,
                                         store_state = V2Store }) ->
    State = convert_from_v2_to_v1_in_memory(State0),
    {QueueName, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun} = rabbit_classic_queue_index_v2:init_args(V2Index),
    V1Index0 = rabbit_queue_index:init_for_conversion(QueueName, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
    {LoSeqId, HiSeqId, _} = rabbit_classic_queue_index_v2:bounds(V2Index),
    V1Index = convert_from_v2_to_v1_loop(QueueName, V1Index0, V2Index, V2Store, LoSeqId, HiSeqId),
    %% We have already closed the v2 index/store FDs when deleting the files.
    State#vqstate{ index_mod   = rabbit_queue_index,
                   index_state = V1Index,
                   store_state = undefined }.

convert_from_v2_to_v1_in_memory(State0 = #vqstate{ q1 = Q1b,
                                                   q2 = Q2b,
                                                   q3 = Q3b,
                                                   q4 = Q4b }) ->
    {Q1, State1} = convert_from_v2_to_v1_queue(Q1b, State0),
    {Q2, State2} = convert_from_v2_to_v1_queue(Q2b, State1),
    {Q3, State3} = convert_from_v2_to_v1_queue(Q3b, State2),
    {Q4, State4} = convert_from_v2_to_v1_queue(Q4b, State3),
    State4#vqstate{ q1 = Q1,
                    q2 = Q2,
                    q3 = Q3,
                    q4 = Q4 }.

%% We fetch the message from the per-queue store if necessary
%% and mark all messages as delivered to make the v1 index happy.
convert_from_v2_to_v1_queue(Q, State0) ->
    List0 = ?QUEUE:to_list(Q),
    {List, State} = lists:mapfoldl(fun (MsgStatus0, State1 = #vqstate{ store_state = StoreState0 }) ->
        case MsgStatus0 of
            #msg_status{ seq_id = SeqId,
                         msg_location = MsgLocation = {rabbit_classic_queue_store_v2, _, _} } ->
                {Msg, StoreState} = rabbit_classic_queue_store_v2:read(SeqId, MsgLocation, StoreState0),
                MsgStatus = MsgStatus0#msg_status{ msg = Msg,
                                                   msg_location = memory,
                                                   is_delivered = true,
                                                   persist_to   = queue_index },
                State2 = stats(ready0, {MsgStatus0, MsgStatus}, 0, State1),
                {MsgStatus, State2#vqstate{ store_state = StoreState }};
            #msg_status{ persist_to = queue_store } ->
                {MsgStatus0#msg_status{ is_delivered = true,
                                        persist_to   = queue_index }, State1};
            _ ->
                {MsgStatus0#msg_status{ is_delivered = true }, State1}
        end
    end, State0, List0),
    {?QUEUE:from_list(List), State}.

convert_from_v2_to_v1_loop(_, V1Index, _, _, HiSeqId, HiSeqId) ->
    V1Index;
convert_from_v2_to_v1_loop(QueueName, V1Index0, V2Index0, V2Store0, LoSeqId, HiSeqId) ->
    UpSeqId = lists:min([rabbit_classic_queue_index_v2:next_segment_boundary(LoSeqId),
                         HiSeqId]),
    {Messages, V2Index1} = rabbit_classic_queue_index_v2:read(LoSeqId, UpSeqId, V2Index0),
    {V1Index3, V2Store3} = lists:foldl(fun
        %% Read per-queue store messages before writing to the index.
        ({_MsgId, SeqId, Location = {rabbit_classic_queue_store_v2, _, _}, Props, IsPersistent},
         {V1Index1, V2Store1}) ->
            {Msg, V2Store2} = rabbit_classic_queue_store_v2:read(SeqId, Location, V2Store1),
            V1Index2 = rabbit_queue_index:publish(Msg, SeqId, rabbit_queue_index, Props, IsPersistent, infinity, V1Index1),
            V1Index2b = rabbit_queue_index:deliver([SeqId], V1Index2),
            {V1Index2b, V2Store2};
        %% Keep messages in the per-vhost store where they are.
        ({MsgId, SeqId, rabbit_msg_store, Props, IsPersistent},
         {V1Index1, V2Store1}) ->
            V1Index2 = rabbit_queue_index:publish(MsgId, SeqId, rabbit_msg_store, Props, IsPersistent, infinity, V1Index1),
            V1Index2b = rabbit_queue_index:deliver([SeqId], V1Index2),
            {V1Index2b, V2Store1}
    end, {V1Index0, V2Store0}, Messages),
    %% Flush to disk to avoid keeping too much in memory between segments.
    V1Index = rabbit_queue_index:flush(V1Index3),
    %% We do a garbage collect because the old index may have created a lot of garbage.
    garbage_collect(),
    %% We have written everything to disk. We can delete the old segment file
    %% to free up much needed space, to avoid doubling disk usage during the upgrade.
    {DeletedSegments, V2Index} = rabbit_classic_queue_index_v2:delete_segment_file_for_seq_id(LoSeqId, V2Index1),
    V2Store = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, V2Store3),
    %% Log some progress to keep the user aware of what's going on, as moving
    %% embedded messages can take quite some time.
    #resource{virtual_host = VHost, name = Name} = QueueName,
    rabbit_log:info("Queue ~s on vhost ~s converted ~b messages from v2 to v1",
                    [Name, VHost, length(Messages)]),
    convert_from_v2_to_v1_loop(QueueName, V1Index, V2Index, V2Store, UpSeqId, HiSeqId).

%% Get the Timestamp property of the first msg, if present. This is
%% the one with the oldest timestamp among the heads of the pending
%% acks and unread queues.  We can't check disk_pending_acks as these
%% are paged out - we assume some will soon be paged in rather than
%% forcing it to happen.  Pending ack msgs are included as they are
%% regarded as unprocessed until acked, this also prevents the result
%% apparently oscillating during repeated rejects.  Q3 is only checked
%% when Q4 is empty as any Q4 msg will be earlier.
head_message_timestamp(Q3, Q4, RPA, QPA) ->
    HeadMsgs = [ HeadMsgStatus#msg_status.msg ||
                   HeadMsgStatus <-
                       [ get_qs_head([Q4, Q3]),
                         get_pa_head(RPA),
                         get_pa_head(QPA) ],
                   HeadMsgStatus /= undefined,
                   HeadMsgStatus#msg_status.msg /= undefined ],

    Timestamps =
        [Timestamp || HeadMsg <- HeadMsgs,
                      Timestamp <- [rabbit_basic:extract_timestamp(
                                      HeadMsg#basic_message.content)],
                      Timestamp /= undefined
        ],

    case Timestamps == [] of
        true -> '';
        false -> lists:min(Timestamps)
    end.

get_qs_head(Qs) ->
    catch lists:foldl(
            fun (Q, Acc) ->
                    case get_q_head(Q) of
                        undefined -> Acc;
                        Val -> throw(Val)
                    end
            end, undefined, Qs).

get_q_head(Q) ->
    get_collection_head(Q, fun ?QUEUE:is_empty/1, fun ?QUEUE:peek/1).

get_pa_head(PA) ->
    get_collection_head(PA, fun gb_trees:is_empty/1, fun gb_trees:smallest/1).

get_collection_head(Col, IsEmpty, GetVal) ->
    case IsEmpty(Col) of
        false ->
            {_, MsgStatus} = GetVal(Col),
            MsgStatus;
        true  -> undefined
    end.

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------
a(State = #vqstate { q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
                     mode             = default,
                     len              = Len,
                     bytes            = Bytes,
                     unacked_bytes    = UnackedBytes,
                     persistent_count = PersistentCount,
                     persistent_bytes = PersistentBytes,
                     ram_msg_count    = RamMsgCount,
                     ram_bytes        = RamBytes}) ->
    E1 = ?QUEUE:is_empty(Q1),
    E2 = ?QUEUE:is_empty(Q2),
    ED = Delta#delta.count == 0,
    E3 = ?QUEUE:is_empty(Q3),
    E4 = ?QUEUE:is_empty(Q4),
    LZ = Len == 0,

    %% if q1 has messages then q3 cannot be empty. See publish/6.
    true = E1 or not E3,
    %% if q2 has messages then we have messages in delta (paged to
    %% disk). See push_alphas_to_betas/2.
    true = E2 or not ED,
    %% if delta has messages then q3 cannot be empty. This is enforced
    %% by paging, where min([segment_entry_count(), len(q3)]) messages
    %% are always kept on RAM.
    true = ED or not E3,
    %% if the queue length is 0, then q3 and q4 must be empty.
    true = LZ == (E3 and E4),

    true = Len             >= 0,
    true = Bytes           >= 0,
    true = UnackedBytes    >= 0,
    true = PersistentCount >= 0,
    true = PersistentBytes >= 0,
    true = RamMsgCount     >= 0,
    true = RamMsgCount     =< Len,
    true = RamBytes        >= 0,
    true = RamBytes        =< Bytes + UnackedBytes,

    State;
a(State = #vqstate { q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
                     mode             = lazy,
                     len              = Len,
                     bytes            = Bytes,
                     unacked_bytes    = UnackedBytes,
                     persistent_count = PersistentCount,
                     persistent_bytes = PersistentBytes,
                     ram_msg_count    = RamMsgCount,
                     ram_bytes        = RamBytes}) ->
    E1 = ?QUEUE:is_empty(Q1),
    E2 = ?QUEUE:is_empty(Q2),
    ED = Delta#delta.count == 0,
    E3 = ?QUEUE:is_empty(Q3),
    E4 = ?QUEUE:is_empty(Q4),
    LZ = Len == 0,
    L3 = ?QUEUE:len(Q3),

    %% q1 must always be empty, since q1 only gets messages during
    %% publish, but for lazy queues messages go straight to delta.
    true = E1,

    %% q2 only gets messages from q1 when push_alphas_to_betas is
    %% called for a non empty delta, which won't be the case for a
    %% lazy queue. This means q2 must always be empty.
    true = E2,

    %% q4 must always be empty, since q1 only gets messages during
    %% publish, but for lazy queues messages go straight to delta.
    true = E4,

    %% if the queue is empty, then delta is empty and q3 is empty.
    true = LZ == (ED and E3),

    %% There should be no messages in q1, q2, and q4
    true = Delta#delta.count + L3 == Len,

    true = Len             >= 0,
    true = Bytes           >= 0,
    true = UnackedBytes    >= 0,
    true = PersistentCount >= 0,
    true = PersistentBytes >= 0,
    true = RamMsgCount     >= 0,
    true = RamMsgCount     =< Len,
    true = RamBytes        >= 0,
    true = RamBytes        =< Bytes + UnackedBytes,

    State.

d(Delta = #delta { start_seq_id = Start, count = Count, end_seq_id = End })
  when Start + Count =< End ->
    Delta.

m(MsgStatus = #msg_status { is_persistent = IsPersistent,
                            msg_location  = MsgLocation,
                            index_on_disk = IndexOnDisk }) ->
    true = (not IsPersistent) or IndexOnDisk,
    true = msg_in_ram(MsgStatus) or (MsgLocation =/= memory),
    MsgStatus.

one_if(true ) -> 1;
one_if(false) -> 0.

cons_if(true,   E, L) -> [E | L];
cons_if(false, _E, L) -> L.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
gb_sets_maybe_insert(true,   Val, Set) -> gb_sets:add(Val, Set).

msg_status(Version, IsPersistent, IsDelivered, SeqId,
           Msg = #basic_message {id = MsgId}, MsgProps, IndexMaxSize) ->
    #msg_status{seq_id        = SeqId,
                msg_id        = MsgId,
                msg           = Msg,
                is_persistent = IsPersistent,
                %% This value will only be correct when the message is going out.
                %% See the set_deliver_flag/2 function.
                is_delivered  = IsDelivered,
                msg_location  = memory,
                index_on_disk = false,
                persist_to    = determine_persist_to(Version, Msg, MsgProps, IndexMaxSize),
                msg_props     = MsgProps}.

beta_msg_status({Msg = #basic_message{id = MsgId},
                 SeqId, rabbit_queue_index, MsgProps, IsPersistent}) ->
    MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent),
    MS0#msg_status{msg_id       = MsgId,
                   msg          = Msg,
                   persist_to   = queue_index,
                   msg_location = memory};

beta_msg_status({MsgId, SeqId, MsgLocation, MsgProps, IsPersistent}) ->
    MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent),
    MS0#msg_status{msg_id       = MsgId,
                   msg          = undefined,
                   persist_to   = case is_tuple(MsgLocation) of
                                      true  -> queue_store;
                                      false -> msg_store
                                  end,
                   msg_location = MsgLocation}.

beta_msg_status0(SeqId, MsgProps, IsPersistent) ->
  #msg_status{seq_id        = SeqId,
              msg           = undefined,
              is_persistent = IsPersistent,
              index_on_disk = true,
              msg_props     = MsgProps}.

trim_msg_status(MsgStatus) ->
    case persist_to(MsgStatus) of
        msg_store   -> MsgStatus#msg_status{msg = undefined};
        queue_store -> MsgStatus#msg_status{msg = undefined};
        queue_index -> MsgStatus
    end.

with_msg_store_state({MSCStateP, MSCStateT},  true, Fun) ->
    {Result, MSCStateP1} = Fun(MSCStateP),
    {Result, {MSCStateP1, MSCStateT}};
with_msg_store_state({MSCStateP, MSCStateT}, false, Fun) ->
    {Result, MSCStateT1} = Fun(MSCStateT),
    {Result, {MSCStateP, MSCStateT1}}.

with_immutable_msg_store_state(MSCState, IsPersistent, Fun) ->
    {Res, MSCState} = with_msg_store_state(MSCState, IsPersistent,
                                           fun (MSCState1) ->
                                                   {Fun(MSCState1), MSCState1}
                                           end),
    Res.

msg_store_client_init(MsgStore, MsgOnDiskFun, Callback, VHost) ->
    msg_store_client_init(MsgStore, rabbit_guid:gen(), MsgOnDiskFun,
                          Callback, VHost).

msg_store_client_init(MsgStore, Ref, MsgOnDiskFun, Callback, VHost) ->
    CloseFDsFun = msg_store_close_fds_fun(MsgStore =:= ?PERSISTENT_MSG_STORE),
    rabbit_vhost_msg_store:client_init(VHost, MsgStore,
                                       Ref, MsgOnDiskFun,
                                       fun () ->
                                           Callback(?MODULE, CloseFDsFun)
                                       end).

msg_store_write(MSCState, IsPersistent, MsgId, Msg) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) ->
              rabbit_msg_store:write_flow(MsgId, Msg, MSCState1)
      end).

msg_store_read(MSCState, IsPersistent, MsgId) ->
    with_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) ->
              rabbit_msg_store:read(MsgId, MSCState1)
      end).

msg_store_remove(MSCState, IsPersistent, MsgIds) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MCSState1) ->
              rabbit_msg_store:remove(MsgIds, MCSState1)
      end).

msg_store_close_fds(MSCState, IsPersistent) ->
    with_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:close_all_indicated(MSCState1) end).

msg_store_close_fds_fun(IsPersistent) ->
    fun (?MODULE, State = #vqstate { msg_store_clients = MSCState }) ->
            {ok, MSCState1} = msg_store_close_fds(MSCState, IsPersistent),
            State #vqstate { msg_store_clients = MSCState1 }
    end.

betas_from_index_entries(List, TransientThreshold, DelsAndAcksFun, State = #vqstate{ next_deliver_seq_id = NextDeliverSeqId0 }) ->
    {Filtered, NextDeliverSeqId, Acks, RamReadyCount, RamBytes, TransientCount, TransientBytes} =
        lists:foldr(
          fun ({_MsgOrId, SeqId, _MsgLocation, _MsgProps, IsPersistent} = M,
               {Filtered1, NextDeliverSeqId1, Acks1, RRC, RB, TC, TB} = Acc) ->
                  case SeqId < TransientThreshold andalso not IsPersistent of
                      true  -> {Filtered1,
                                next_deliver_seq_id(SeqId, NextDeliverSeqId1),
                                [SeqId | Acks1], RRC, RB, TC, TB};
                      false -> MsgStatus = m(beta_msg_status(M)),
                               HaveMsg = msg_in_ram(MsgStatus),
                               Size = msg_size(MsgStatus),
                               case is_msg_in_pending_acks(SeqId, State) of
                                   false -> {?QUEUE:in_r(MsgStatus, Filtered1),
                                             NextDeliverSeqId1, Acks1,
                                             RRC + one_if(HaveMsg),
                                             RB + one_if(HaveMsg) * Size,
                                             TC + one_if(not IsPersistent),
                                             TB + one_if(not IsPersistent) * Size};
                                   true  -> Acc %% [0]
                               end
                  end
          end, {?QUEUE:new(), NextDeliverSeqId0, [], 0, 0, 0, 0}, List),
    {Filtered, RamReadyCount, RamBytes, DelsAndAcksFun(NextDeliverSeqId, Acks, State),
     TransientCount, TransientBytes}.
%% [0] We don't increase RamBytes here, even though it pertains to
%% unacked messages too, since if HaveMsg then the message must have
%% been stored in the QI, thus the message must have been in
%% qi_pending_ack, thus it must already have been in RAM.

%% We increase the next_deliver_seq_id only when the next
%% message (next seq_id) was delivered.
next_deliver_seq_id(SeqId, NextDeliverSeqId)
        when SeqId =:= NextDeliverSeqId ->
    NextDeliverSeqId + 1;
next_deliver_seq_id(_, NextDeliverSeqId) ->
    NextDeliverSeqId.

is_msg_in_pending_acks(SeqId, #vqstate { ram_pending_ack  = RPA,
                                         disk_pending_ack = DPA,
                                         qi_pending_ack   = QPA }) ->
    (gb_trees:is_defined(SeqId, RPA) orelse
     gb_trees:is_defined(SeqId, DPA) orelse
     gb_trees:is_defined(SeqId, QPA)).

expand_delta(SeqId, ?BLANK_DELTA_PATTERN(X), IsPersistent) ->
    d(#delta { start_seq_id = SeqId, count = 1, end_seq_id = SeqId + 1,
               transient = one_if(not IsPersistent)});
expand_delta(SeqId, #delta { start_seq_id = StartSeqId,
                             count        = Count,
                             transient    = Transient } = Delta,
             IsPersistent )
  when SeqId < StartSeqId ->
    d(Delta #delta { start_seq_id = SeqId, count = Count + 1,
                     transient = Transient + one_if(not IsPersistent)});
expand_delta(SeqId, #delta { count        = Count,
                             end_seq_id   = EndSeqId,
                             transient    = Transient } = Delta,
             IsPersistent)
  when SeqId >= EndSeqId ->
    d(Delta #delta { count = Count + 1, end_seq_id = SeqId + 1,
                     transient = Transient + one_if(not IsPersistent)});
expand_delta(_SeqId, #delta { count       = Count,
                              transient   = Transient } = Delta,
             IsPersistent ) ->
    d(Delta #delta { count = Count + 1,
                     transient = Transient + one_if(not IsPersistent) }).

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(QueueVsn, IsDurable, IndexMod, IndexState, StoreState, DeltaCount, DeltaBytes, Terms,
     PersistentClient, TransientClient, VHost) ->
    {LowSeqId, HiSeqId, IndexState1} = IndexMod:bounds(IndexState),

    {NextSeqId, NextDeliverSeqId, DeltaCount1, DeltaBytes1} =
        case Terms of
            non_clean_shutdown -> {HiSeqId, HiSeqId, DeltaCount, DeltaBytes};
            _                  -> NextSeqId0 = proplists:get_value(next_seq_id,
                                                                   Terms, 0),
                                  {NextSeqId0,
                                   proplists:get_value(next_deliver_seq_id,
                                                       Terms, NextSeqId0),
                                   proplists:get_value(persistent_count,
                                                       Terms, DeltaCount),
                                   proplists:get_value(persistent_bytes,
                                                       Terms, DeltaBytes)}
        end,
    Delta = case DeltaCount1 == 0 andalso DeltaCount /= undefined of
                true  -> ?BLANK_DELTA;
                false -> d(#delta { start_seq_id = LowSeqId,
                                    count        = DeltaCount1,
                                    transient    = 0,
                                    end_seq_id   = HiSeqId })
            end,
    Now = erlang:monotonic_time(),
    IoBatchSize = rabbit_misc:get_env(rabbit, msg_store_io_batch_size,
                                      ?IO_BATCH_SIZE),

    {ok, IndexMaxSize} = application:get_env(
                           rabbit, queue_index_embed_msgs_below),
    State = #vqstate {
      q1                  = ?QUEUE:new(),
      q2                  = ?QUEUE:new(),
      delta               = Delta,
      q3                  = ?QUEUE:new(),
      q4                  = ?QUEUE:new(),
      next_seq_id         = NextSeqId,
      next_deliver_seq_id = NextDeliverSeqId,
      ram_pending_ack     = gb_trees:empty(),
      disk_pending_ack    = gb_trees:empty(),
      qi_pending_ack      = gb_trees:empty(),
      index_mod           = IndexMod,
      index_state         = IndexState1,
      store_state         = StoreState,
      msg_store_clients   = {PersistentClient, TransientClient},
      durable             = IsDurable,
      transient_threshold = NextSeqId,
      qi_embed_msgs_below = IndexMaxSize,

      len                 = DeltaCount1,
      persistent_count    = DeltaCount1,
      bytes               = DeltaBytes1,
      persistent_bytes    = DeltaBytes1,
      delta_transient_bytes = 0,

      target_ram_count    = infinity,
      ram_msg_count       = 0,
      ram_msg_count_prev  = 0,
      ram_ack_count_prev  = 0,
      ram_bytes           = 0,
      unacked_bytes       = 0,
      out_counter         = 0,
      in_counter          = 0,
      rates               = blank_rates(Now),
      msgs_on_disk        = gb_sets:new(),
      msg_indices_on_disk = gb_sets:new(),
      unconfirmed         = gb_sets:new(),
      confirmed           = gb_sets:new(),
      ack_out_counter     = 0,
      ack_in_counter      = 0,
      disk_read_count     = 0,
      disk_write_count    = 0,

      io_batch_size       = IoBatchSize,

      mode                = default,
      version             = QueueVsn,
      memory_reduction_run_count = 0,
      virtual_host        = VHost},
    a(maybe_deltas_to_betas(State)).

blank_rates(Now) ->
    #rates { in        = 0.0,
             out       = 0.0,
             ack_in    = 0.0,
             ack_out   = 0.0,
             timestamp = Now}.

in_r(MsgStatus = #msg_status { msg = undefined },
     State = #vqstate { mode = default, q3 = Q3, q4 = Q4 }) ->
    case ?QUEUE:is_empty(Q4) of
        true  -> State #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3) };
        false -> {Msg, State1 = #vqstate { q4 = Q4a }} =
                     read_msg(MsgStatus, State),
                 MsgStatus1 = MsgStatus#msg_status{msg = Msg},
                 stats(ready0, {MsgStatus, MsgStatus1}, 0,
                       State1 #vqstate { q4 = ?QUEUE:in_r(MsgStatus1, Q4a) })
    end;
in_r(MsgStatus,
     State = #vqstate { mode = default, q4 = Q4 }) ->
    State #vqstate { q4 = ?QUEUE:in_r(MsgStatus, Q4) };
%% lazy queues
in_r(MsgStatus = #msg_status { seq_id = SeqId, is_persistent = IsPersistent },
     State = #vqstate { mode = lazy, q3 = Q3, delta = Delta}) ->
    case ?QUEUE:is_empty(Q3) of
        true  ->
            {_MsgStatus1, State1} =
                maybe_write_to_disk(true, true, MsgStatus, State),
            State2 = stats(ready0, {MsgStatus, none}, 1, State1),
            Delta1 = expand_delta(SeqId, Delta, IsPersistent),
            State2 #vqstate{ delta = Delta1};
        false ->
            State #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3) }
    end.

queue_out(State = #vqstate { mode = default, q4 = Q4 }) ->
    case ?QUEUE:out(Q4) of
        {empty, _Q4} ->
            case fetch_from_q3(State) of
                {empty, _State1} = Result     -> Result;
                {loaded, {MsgStatus, State1}} -> {{value, set_deliver_flag(State, MsgStatus)}, State1}
            end;
        {{value, MsgStatus}, Q4a} ->
            {{value, set_deliver_flag(State, MsgStatus)}, State #vqstate { q4 = Q4a }}
    end;
%% lazy queues
queue_out(State = #vqstate { mode = lazy }) ->
    case fetch_from_q3(State) of
        {empty, _State1} = Result     -> Result;
        {loaded, {MsgStatus, State1}} -> {{value, set_deliver_flag(State, MsgStatus)}, State1}
    end.

set_deliver_flag(#vqstate{ next_deliver_seq_id = NextDeliverSeqId },
                 MsgStatus = #msg_status{ seq_id = SeqId }) ->
    MsgStatus#msg_status{ is_delivered = SeqId < NextDeliverSeqId }.

read_msg(#msg_status{seq_id        = SeqId,
                     msg           = undefined,
                     msg_id        = MsgId,
                     is_persistent = IsPersistent,
                     msg_location  = MsgLocation}, State) ->
    read_msg(SeqId, MsgId, IsPersistent, MsgLocation, State);
read_msg(#msg_status{msg = Msg}, State) ->
    {Msg, State}.

read_msg(SeqId, _, _, MsgLocation, State = #vqstate{ store_state = StoreState0 })
        when is_tuple(MsgLocation) ->
    {Msg, StoreState} = rabbit_classic_queue_store_v2:read(SeqId, MsgLocation, StoreState0),
    {Msg, State#vqstate{ store_state = StoreState }};
read_msg(_, MsgId, IsPersistent, rabbit_msg_store, State = #vqstate{msg_store_clients = MSCState,
                                                                    disk_read_count   = Count}) ->
    {{ok, Msg = #basic_message {}}, MSCState1} =
        msg_store_read(MSCState, IsPersistent, MsgId),
    {Msg, State #vqstate {msg_store_clients = MSCState1,
                          disk_read_count   = Count + 1}}.

stats(Signs, Statuses, DeltaPaged, State) ->
    stats0(expand_signs(Signs), expand_statuses(Statuses), DeltaPaged, State).

expand_signs(ready0)        -> {0, 0, true};
expand_signs(lazy_pub)      -> {1, 0, true};
expand_signs({A, B})        -> {A, B, false}.

expand_statuses({none, A})    -> {false,         msg_in_ram(A), A};
expand_statuses({B,    none}) -> {msg_in_ram(B), false,         B};
expand_statuses({lazy, A})    -> {false        , false,         A};
expand_statuses({B,    A})    -> {msg_in_ram(B), msg_in_ram(A), B}.

%% In this function at least, we are religious: the variable name
%% contains "Ready" or "Unacked" iff that is what it counts. If
%% neither is present it counts both.
stats0({DeltaReady, DeltaUnacked, ReadyMsgPaged},
       {InRamBefore, InRamAfter, MsgStatus}, DeltaPaged,
       State = #vqstate{len              = ReadyCount,
                        bytes            = ReadyBytes,
                        ram_msg_count    = RamReadyCount,
                        persistent_count = PersistentCount,
                        unacked_bytes    = UnackedBytes,
                        ram_bytes        = RamBytes,
                        delta_transient_bytes = DeltaBytes,
                        persistent_bytes = PersistentBytes}) ->
    S = msg_size(MsgStatus),
    DeltaTotal = DeltaReady + DeltaUnacked,
    DeltaRam = case {InRamBefore, InRamAfter} of
                   {false, false} ->  0;
                   {false, true}  ->  1;
                   {true,  false} -> -1;
                   {true,  true}  ->  0
               end,
    DeltaRamReady = case DeltaReady of
                        1                    -> one_if(InRamAfter);
                        -1                   -> -one_if(InRamBefore);
                        0 when ReadyMsgPaged -> DeltaRam;
                        0                    -> 0
                    end,
    DeltaPersistent = DeltaTotal * one_if(MsgStatus#msg_status.is_persistent),
    State#vqstate{len               = ReadyCount      + DeltaReady,
                  ram_msg_count     = RamReadyCount   + DeltaRamReady,
                  persistent_count  = PersistentCount + DeltaPersistent,
                  bytes             = ReadyBytes      + DeltaReady       * S,
                  unacked_bytes     = UnackedBytes    + DeltaUnacked     * S,
                  ram_bytes         = RamBytes        + DeltaRam         * S,
                  persistent_bytes  = PersistentBytes + DeltaPersistent  * S,
                  delta_transient_bytes = DeltaBytes  + DeltaPaged * one_if(not MsgStatus#msg_status.is_persistent) * S}.

msg_size(#msg_status{msg_props = #message_properties{size = Size}}) -> Size.

msg_in_ram(#msg_status{msg = Msg}) -> Msg =/= undefined.

%% first param: AckRequired
remove(true, MsgStatus = #msg_status {
               seq_id        = SeqId },
       State = #vqstate {next_deliver_seq_id = NextDeliverSeqId,
                         out_counter         = OutCount,
                         index_state         = IndexState1 }) ->

    State1 = record_pending_ack(
               MsgStatus #msg_status {
                 is_delivered = true }, State),

    State2 = stats({-1, 1}, {MsgStatus, MsgStatus}, 0, State1),

    {SeqId, maybe_update_rates(
              State2 #vqstate {next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                               out_counter         = OutCount + 1,
                               index_state         = IndexState1})};

%% This function body has the same behaviour as remove_queue_entries/3
%% but instead of removing messages based on a ?QUEUE, this removes
%% just one message, the one referenced by the MsgStatus provided.
remove(false, MsgStatus = #msg_status {
                seq_id        = SeqId,
                msg_id        = MsgId,
                is_persistent = IsPersistent,
                msg_location  = MsgLocation,
                index_on_disk = IndexOnDisk },
       State = #vqstate {next_deliver_seq_id = NextDeliverSeqId,
                         out_counter         = OutCount,
                         index_mod           = IndexMod,
                         index_state         = IndexState1,
                         store_state         = StoreState0,
                         msg_store_clients   = MSCState}) ->

    %% Remove from msg_store and queue index, if necessary
    StoreState1 = case MsgLocation of
        ?IN_SHARED_STORE -> ok = msg_store_remove(MSCState, IsPersistent, [MsgId]), StoreState0;
        ?IN_QUEUE_STORE  -> rabbit_classic_queue_store_v2:remove(SeqId, StoreState0);
        ?IN_QUEUE_INDEX  -> StoreState0;
        ?IN_MEMORY       -> StoreState0
    end,

    {DeletedSegments, IndexState2} =
        case IndexOnDisk of
            true  -> IndexMod:ack([SeqId], IndexState1);
            false -> {[], IndexState1}
        end,

    StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState1),

    State1 = stats({-1, 0}, {MsgStatus, none}, 0, State),

    {undefined, maybe_update_rates(
                  State1 #vqstate {next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                                   out_counter         = OutCount + 1,
                                   index_state         = IndexState2,
                                   store_state         = StoreState })}.

%% This function exists as a way to improve dropwhile/2
%% performance. The idea of having this function is to optimise calls
%% to rabbit_queue_index by batching delivers and acks, instead of
%% sending them one by one.
%%
%% Instead of removing every message as their are popped from the
%% queue, it first accumulates them and then removes them by calling
%% remove_queue_entries/3, since the behaviour of
%% remove_queue_entries/3 when used with
%% process_delivers_and_acks_fun(deliver_and_ack) is the same as
%% calling remove(false, MsgStatus, State).
%%
%% remove/3 also updates the out_counter in every call, but here we do
%% it just once at the end.
%%
%% @todo This function is really bad. If there are 1 million messages
%%       expired, it will first collect the 1 million messages and then
%%       process them. It should probably limit the number of messages
%%       it removes at once and loop until satisfied instead. It could
%%       also let the index first figure out until what seq_id() to read
%%       (since the index has expiration encoded, it could use binary
%%       search to find where it should stop reading) and then in a second
%%       step do the reading with a limit for each read and drop only that.
remove_by_predicate(Pred, State = #vqstate {out_counter = OutCount}) ->
    {MsgProps, QAcc, State1} =
        collect_by_predicate(Pred, ?QUEUE:new(), State),
    State2 =
        remove_queue_entries(
          QAcc, process_delivers_and_acks_fun(deliver_and_ack), State1),
    %% maybe_update_rates/1 is called in remove/2 for every
    %% message. Since we update out_counter only once, we call it just
    %% there.
    {MsgProps, maybe_update_rates(
                 State2 #vqstate {
                   out_counter = OutCount + ?QUEUE:len(QAcc)})}.

%% This function exists as a way to improve fetchwhile/4
%% performance. The idea of having this function is to optimise calls
%% to rabbit_queue_index by batching delivers, instead of sending them
%% one by one.
%%
%% Fun is the function passed to fetchwhile/4 that's
%% applied to every fetched message and used to build the fetchwhile/4
%% result accumulator FetchAcc.
%%
%% @todo See todo in remove_by_predicate/2 function.
fetch_by_predicate(Pred, Fun, FetchAcc,
                   State = #vqstate {
                              index_state = IndexState1,
                              out_counter = OutCount}) ->
    {MsgProps, QAcc, State1} =
        collect_by_predicate(Pred, ?QUEUE:new(), State),

    {NextDeliverSeqId, FetchAcc1, State2} =
        process_queue_entries(QAcc, Fun, FetchAcc, State1),

    {MsgProps, FetchAcc1, maybe_update_rates(
                            State2 #vqstate {
                              next_deliver_seq_id = NextDeliverSeqId,
                              index_state         = IndexState1,
                              out_counter         = OutCount + ?QUEUE:len(QAcc)})}.

%% We try to do here the same as what remove(true, State) does but
%% processing several messages at the same time. The idea is to
%% optimize rabbit_queue_index:deliver/2 calls by sending a list of
%% SeqIds instead of one by one, thus process_queue_entries1 will
%% accumulate the required deliveries, will record_pending_ack for
%% each message, and will update stats, like remove/2 does.
%%
%% For the meaning of Fun and FetchAcc arguments see
%% fetch_by_predicate/4 above.
process_queue_entries(Q, Fun, FetchAcc, State = #vqstate{ next_deliver_seq_id = NextDeliverSeqId }) ->
    ?QUEUE:foldl(fun (MsgStatus, Acc) ->
                         process_queue_entries1(MsgStatus, Fun, Acc)
                 end,
                 {NextDeliverSeqId, FetchAcc, State}, Q).

process_queue_entries1(
  #msg_status { seq_id = SeqId } = MsgStatus,
  Fun,
  {NextDeliverSeqId, FetchAcc, State}) ->
    {Msg, State1} = read_msg(MsgStatus, State),
    State2 = record_pending_ack(
               MsgStatus #msg_status {
                 is_delivered = true }, State1),
    {next_deliver_seq_id(SeqId, NextDeliverSeqId),
     Fun(Msg, SeqId, FetchAcc),
     stats({-1, 1}, {MsgStatus, MsgStatus}, 0, State2)}.

collect_by_predicate(Pred, QAcc, State) ->
    case queue_out(State) of
        {empty, State1} ->
            {undefined, QAcc, State1};
        {{value, MsgStatus = #msg_status { msg_props = MsgProps }}, State1} ->
            case Pred(MsgProps) of
                true  -> collect_by_predicate(Pred, ?QUEUE:in(MsgStatus, QAcc),
                                              State1);
                false -> {MsgProps, QAcc, in_r(MsgStatus, State1)}
            end
    end.

%%----------------------------------------------------------------------------
%% Helpers for Public API purge/1 function
%%----------------------------------------------------------------------------

%% The difference between purge_when_pending_acks/1
%% vs. purge_and_index_reset/1 is that the first one issues a deliver
%% and an ack to the queue index for every message that's being
%% removed, while the later just resets the queue index state.
purge_when_pending_acks(State) ->
    State1 = purge1(process_delivers_and_acks_fun(deliver_and_ack), State),
    a(State1).

purge_and_index_reset(State) ->
    State1 = purge1(process_delivers_and_acks_fun(none), State),
    %% @todo Also reset the store.
    a(reset_qi_state(State1)).

%% This function removes messages from each of {q1, q2, q3, q4}.
%%
%% With remove_queue_entries/3 q1 and q4 are emptied, while q2 and q3
%% are specially handled by purge_betas_and_deltas/2.
%%
%% purge_betas_and_deltas/2 loads messages from the queue index,
%% filling up q3 and in some cases moving messages form q2 to q3 while
%% resetting q2 to an empty queue (see maybe_deltas_to_betas/2). The
%% messages loaded into q3 are removed by calling
%% remove_queue_entries/3 until there are no more messages to be read
%% from the queue index. Messages are read in batches from the queue
%% index.
purge1(AfterFun, State = #vqstate { q4 = Q4}) ->
    State1 = remove_queue_entries(Q4, AfterFun, State),

    State2 = #vqstate {q1 = Q1} =
        purge_betas_and_deltas(AfterFun, State1#vqstate{q4 = ?QUEUE:new()}),

    State3 = remove_queue_entries(Q1, AfterFun, State2),

    a(State3#vqstate{q1 = ?QUEUE:new()}).

reset_qi_state(State = #vqstate{index_mod   = IndexMod,
                                index_state = IndexState}) ->
    State#vqstate{index_state = IndexMod:reset_state(IndexState)}.

is_pending_ack_empty(State) ->
    count_pending_acks(State) =:= 0.

is_unconfirmed_empty(#vqstate { unconfirmed = UC }) ->
    gb_sets:is_empty(UC).

count_pending_acks(#vqstate { ram_pending_ack   = RPA,
                              disk_pending_ack  = DPA,
                              qi_pending_ack    = QPA }) ->
    gb_trees:size(RPA) + gb_trees:size(DPA) + gb_trees:size(QPA).

purge_betas_and_deltas(DelsAndAcksFun, State = #vqstate { mode = Mode }) ->
    State0 = #vqstate { q3 = Q3 } =
        case Mode of
            lazy -> maybe_deltas_to_betas(DelsAndAcksFun, State);
            _    -> State
        end,

    case ?QUEUE:is_empty(Q3) of
        true  -> State0;
        false -> State1 = remove_queue_entries(Q3, DelsAndAcksFun, State0),
                 purge_betas_and_deltas(DelsAndAcksFun,
                                        maybe_deltas_to_betas(
                                          DelsAndAcksFun,
                                          State1#vqstate{q3 = ?QUEUE:new()}))
    end.

remove_queue_entries(Q, DelsAndAcksFun,
                     State = #vqstate{next_deliver_seq_id = NextDeliverSeqId0, msg_store_clients = MSCState}) ->
    {MsgIdsByStore, NextDeliverSeqId, Acks, State1} =
        ?QUEUE:foldl(fun remove_queue_entries1/2,
                     {maps:new(), NextDeliverSeqId0, [], State}, Q),
    remove_vhost_msgs_by_id(MsgIdsByStore, MSCState),
    DelsAndAcksFun(NextDeliverSeqId, Acks, State1).

remove_queue_entries1(
  #msg_status { msg_id = MsgId, seq_id = SeqId,
                msg_location = MsgLocation, index_on_disk = IndexOnDisk,
                is_persistent = IsPersistent} = MsgStatus,
  {MsgIdsByStore, NextDeliverSeqId, Acks, State}) ->
    {case MsgLocation of
         ?IN_SHARED_STORE -> rabbit_misc:maps_cons(IsPersistent, MsgId, MsgIdsByStore);
         _ -> MsgIdsByStore
     end,
     next_deliver_seq_id(SeqId, NextDeliverSeqId),
     cons_if(IndexOnDisk, SeqId, Acks),
     stats({-1, 0}, {MsgStatus, none}, 0, State)}.

process_delivers_and_acks_fun(deliver_and_ack) ->
    fun (NextDeliverSeqId, Acks, State = #vqstate { index_mod   = IndexMod,
                                                    index_state = IndexState,
                                                    store_state = StoreState0}) ->
            %% We do not send delivers to the v1 index because
            %% we've already done so when publishing.
            {DeletedSegments, IndexState1} = IndexMod:ack(Acks, IndexState),

            StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState0),

            State #vqstate { index_state         = IndexState1,
                             store_state         = StoreState,
                             %% We indiscriminately update because we already took care
                             %% of calling next_deliver_seq_id/2 in the functions that
                             %% end up calling this fun.
                             next_deliver_seq_id = NextDeliverSeqId }
    end;
process_delivers_and_acks_fun(_) ->
    fun (NextDeliverSeqId, _, State) ->
            State #vqstate { next_deliver_seq_id = NextDeliverSeqId }
    end.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

publish1(Msg = #basic_message { is_persistent = IsPersistent, id = MsgId },
         MsgProps = #message_properties { needs_confirming = NeedsConfirming },
         IsDelivered, _ChPid, _Flow, PersistFun,
         State = #vqstate { q1 = Q1, q3 = Q3, q4 = Q4,
                            mode                = default,
                            version             = Version,
                            qi_embed_msgs_below = IndexMaxSize,
                            next_seq_id         = SeqId,
                            next_deliver_seq_id = NextDeliverSeqId,
                            in_counter          = InCount,
                            durable             = IsDurable,
                            unconfirmed         = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(Version, IsPersistent1, IsDelivered, SeqId, Msg, MsgProps, IndexMaxSize),
    {MsgStatus1, State1} = PersistFun(false, false, MsgStatus, State),
    State2 = case ?QUEUE:is_empty(Q3) of
                 false -> State1 #vqstate { q1 = ?QUEUE:in(m(MsgStatus1), Q1) };
                 true  -> State1 #vqstate { q4 = ?QUEUE:in(m(MsgStatus1), Q4) }
             end,
    InCount1 = InCount + 1,
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    stats({1, 0}, {none, MsgStatus1}, 0,
          State2#vqstate{ next_seq_id         = SeqId + 1,
                          next_deliver_seq_id = maybe_next_deliver_seq_id(SeqId, NextDeliverSeqId, IsDelivered),
                          in_counter          = InCount1,
                          unconfirmed         = UC1 });
publish1(Msg = #basic_message { is_persistent = IsPersistent, id = MsgId },
             MsgProps = #message_properties { needs_confirming = NeedsConfirming },
             IsDelivered, _ChPid, _Flow, PersistFun,
             State = #vqstate { mode                = lazy,
                                version             = Version,
                                qi_embed_msgs_below = IndexMaxSize,
                                next_seq_id         = SeqId,
                                next_deliver_seq_id = NextDeliverSeqId,
                                in_counter          = InCount,
                                durable             = IsDurable,
                                unconfirmed         = UC,
                                delta               = Delta}) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(Version, IsPersistent1, IsDelivered, SeqId, Msg, MsgProps, IndexMaxSize),
    {MsgStatus1, State1} = PersistFun(true, true, MsgStatus, State),
    Delta1 = expand_delta(SeqId, Delta, IsPersistent),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    stats(lazy_pub, {lazy, m(MsgStatus1)}, 1,
          State1#vqstate{ delta               = Delta1,
                          next_seq_id         = SeqId + 1,
                          next_deliver_seq_id = maybe_next_deliver_seq_id(SeqId, NextDeliverSeqId, IsDelivered),
                          in_counter          = InCount + 1,
                          unconfirmed         = UC1}).

%% Only attempt to increase the next_deliver_seq_id for delivered messages.
maybe_next_deliver_seq_id(SeqId, NextDeliverSeqId, true) ->
    next_deliver_seq_id(SeqId, NextDeliverSeqId);
maybe_next_deliver_seq_id(_, NextDeliverSeqId, false) ->
    NextDeliverSeqId.

batch_publish1({Msg, MsgProps, IsDelivered}, {ChPid, Flow, State}) ->
    {ChPid, Flow, publish1(Msg, MsgProps, IsDelivered, ChPid, Flow,
                           fun maybe_prepare_write_to_disk/4, State)}.

publish_delivered1(Msg = #basic_message { is_persistent = IsPersistent,
                                          id = MsgId },
                   MsgProps = #message_properties {
                                 needs_confirming = NeedsConfirming },
                   _ChPid, _Flow, PersistFun,
                   State = #vqstate { mode                = default,
                                      version             = Version,
                                      qi_embed_msgs_below = IndexMaxSize,
                                      next_seq_id         = SeqId,
                                      next_deliver_seq_id = NextDeliverSeqId,
                                      out_counter         = OutCount,
                                      in_counter          = InCount,
                                      durable             = IsDurable,
                                      unconfirmed         = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(Version, IsPersistent1, true, SeqId, Msg, MsgProps, IndexMaxSize),
    {MsgStatus1, State1} = PersistFun(false, false, MsgStatus, State),
    State2 = record_pending_ack(m(MsgStatus1), State1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    State3 = stats({0, 1}, {none, MsgStatus1}, 0,
                   State2 #vqstate { next_seq_id         = SeqId    + 1,
                                     next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                                     out_counter         = OutCount + 1,
                                     in_counter          = InCount  + 1,
                                     unconfirmed         = UC1 }),
    {SeqId, State3};
publish_delivered1(Msg = #basic_message { is_persistent = IsPersistent,
                                          id = MsgId },
                   MsgProps = #message_properties {
                                 needs_confirming = NeedsConfirming },
                   _ChPid, _Flow, PersistFun,
                   State = #vqstate { mode                = lazy,
                                      version             = Version,
                                      qi_embed_msgs_below = IndexMaxSize,
                                      next_seq_id         = SeqId,
                                      next_deliver_seq_id = NextDeliverSeqId,
                                      out_counter         = OutCount,
                                      in_counter          = InCount,
                                      durable             = IsDurable,
                                      unconfirmed         = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(Version, IsPersistent1, true, SeqId, Msg, MsgProps, IndexMaxSize),
    {MsgStatus1, State1} = PersistFun(true, true, MsgStatus, State),
    State2 = record_pending_ack(m(MsgStatus1), State1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    State3 = stats({0, 1}, {none, MsgStatus1}, 0,
                   State2 #vqstate { next_seq_id         = SeqId    + 1,
                                     next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                                     out_counter         = OutCount + 1,
                                     in_counter          = InCount  + 1,
                                     unconfirmed         = UC1 }),
    {SeqId, State3}.

batch_publish_delivered1({Msg, MsgProps}, {ChPid, Flow, SeqIds, State}) ->
    {SeqId, State1} =
        publish_delivered1(Msg, MsgProps, ChPid, Flow,
                           fun maybe_prepare_write_to_disk/4,
                           State),
    {ChPid, Flow, [SeqId | SeqIds], State1}.

maybe_write_msg_to_disk(Force, MsgStatus = #msg_status {
                                 seq_id = SeqId,
                                 msg = Msg, msg_id = MsgId,
                                 is_persistent = IsPersistent,
                                 msg_location = ?IN_MEMORY,
                                 msg_props = Props },
                        State = #vqstate{ store_state = StoreState0,
                                          msg_store_clients = MSCState,
                                          disk_write_count  = Count})
  when Force orelse IsPersistent ->
    case persist_to(MsgStatus) of
        msg_store   -> ok = msg_store_write(MSCState, IsPersistent, MsgId,
                                            prepare_to_store(Msg)),
                       {MsgStatus#msg_status{msg_location = ?IN_SHARED_STORE},
                        State#vqstate{disk_write_count = Count + 1}};
        queue_store -> {MsgLocation, StoreState} = rabbit_classic_queue_store_v2:write(SeqId, prepare_to_store(Msg), Props, StoreState0),
                       {MsgStatus#msg_status{ msg_location = MsgLocation },
                        State#vqstate{ store_state = StoreState,
                                       disk_write_count = Count + 1}};
        queue_index -> {MsgStatus, State}
    end;
maybe_write_msg_to_disk(_Force, MsgStatus, State) ->
    {MsgStatus, State}.

%% Due to certain optimisations made inside
%% rabbit_queue_index:pre_publish/7 we need to have two separate
%% functions for index persistence. This one is only used when paging
%% during memory pressure. We didn't want to modify
%% maybe_write_index_to_disk/3 because that function is used in other
%% places.
maybe_batch_write_index_to_disk(_Force,
                                MsgStatus = #msg_status {
                                  index_on_disk = true }, State) ->
    {MsgStatus, State};
maybe_batch_write_index_to_disk(Force,
                                MsgStatus = #msg_status {
                                  msg           = Msg,
                                  msg_id        = MsgId,
                                  seq_id        = SeqId,
                                  is_persistent = IsPersistent,
                                  msg_location  = MsgLocation,
                                  msg_props     = MsgProps},
                                State = #vqstate {
                                           target_ram_count = TargetRamCount,
                                           disk_write_count = DiskWriteCount,
                                           index_mod        = IndexMod,
                                           index_state      = IndexState})
  when Force orelse IsPersistent ->
    {MsgOrId, DiskWriteCount1} =
        case persist_to(MsgStatus) of
            msg_store   -> {MsgId, DiskWriteCount};
            queue_store -> {MsgId, DiskWriteCount};
            queue_index -> {prepare_to_store(Msg), DiskWriteCount + 1}
        end,
    IndexState1 = case IndexMod of
        %% The old index needs IsDelivered to apply some of its optimisations.
        %% But because the deliver tracking is now in the queue we always pass 'true'.
        %% It also does not need the location so it is not given here.
        rabbit_queue_index ->
            IndexMod:pre_publish(
                            MsgOrId, SeqId, MsgProps, IsPersistent, true,
                            TargetRamCount, IndexState);
        _ ->
            IndexMod:pre_publish(
                            MsgOrId, SeqId, MsgLocation, MsgProps, IsPersistent,
                            TargetRamCount, IndexState)
    end,
    {MsgStatus#msg_status{index_on_disk = true},
     State#vqstate{index_state      = IndexState1,
                   disk_write_count = DiskWriteCount1}};
maybe_batch_write_index_to_disk(_Force, MsgStatus, State) ->
    {MsgStatus, State}.

maybe_write_index_to_disk(_Force, MsgStatus = #msg_status {
                                    index_on_disk = true }, State) ->
    {MsgStatus, State};
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
                                   msg           = Msg,
                                   msg_id        = MsgId,
                                   seq_id        = SeqId,
                                   is_persistent = IsPersistent,
                                   msg_location  = MsgLocation,
                                   msg_props     = MsgProps},
                          State = #vqstate{target_ram_count = TargetRamCount,
                                           disk_write_count = DiskWriteCount,
                                           index_mod        = IndexMod,
                                           index_state      = IndexState})
  when Force orelse IsPersistent ->
    {MsgOrId, DiskWriteCount1} =
        case persist_to(MsgStatus) of
            msg_store   -> {MsgId, DiskWriteCount};
            queue_store -> {MsgId, DiskWriteCount};
            queue_index -> {prepare_to_store(Msg), DiskWriteCount + 1}
        end,
    IndexState2 = IndexMod:publish(
                    MsgOrId, SeqId, MsgLocation, MsgProps, IsPersistent, TargetRamCount,
                    IndexState),
    %% We always deliver messages when the old index is used.
    %% We are actually tracking message deliveries per-queue
    %% but the old index expects delivers to be handled
    %% per-message. Always delivering on publish prevents
    %% issues related to delivers.
    IndexState3 = case IndexMod of
        rabbit_queue_index -> IndexMod:deliver([SeqId], IndexState2);
        _ -> IndexState2
    end,
    {MsgStatus#msg_status{index_on_disk = true},
     State#vqstate{index_state      = IndexState3,
                   disk_write_count = DiskWriteCount1}};

maybe_write_index_to_disk(_Force, MsgStatus, State) ->
    {MsgStatus, State}.

maybe_write_to_disk(ForceMsg, ForceIndex, MsgStatus, State) ->
    {MsgStatus1, State1} = maybe_write_msg_to_disk(ForceMsg, MsgStatus, State),
    maybe_write_index_to_disk(ForceIndex, MsgStatus1, State1).

maybe_prepare_write_to_disk(ForceMsg, ForceIndex, MsgStatus, State) ->
    {MsgStatus1, State1} = maybe_write_msg_to_disk(ForceMsg, MsgStatus, State),
    maybe_batch_write_index_to_disk(ForceIndex, MsgStatus1, State1).

determine_persist_to(Version,
                     #basic_message{
                        content = #content{properties     = Props,
                                           properties_bin = PropsBin}},
                     #message_properties{size = BodySize},
                     IndexMaxSize) ->
    %% The >= is so that you can set the env to 0 and never persist
    %% to the index.
    %%
    %% We want this to be fast, so we avoid size(term_to_binary())
    %% here, or using the term size estimation from truncate.erl, both
    %% of which are too slow. So instead, if the message body size
    %% goes over the limit then we avoid any other checks.
    %%
    %% If it doesn't we need to decide if the properties will push
    %% it past the limit. If we have the encoded properties (usual
    %% case) we can just check their size. If we don't (message came
    %% via the direct client), we make a guess based on the number of
    %% headers.
    case BodySize >= IndexMaxSize of
        true  -> msg_store;
        false -> Est = case is_binary(PropsBin) of
                           true  -> BodySize + size(PropsBin);
                           false -> #'P_basic'{headers = Hs} = Props,
                                    case Hs of
                                        undefined -> 0;
                                        _         -> length(Hs)
                                    end * ?HEADER_GUESS_SIZE + BodySize
                       end,
                 case Est >= IndexMaxSize of
                     true                     -> msg_store;
                     false when Version =:= 1 -> queue_index;
                     false when Version =:= 2 -> queue_store
                 end
    end.

persist_to(#msg_status{persist_to = To}) -> To.

prepare_to_store(Msg) ->
    Msg#basic_message{
      %% don't persist any recoverable decoded properties
      content = rabbit_binary_parser:clear_decoded_content(
                  Msg #basic_message.content)}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#msg_status { seq_id = SeqId } = MsgStatus,
                   State = #vqstate { ram_pending_ack  = RPA,
                                      disk_pending_ack = DPA,
                                      qi_pending_ack   = QPA,
                                      ack_in_counter   = AckInCount}) ->
    Insert = fun (Tree) -> gb_trees:insert(SeqId, MsgStatus, Tree) end,
    {RPA1, DPA1, QPA1} =
        case {msg_in_ram(MsgStatus), persist_to(MsgStatus)} of
            {false, _}           -> {RPA, Insert(DPA), QPA};
            {_,     queue_index} -> {RPA, DPA, Insert(QPA)};
            {_,     queue_store} -> {Insert(RPA), DPA, QPA};
            {_,     msg_store}   -> {Insert(RPA), DPA, QPA}
        end,
    State #vqstate { ram_pending_ack  = RPA1,
                     disk_pending_ack = DPA1,
                     qi_pending_ack   = QPA1,
                     ack_in_counter   = AckInCount + 1}.

lookup_pending_ack(SeqId, #vqstate { ram_pending_ack  = RPA,
                                     disk_pending_ack = DPA,
                                     qi_pending_ack   = QPA}) ->
    case gb_trees:lookup(SeqId, RPA) of
        {value, V} -> V;
        none       -> case gb_trees:lookup(SeqId, DPA) of
                          {value, V} -> V;
                          none       -> gb_trees:get(SeqId, QPA)
                      end
    end.

%% First parameter = UpdateStats
remove_pending_ack(true, SeqId, State) ->
    case remove_pending_ack(false, SeqId, State) of
        {none, _} ->
            {none, State};
        {MsgStatus, State1} ->
            {MsgStatus, stats({0, -1}, {MsgStatus, none}, 0, State1)}
    end;
remove_pending_ack(false, SeqId, State = #vqstate{ram_pending_ack  = RPA,
                                                  disk_pending_ack = DPA,
                                                  qi_pending_ack   = QPA}) ->
    case gb_trees:lookup(SeqId, RPA) of
        {value, V} -> RPA1 = gb_trees:delete(SeqId, RPA),
                      {V, State #vqstate { ram_pending_ack = RPA1 }};
        none       -> case gb_trees:lookup(SeqId, DPA) of
                          {value, V} ->
                              DPA1 = gb_trees:delete(SeqId, DPA),
                              {V, State#vqstate{disk_pending_ack = DPA1}};
                          none ->
                              case gb_trees:lookup(SeqId, QPA) of
                                  {value, V} ->
                                      QPA1 = gb_trees:delete(SeqId, QPA),
                                      {V, State#vqstate{qi_pending_ack = QPA1}};
                                  none ->
                                      {none, State}
                              end
                      end
    end.

purge_pending_ack(KeepPersistent,
                  State = #vqstate { index_mod         = IndexMod,
                                     index_state       = IndexState,
                                     store_state       = StoreState0,
                                     msg_store_clients = MSCState }) ->
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, State1} = purge_pending_ack1(State),
    StoreState1 = lists:foldl(fun rabbit_classic_queue_store_v2:remove/2, StoreState0, SeqIdsInStore),
    %% @todo Sounds like we might want to remove only transients from the cache?
    case KeepPersistent of
        true  -> remove_transient_msgs_by_id(MsgIdsByStore, MSCState),
                 State1 #vqstate { store_state = StoreState1 };
        false -> {DeletedSegments, IndexState1} =
                     IndexMod:ack(IndexOnDiskSeqIds, IndexState),
                 StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState1),
                 remove_vhost_msgs_by_id(MsgIdsByStore, MSCState),
                 State1 #vqstate { index_state = IndexState1,
                                   store_state = StoreState }
    end.

purge_pending_ack_delete_and_terminate(
  State = #vqstate { index_mod         = IndexMod,
                     index_state       = IndexState,
                     msg_store_clients = MSCState }) ->
    {_, MsgIdsByStore, _SeqIdsInStore, State1} = purge_pending_ack1(State),
    IndexState1 = IndexMod:delete_and_terminate(IndexState),
    %% @todo delete queue store.
    remove_vhost_msgs_by_id(MsgIdsByStore, MSCState),
    State1 #vqstate { index_state = IndexState1 }.

purge_pending_ack1(State = #vqstate { ram_pending_ack   = RPA,
                                      disk_pending_ack  = DPA,
                                      qi_pending_ack    = QPA }) ->
    F = fun (_SeqId, MsgStatus, Acc) -> accumulate_ack(MsgStatus, Acc) end,
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, _AllMsgIds} =
        rabbit_misc:gb_trees_fold(
          F, rabbit_misc:gb_trees_fold(
               F,  rabbit_misc:gb_trees_fold(
                     F, accumulate_ack_init(), RPA), DPA), QPA),
    State1 = State #vqstate { ram_pending_ack  = gb_trees:empty(),
                              disk_pending_ack = gb_trees:empty(),
                              qi_pending_ack   = gb_trees:empty()},
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, State1}.

%% MsgIdsByStore is an map with two keys:
%%
%% true: holds a list of Persistent Message Ids.
%% false: holds a list of Transient Message Ids.
%%
%% When we call maps:to_list/1 we get two sets of msg ids, where
%% IsPersistent is either true for persistent messages or false for
%% transient ones. The msg_store_remove/3 function takes this boolean
%% flag to determine from which store the messages should be removed
%% from.
remove_vhost_msgs_by_id(MsgIdsByStore, MSCState) ->
    [ok = msg_store_remove(MSCState, IsPersistent, MsgIds)
     || {IsPersistent, MsgIds} <- maps:to_list(MsgIdsByStore)].

remove_transient_msgs_by_id(MsgIdsByStore, MSCState) ->
    case maps:find(false, MsgIdsByStore) of
        error        -> ok;
        {ok, MsgIds} -> ok = msg_store_remove(MSCState, false, MsgIds)
    end.

accumulate_ack_init() -> {[], maps:new(), [], []}.

accumulate_ack(#msg_status { seq_id        = SeqId,
                             msg_id        = MsgId,
                             is_persistent = IsPersistent,
                             msg_location  = MsgLocation,
                             index_on_disk = IndexOnDisk },
               {IndexOnDiskSeqIdsAcc, MsgIdsByStore, SeqIdsInStore, AllMsgIds}) ->
    {cons_if(IndexOnDisk, SeqId, IndexOnDiskSeqIdsAcc),
     case MsgLocation of
         ?IN_SHARED_STORE -> rabbit_misc:maps_cons(IsPersistent, MsgId, MsgIdsByStore);
         _                -> MsgIdsByStore
     end,
     case MsgLocation of
         ?IN_QUEUE_STORE -> [SeqId|SeqIdsInStore];
         ?IN_QUEUE_INDEX -> [SeqId|SeqIdsInStore];
         _               -> SeqIdsInStore
     end,
     [MsgId | AllMsgIds]}.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

record_confirms(MsgIdSet, State = #vqstate { msgs_on_disk        = MOD,
                                             msg_indices_on_disk = MIOD,
                                             unconfirmed         = UC,
                                             confirmed           = C }) ->
    State #vqstate {
      msgs_on_disk        = rabbit_misc:gb_sets_difference(MOD,  MsgIdSet),
      msg_indices_on_disk = rabbit_misc:gb_sets_difference(MIOD, MsgIdSet),
      unconfirmed         = rabbit_misc:gb_sets_difference(UC,   MsgIdSet),
      confirmed           = gb_sets:union(C, MsgIdSet) }.

msgs_written_to_disk(Callback, MsgIdSet, ignored) ->
    Callback(?MODULE,
             fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end);
msgs_written_to_disk(Callback, MsgIdSet, written) ->
    Callback(?MODULE,
             fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
                                              msg_indices_on_disk = MIOD,
                                              unconfirmed         = UC }) ->
                     %% @todo Apparently the message store ALWAYS calls this function
                     %%       for all message IDs. This is a waste. We should only
                     %%       call it for messages that need confirming, and avoid
                     %%       this intersection call.
                     %%
                     %%       The same may apply to msg_indices_written_to_disk as well.
                     Confirmed = gb_sets:intersection(UC, MsgIdSet),
                     record_confirms(gb_sets:intersection(MsgIdSet, MIOD),
                                     State #vqstate {
                                       msgs_on_disk =
                                           gb_sets:union(MOD, Confirmed) })
             end).

msg_indices_written_to_disk(Callback, MsgIdSet) ->
    Callback(?MODULE,
             fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
                                              msg_indices_on_disk = MIOD,
                                              unconfirmed         = UC }) ->
                     Confirmed = gb_sets:intersection(UC, MsgIdSet),
                     record_confirms(gb_sets:intersection(MsgIdSet, MOD),
                                     State #vqstate {
                                       msg_indices_on_disk =
                                           gb_sets:union(MIOD, Confirmed) })
             end).

msgs_and_indices_written_to_disk(Callback, MsgIdSet) ->
    Callback(?MODULE,
             fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end).

%%----------------------------------------------------------------------------
%% Internal plumbing for requeue
%%----------------------------------------------------------------------------

publish_alpha(#msg_status { msg = undefined } = MsgStatus, State) ->
    {Msg, State1} = read_msg(MsgStatus, State),
    MsgStatus1 = MsgStatus#msg_status { msg = Msg },
    {MsgStatus1, stats({1, -1}, {MsgStatus, MsgStatus1}, 0, State1)};
publish_alpha(MsgStatus, State) ->
    {MsgStatus, stats({1, -1}, {MsgStatus, MsgStatus}, 0, State)}.

publish_beta(MsgStatus, State) ->
    {MsgStatus1, State1} = maybe_prepare_write_to_disk(true, false, MsgStatus, State),
    MsgStatus2 = m(trim_msg_status(MsgStatus1)),
    {MsgStatus2, stats({1, -1}, {MsgStatus, MsgStatus2}, 0, State1)}.

%% Rebuild queue, inserting sequence ids to maintain ordering
queue_merge(SeqIds, Q, MsgIds, Limit, PubFun, State) ->
    queue_merge(SeqIds, Q, ?QUEUE:new(), MsgIds,
                Limit, PubFun, State).

queue_merge([SeqId | Rest] = SeqIds, Q, Front, MsgIds,
            Limit, PubFun, State)
  when Limit == undefined orelse SeqId < Limit ->
    case ?QUEUE:out(Q) of
        {{value, #msg_status { seq_id = SeqIdQ } = MsgStatus}, Q1}
          when SeqIdQ < SeqId ->
            %% enqueue from the remaining queue
            queue_merge(SeqIds, Q1, ?QUEUE:in(MsgStatus, Front), MsgIds,
                        Limit, PubFun, State);
        {_, _Q1} ->
            %% enqueue from the remaining list of sequence ids
            case msg_from_pending_ack(SeqId, State) of
                {none, _} ->
                    queue_merge(Rest, Q, Front, MsgIds, Limit, PubFun, State);
                {MsgStatus, State1} ->
                    {#msg_status { msg_id = MsgId } = MsgStatus1, State2} =
                        PubFun(MsgStatus, State1),
                    queue_merge(Rest, Q, ?QUEUE:in(MsgStatus1, Front), [MsgId | MsgIds],
                                Limit, PubFun, State2)
            end
    end;
queue_merge(SeqIds, Q, Front, MsgIds,
            _Limit, _PubFun, State) ->
    {SeqIds, ?QUEUE:join(Front, Q), MsgIds, State}.

delta_merge([], Delta, MsgIds, State) ->
    {Delta, MsgIds, State};
delta_merge(SeqIds, Delta, MsgIds, State) ->
    lists:foldl(fun (SeqId, {Delta0, MsgIds0, State0} = Acc) ->
                        case msg_from_pending_ack(SeqId, State0) of
                            {none, _} ->
                                Acc;
                        {#msg_status { msg_id = MsgId,
                                       is_persistent = IsPersistent } = MsgStatus, State1} ->
                                {_MsgStatus, State2} =
                                    maybe_prepare_write_to_disk(true, true, MsgStatus, State1),
                                {expand_delta(SeqId, Delta0, IsPersistent), [MsgId | MsgIds0],
                                 stats({1, -1}, {MsgStatus, none}, 1, State2)}
                        end
                end, {Delta, MsgIds, State}, SeqIds).

%% Mostly opposite of record_pending_ack/2
msg_from_pending_ack(SeqId, State) ->
    case remove_pending_ack(false, SeqId, State) of
        {none, _} ->
            {none, State};
        {#msg_status { msg_props = MsgProps } = MsgStatus, State1} ->
            {MsgStatus #msg_status {
               msg_props = MsgProps #message_properties { needs_confirming = false } },
             State1}
    end.

beta_limit(Q) ->
    case ?QUEUE:peek(Q) of
        {value, #msg_status { seq_id = SeqId }} -> SeqId;
        empty                                   -> undefined
    end.

delta_limit(?BLANK_DELTA_PATTERN(_))              -> undefined;
delta_limit(#delta { start_seq_id = StartSeqId }) -> StartSeqId.

%%----------------------------------------------------------------------------
%% Iterator
%%----------------------------------------------------------------------------

ram_ack_iterator(State) ->
    {ack, gb_trees:iterator(State#vqstate.ram_pending_ack)}.

disk_ack_iterator(State) ->
    {ack, gb_trees:iterator(State#vqstate.disk_pending_ack)}.

qi_ack_iterator(State) ->
    {ack, gb_trees:iterator(State#vqstate.qi_pending_ack)}.

msg_iterator(State) -> istate(start, State).

istate(start, State) -> {q4,    State#vqstate.q4,    State};
istate(q4,    State) -> {q3,    State#vqstate.q3,    State};
istate(q3,    State) -> {delta, State#vqstate.delta, State};
istate(delta, State) -> {q2,    State#vqstate.q2,    State};
istate(q2,    State) -> {q1,    State#vqstate.q1,    State};
istate(q1,   _State) -> done.

next({ack, It}, IndexState) ->
    case gb_trees:next(It) of
        none                     -> {empty, IndexState};
        {_SeqId, MsgStatus, It1} -> Next = {ack, It1},
                                    {value, MsgStatus, true, Next, IndexState}
    end;
next(done, IndexState) -> {empty, IndexState};
next({delta, #delta{start_seq_id = SeqId,
                    end_seq_id   = SeqId}, State}, IndexState) ->
    next(istate(delta, State), IndexState);
next({delta, #delta{start_seq_id = SeqId,
                    end_seq_id   = SeqIdEnd} = Delta, State = #vqstate{index_mod = IndexMod}}, IndexState) ->
    SeqIdB = IndexMod:next_segment_boundary(SeqId),
    SeqId1 = lists:min([SeqIdB,
                        %% We must limit the number of messages read at once
                        %% otherwise the queue will attempt to read up to segment_entry_count()
                        %% messages from the index each time. The value
                        %% chosen here is arbitrary.
                        %% @todo We have a problem where reduce_memory_usage puts messages back to 0,
                        %%       and then this or the maybe_deltas_to_betas function is called and it
                        %%       fetches 2048 messages again. This is not good. Maybe the reduce_memory_usage
                        %%       function should reduce the number of messages we fetch at once at the
                        %%       same time (start at 2048, divide by 2 every time we reduce, or something).
                        %%       Maybe expiration does that?
                        SeqId + 2048,
                        SeqIdEnd]),
    {List, IndexState1} = IndexMod:read(SeqId, SeqId1, IndexState),
    next({delta, Delta#delta{start_seq_id = SeqId1}, List, State}, IndexState1);
next({delta, Delta, [], State}, IndexState) ->
    next({delta, Delta, State}, IndexState);
next({delta, Delta, [{_, SeqId, _, _, _} = M | Rest], State}, IndexState) ->
    case is_msg_in_pending_acks(SeqId, State) of
        false -> Next = {delta, Delta, Rest, State},
                 {value, beta_msg_status(M), false, Next, IndexState};
        true  -> next({delta, Delta, Rest, State}, IndexState)
    end;
next({Key, Q, State}, IndexState) ->
    case ?QUEUE:out(Q) of
        {empty, _Q}              -> next(istate(Key, State), IndexState);
        {{value, MsgStatus}, QN} -> Next = {Key, QN, State},
                                    {value, MsgStatus, false, Next, IndexState}
    end.

inext(It, {Its, IndexState}) ->
    case next(It, IndexState) of
        {empty, IndexState1} ->
            {Its, IndexState1};
        {value, MsgStatus1, Unacked, It1, IndexState1} ->
            {[{MsgStatus1, Unacked, It1} | Its], IndexState1}
    end.

ifold(_Fun, Acc, [], State0) ->
    {Acc, State0};
ifold(Fun, Acc, Its0, State0) ->
    [{MsgStatus, Unacked, It} | Rest] =
        lists:sort(fun ({#msg_status{seq_id = SeqId1}, _, _},
                        {#msg_status{seq_id = SeqId2}, _, _}) ->
                           SeqId1 =< SeqId2
                   end, Its0),
    {Msg, State1} = read_msg(MsgStatus, State0),
    case Fun(Msg, MsgStatus#msg_status.msg_props, Unacked, Acc) of
        {stop, Acc1} ->
            {Acc1, State1};
        {cont, Acc1} ->
            IndexState0 = State1#vqstate.index_state,
            {Its1, IndexState1} = inext(It, {Rest, IndexState0}),
            State2 = State1#vqstate{index_state = IndexState1},
            ifold(Fun, Acc1, Its1, State2)
    end.

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

maybe_reduce_memory_use(State = #vqstate {memory_reduction_run_count = MRedRunCount,
                                          mode = Mode}) ->
    case MRedRunCount >= ?EXPLICIT_GC_RUN_OP_THRESHOLD(Mode) of
        true -> State1 = reduce_memory_use(State),
                State1#vqstate{memory_reduction_run_count =  0};
        false -> State#vqstate{memory_reduction_run_count =  MRedRunCount + 1}
    end.

reduce_memory_use(State = #vqstate { target_ram_count = infinity }) ->
    State;
reduce_memory_use(State = #vqstate {
                    mode             = default,
                    ram_pending_ack  = RPA,
                    ram_msg_count    = RamMsgCount,
                    target_ram_count = TargetRamCount,
                    io_batch_size    = IoBatchSize,
                    rates            = #rates { in      = AvgIngress,
                                                out     = AvgEgress,
                                                ack_in  = AvgAckIngress,
                                                ack_out = AvgAckEgress } }) ->
    {CreditDiscBound, _} =rabbit_misc:get_env(rabbit,
                                              msg_store_credit_disc_bound,
                                              ?CREDIT_DISC_BOUND),
    {NeedResumeA2B, State1} = {_, #vqstate { q2 = Q2, q3 = Q3 }} =
        case chunk_size(RamMsgCount + gb_trees:size(RPA), TargetRamCount) of
            0  -> {false, State};
            %% Reduce memory of pending acks and alphas. The order is
            %% determined based on which is growing faster. Whichever
            %% comes second may very well get a quota of 0 if the
            %% first manages to push out the max number of messages.
            A2BChunk ->
                %% In case there are few messages to be sent to a message store
                %% and many messages to be embedded to the queue index,
                %% we should limit the number of messages to be flushed
                %% to avoid blocking the process.
                A2BChunkActual = case A2BChunk > CreditDiscBound * 2 of
                    true  -> CreditDiscBound * 2;
                    false -> A2BChunk
                end,
                Funs = case ((AvgAckIngress - AvgAckEgress) >
                                   (AvgIngress - AvgEgress)) of
                             true  -> [fun limit_ram_acks/2,
                                       fun push_alphas_to_betas/2];
                             false -> [fun push_alphas_to_betas/2,
                                       fun limit_ram_acks/2]
                         end,
                {Quota, State2} = lists:foldl(fun (ReduceFun, {QuotaN, StateN}) ->
                                                    ReduceFun(QuotaN, StateN)
                                              end, {A2BChunkActual, State}, Funs),
                {(Quota == 0) andalso (A2BChunk > A2BChunkActual), State2}
        end,
    Permitted = permitted_beta_count(State1),
    {NeedResumeB2D, State3} =
        %% If there are more messages with their queue position held in RAM,
        %% a.k.a. betas, in Q2 & Q3 than IoBatchSize,
        %% write their queue position to disk, a.k.a. push_betas_to_deltas
        case chunk_size(?QUEUE:len(Q2) + ?QUEUE:len(Q3),
                        Permitted) of
            B2DChunk when B2DChunk >= IoBatchSize ->
                %% Same as for alphas to betas. Limit a number of messages
                %% to be flushed to disk at once to avoid blocking the process.
                B2DChunkActual = case B2DChunk > CreditDiscBound * 2 of
                    true -> CreditDiscBound * 2;
                    false -> B2DChunk
                end,
                StateBD = push_betas_to_deltas(B2DChunkActual, State1),
                {B2DChunk > B2DChunkActual, StateBD};
            _  ->
                {false, State1}
        end,
    #vqstate{ index_mod = IndexMod, index_state = IndexState } = State3,
    %% @todo Sync the store before the index but only if IndexMod:needs_sync != false.
    State4 = State3#vqstate{ index_state = IndexMod:flush(IndexState) },
    %% We can be blocked by the credit flow, or limited by a batch size,
    %% or finished with flushing.
    %% If blocked by the credit flow - the credit grant will resume processing,
    %% if limited by a batch - the batch continuation message should be sent.
    %% The continuation message will be prioritised over publishes,
    %% but not consumptions, so the queue can make progess.
    Blocked = credit_flow:blocked(),
    case {Blocked, NeedResumeA2B orelse NeedResumeB2D} of
        %% Credit bump will continue paging
        {true, _}      -> State4;
        %% Finished with paging
        {false, false} -> State4;
        %% Planning next batch
        {false, true}  ->
            %% We don't want to use self-credit-flow, because it's harder to
            %% reason about. So the process sends a (prioritised) message to
            %% itself and sets a waiting_bump value to keep the message box clean
            maybe_bump_reduce_memory_use(State4)
    end;
%% When using lazy queues, there are no alphas, so we don't need to
%% call push_alphas_to_betas/2.
reduce_memory_use(State = #vqstate {
                             mode = lazy,
                             ram_pending_ack  = RPA,
                             ram_msg_count    = RamMsgCount,
                             target_ram_count = TargetRamCount }) ->
    State1 = #vqstate { q3 = Q3 } =
        case chunk_size(RamMsgCount + gb_trees:size(RPA), TargetRamCount) of
            0  -> State;
            S1 -> {_, State2} = limit_ram_acks(S1, State),
                  State2
        end,

    State3 =
        case chunk_size(?QUEUE:len(Q3),
                        permitted_beta_count(State1)) of
            0  ->
                State1;
            S2 ->
                push_betas_to_deltas(S2, State1)
        end,
    #vqstate{ index_mod = IndexMod, index_state = IndexState } = State3,
    %% @todo Sync the store before the index but only if IndexMod:needs_sync != false.
    State4 = State3#vqstate{ index_state = IndexMod:flush(IndexState) },
    garbage_collect(),
    State4.

maybe_bump_reduce_memory_use(State = #vqstate{ waiting_bump = true }) ->
    State;
maybe_bump_reduce_memory_use(State) ->
    self() ! bump_reduce_memory_use,
    State#vqstate{ waiting_bump = true }.

limit_ram_acks(0, State) ->
    {0, ui(State)};
limit_ram_acks(Quota, State = #vqstate { ram_pending_ack  = RPA,
                                         disk_pending_ack = DPA }) ->
    case gb_trees:is_empty(RPA) of
        true ->
            {Quota, ui(State)};
        false ->
            {SeqId, MsgStatus, RPA1} = gb_trees:take_largest(RPA),
            {MsgStatus1, State1} =
                maybe_prepare_write_to_disk(true, false, MsgStatus, State),
            MsgStatus2 = m(trim_msg_status(MsgStatus1)),
            DPA1 = gb_trees:insert(SeqId, MsgStatus2, DPA),
            limit_ram_acks(Quota - 1,
                           stats({0, 0}, {MsgStatus, MsgStatus2}, 0,
                                 State1 #vqstate { ram_pending_ack  = RPA1,
                                                   disk_pending_ack = DPA1 }))
    end.

permitted_beta_count(#vqstate { len = 0 }) ->
    infinity;
permitted_beta_count(#vqstate { mode             = lazy,
                                target_ram_count = TargetRamCount}) ->
    TargetRamCount;
permitted_beta_count(#vqstate { target_ram_count = 0, q3 = Q3, index_mod = IndexMod }) ->
    lists:min([?QUEUE:len(Q3), IndexMod:next_segment_boundary(0)]);
permitted_beta_count(#vqstate { q1               = Q1,
                                q4               = Q4,
                                index_mod        = IndexMod,
                                target_ram_count = TargetRamCount,
                                len              = Len }) ->
    BetaDelta = Len - ?QUEUE:len(Q1) - ?QUEUE:len(Q4),
    lists:max([IndexMod:next_segment_boundary(0),
               BetaDelta - ((BetaDelta * BetaDelta) div
                                (BetaDelta + TargetRamCount))]).

chunk_size(Current, Permitted)
  when Permitted =:= infinity orelse Permitted >= Current ->
    0;
chunk_size(Current, Permitted) ->
    Current - Permitted.

fetch_from_q3(State = #vqstate { mode  = default,
                                 q1    = Q1,
                                 q2    = Q2,
                                 delta = #delta { count = DeltaCount },
                                 q3    = Q3,
                                 q4    = Q4 }) ->
    case ?QUEUE:out(Q3) of
        {empty, _Q3} ->
            {empty, State};
        {{value, MsgStatus}, Q3a} ->
            State1 = State #vqstate { q3 = Q3a },
            State2 = case {?QUEUE:is_empty(Q3a), 0 == DeltaCount} of
                         {true, true} ->
                             %% q3 is now empty, it wasn't before;
                             %% delta is still empty. So q2 must be
                             %% empty, and we know q4 is empty
                             %% otherwise we wouldn't be loading from
                             %% q3. As such, we can just set q4 to Q1.
                             true = ?QUEUE:is_empty(Q2), %% ASSERTION
                             true = ?QUEUE:is_empty(Q4), %% ASSERTION
                             State1 #vqstate { q1 = ?QUEUE:new(), q4 = Q1 };
                         {true, false} ->
                             maybe_deltas_to_betas(State1);
                         {false, _} ->
                             %% q3 still isn't empty, we've not
                             %% touched delta, so the invariants
                             %% between q1, q2, delta and q3 are
                             %% maintained
                             State1
                     end,
            {loaded, {MsgStatus, State2}}
    end;
%% lazy queues
fetch_from_q3(State = #vqstate { mode  = lazy,
                                 delta = #delta { count = DeltaCount },
                                 q3    = Q3 }) ->
    case ?QUEUE:out(Q3) of
        {empty, _Q3} when DeltaCount =:= 0 ->
            {empty, State};
        {empty, _Q3} ->
            fetch_from_q3(maybe_deltas_to_betas(State));
        {{value, MsgStatus}, Q3a} ->
            State1 = State #vqstate { q3 = Q3a },
            {loaded, {MsgStatus, State1}}
    end.

maybe_deltas_to_betas(State) ->
    AfterFun = process_delivers_and_acks_fun(deliver_and_ack),
    maybe_deltas_to_betas(AfterFun, State).

maybe_deltas_to_betas(_DelsAndAcksFun,
                      State = #vqstate {delta = ?BLANK_DELTA_PATTERN(X) }) ->
    State;
maybe_deltas_to_betas(DelsAndAcksFun,
                      State = #vqstate {
                        q2                   = Q2,
                        delta                = Delta,
                        q3                   = Q3,
                        index_mod            = IndexMod,
                        index_state          = IndexState,
                        ram_msg_count        = RamMsgCount,
                        ram_bytes            = RamBytes,
                        disk_read_count      = DiskReadCount,
                        delta_transient_bytes = DeltaTransientBytes,
                        transient_threshold  = TransientThreshold }) ->
    #delta { start_seq_id = DeltaSeqId,
             count        = DeltaCount,
             transient    = Transient,
             end_seq_id   = DeltaSeqIdEnd } = Delta,
    DeltaSeqId1 =
        lists:min([IndexMod:next_segment_boundary(DeltaSeqId),
                   %% We must limit the number of messages read at once
                   %% otherwise the queue will attempt to read up to segment_entry_count()
                   %% messages from the index each time. The value
                   %% chosen here is arbitrary.
                   DeltaSeqId + 2048,
                   DeltaSeqIdEnd]),
    {List, IndexState1} = IndexMod:read(DeltaSeqId, DeltaSeqId1, IndexState),
    {Q3a, RamCountsInc, RamBytesInc, State1, TransientCount, TransientBytes} =
        betas_from_index_entries(List, TransientThreshold,
                                 DelsAndAcksFun,
                                 State #vqstate { index_state = IndexState1 }),
    State2 = State1 #vqstate { ram_msg_count     = RamMsgCount   + RamCountsInc,
                               ram_bytes         = RamBytes      + RamBytesInc,
                               disk_read_count   = DiskReadCount + RamCountsInc },
    case ?QUEUE:len(Q3a) of
        0 ->
            %% we ignored every message in the segment due to it being
            %% transient and below the threshold
            maybe_deltas_to_betas(
              DelsAndAcksFun,
              State2 #vqstate {
                delta = d(Delta #delta { start_seq_id = DeltaSeqId1 })});
        Q3aLen ->
            Q3b = ?QUEUE:join(Q3, Q3a),
            case DeltaCount - Q3aLen of
                0 ->
                    %% delta is now empty, but it wasn't before, so
                    %% can now join q2 onto q3
                    State2 #vqstate { q2    = ?QUEUE:new(),
                                      delta = ?BLANK_DELTA,
                                      q3    = ?QUEUE:join(Q3b, Q2),
                                      delta_transient_bytes = 0};
                N when N > 0 ->
                    Delta1 = d(#delta { start_seq_id = DeltaSeqId1,
                                        count        = N,
                                        transient    = Transient - TransientCount,
                                        end_seq_id   = DeltaSeqIdEnd }),
                    State2 #vqstate { delta = Delta1,
                                      q3    = Q3b,
                                      delta_transient_bytes = DeltaTransientBytes - TransientBytes }
            end
    end.

push_alphas_to_betas(Quota, State) ->
    {Quota1, State1} =
        push_alphas_to_betas(
          fun ?QUEUE:out/1,
          fun (MsgStatus, Q1a,
               State0 = #vqstate { q3 = Q3, delta = #delta { count = 0,
                                                             transient = 0 } }) ->
                  State0 #vqstate { q1 = Q1a, q3 = ?QUEUE:in(MsgStatus, Q3) };
              (MsgStatus, Q1a, State0 = #vqstate { q2 = Q2 }) ->
                  State0 #vqstate { q1 = Q1a, q2 = ?QUEUE:in(MsgStatus, Q2) }
          end, Quota, State #vqstate.q1, State),
    {Quota2, State2} =
        push_alphas_to_betas(
          fun ?QUEUE:out_r/1,
          fun (MsgStatus, Q4a, State0 = #vqstate { q3 = Q3 }) ->
                  State0 #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3), q4 = Q4a }
          end, Quota1, State1 #vqstate.q4, State1),
    {Quota2, State2}.

push_alphas_to_betas(_Generator, _Consumer, Quota, _Q,
                     State = #vqstate { ram_msg_count    = RamMsgCount,
                                        target_ram_count = TargetRamCount })
  when Quota =:= 0 orelse
       TargetRamCount =:= infinity orelse
       TargetRamCount >= RamMsgCount ->
    {Quota, ui(State)};
push_alphas_to_betas(Generator, Consumer, Quota, Q, State) ->
    %% We consume credits from the message_store whenever we need to
    %% persist a message to disk. See:
    %% rabbit_variable_queue:msg_store_write/4. So perhaps the
    %% msg_store is trying to throttle down our queue.
    case credit_flow:blocked() of
        true  -> {Quota, ui(State)};
        false -> case Generator(Q) of
                     {empty, _Q} ->
                         {Quota, ui(State)};
                     {{value, MsgStatus}, Qa} ->
                         {MsgStatus1, State1} =
                             maybe_prepare_write_to_disk(true, false, MsgStatus,
                                                         State),
                         MsgStatus2 = m(trim_msg_status(MsgStatus1)),
                         State2 = stats(
                                    ready0, {MsgStatus, MsgStatus2}, 0, State1),
                         State3 = Consumer(MsgStatus2, Qa, State2),
                         push_alphas_to_betas(Generator, Consumer, Quota - 1,
                                              Qa, State3)
                 end
    end.

push_betas_to_deltas(Quota, State = #vqstate { mode      = default,
                                               q2        = Q2,
                                               delta     = Delta,
                                               q3        = Q3,
                                               index_mod = IndexMod }) ->
    PushState = {Quota, Delta, State},
    {Q3a, PushState1} = push_betas_to_deltas(
                          fun ?QUEUE:out_r/1,
                          fun IndexMod:next_segment_boundary/1,
                          Q3, PushState),
    {Q2a, PushState2} = push_betas_to_deltas(
                          fun ?QUEUE:out/1,
                          fun (Q2MinSeqId) -> Q2MinSeqId end,
                          Q2, PushState1),
    {_, Delta1, State1} = PushState2,
    State1 #vqstate { q2    = Q2a,
                      delta = Delta1,
                      q3    = Q3a };
%% In the case of lazy queues we want to page as many messages as
%% possible from q3.
push_betas_to_deltas(Quota, State = #vqstate { mode  = lazy,
                                               delta = Delta,
                                               q3    = Q3}) ->
    PushState = {Quota, Delta, State},
    {Q3a, PushState1} = push_betas_to_deltas(
                          fun ?QUEUE:out_r/1,
                          fun (Q2MinSeqId) -> Q2MinSeqId end,
                          Q3, PushState),
    {_, Delta1, State1} = PushState1,
    State1 #vqstate { delta = Delta1,
                      q3    = Q3a }.


push_betas_to_deltas(Generator, LimitFun, Q, PushState) ->
    case ?QUEUE:is_empty(Q) of
        true ->
            {Q, PushState};
        false ->
            {value, #msg_status { seq_id = MinSeqId }} = ?QUEUE:peek(Q),
            {value, #msg_status { seq_id = MaxSeqId }} = ?QUEUE:peek_r(Q),
            Limit = LimitFun(MinSeqId),
            case MaxSeqId < Limit of
                true  -> {Q, PushState};
                false -> push_betas_to_deltas1(Generator, Limit, Q, PushState)
            end
    end.

push_betas_to_deltas1(_Generator, _Limit, Q, {0, Delta, State}) ->
    {Q, {0, Delta, ui(State)}};
push_betas_to_deltas1(Generator, Limit, Q, {Quota, Delta, State}) ->
    case Generator(Q) of
        {empty, _Q} ->
            {Q, {Quota, Delta, ui(State)}};
        {{value, #msg_status { seq_id = SeqId }}, _Qa}
          when SeqId < Limit ->
            {Q, {Quota, Delta, ui(State)}};
        {{value, MsgStatus = #msg_status { seq_id = SeqId }}, Qa} ->
            {#msg_status { index_on_disk = true,
                           is_persistent = IsPersistent }, State1} =
                maybe_batch_write_index_to_disk(true, MsgStatus, State),
            State2 = stats(ready0, {MsgStatus, none}, 1, State1),
            Delta1 = expand_delta(SeqId, Delta, IsPersistent),
            push_betas_to_deltas1(Generator, Limit, Qa,
                                  {Quota - 1, Delta1, State2})
    end.

%% Flushes queue index batch caches and updates queue index state.
ui(#vqstate{index_mod        = IndexMod,
            index_state      = IndexState,
            target_ram_count = TargetRamCount} = State) ->
    IndexState1 = IndexMod:flush_pre_publish_cache(
                    TargetRamCount, IndexState),
    State#vqstate{index_state = IndexState1}.

%%----------------------------------------------------------------------------
%% Upgrading
%%----------------------------------------------------------------------------

-spec multiple_routing_keys() -> 'ok'.

multiple_routing_keys() ->
    transform_storage(
      fun ({basic_message, ExchangeName, Routing_Key, Content,
            MsgId, Persistent}) ->
              {ok, {basic_message, ExchangeName, [Routing_Key], Content,
                    MsgId, Persistent}};
          (_) -> {error, corrupt_message}
      end),
    ok.


%% Assumes message store is not running
transform_storage(TransformFun) ->
    transform_store(?PERSISTENT_MSG_STORE, TransformFun),
    transform_store(?TRANSIENT_MSG_STORE, TransformFun).

transform_store(Store, TransformFun) ->
    rabbit_msg_store:force_recovery(rabbit_mnesia:dir(), Store),
    rabbit_msg_store:transform_dir(rabbit_mnesia:dir(), Store, TransformFun).

move_messages_to_vhost_store() ->
    case list_persistent_queues() of
        []     ->
            log_upgrade("No durable queues found."
                        " Skipping message store migration"),
            ok;
        Queues ->
            move_messages_to_vhost_store(Queues)
    end,
    ok = delete_old_store(),
    ok = rabbit_queue_index:cleanup_global_recovery_terms().

move_messages_to_vhost_store(Queues) ->
    log_upgrade("Moving messages to per-vhost message store"),
    %% Move the queue index for each persistent queue to the new store
    lists:foreach(
        fun(Queue) ->
            QueueName = amqqueue:get_name(Queue),
            rabbit_queue_index:move_to_per_vhost_stores(QueueName)
        end,
        Queues),
    %% Legacy (global) msg_store may require recovery.
    %% This upgrade step should only be started
    %% if we are upgrading from a pre-3.7.0 version.
    {QueuesWithTerms, RecoveryRefs, StartFunState} = read_old_recovery_terms(Queues),

    OldStore = run_old_persistent_store(RecoveryRefs, StartFunState),

    VHosts = rabbit_vhost:list_names(),

    %% New store should not be recovered.
    NewMsgStore = start_new_store(VHosts),
    %% Recovery terms should be started for all vhosts for new store.
    [ok = rabbit_recovery_terms:open_table(VHost) || VHost <- VHosts],

    MigrationBatchSize = application:get_env(rabbit, queue_migration_batch_size,
                                             ?QUEUE_MIGRATION_BATCH_SIZE),
    in_batches(MigrationBatchSize,
        {rabbit_variable_queue, migrate_queue, [OldStore, NewMsgStore]},
        QueuesWithTerms,
        "message_store upgrades: Migrating batch ~p of ~p queues. Out of total ~p ",
        "message_store upgrades: Batch ~p of ~p queues migrated ~n. ~p total left"),

    log_upgrade("Message store migration finished"),
    ok = rabbit_sup:stop_child(OldStore),
    [ok= rabbit_recovery_terms:close_table(VHost) || VHost <- VHosts],
    ok = stop_new_store(NewMsgStore).

in_batches(Size, MFA, List, MessageStart, MessageEnd) ->
    in_batches(Size, 1, MFA, List, MessageStart, MessageEnd).

in_batches(_, _, _, [], _, _) -> ok;
in_batches(Size, BatchNum, MFA, List, MessageStart, MessageEnd) ->
    Length = length(List),
    {Batch, Tail} = case Size > Length of
        true  -> {List, []};
        false -> lists:split(Size, List)
    end,
    ProcessedLength = (BatchNum - 1) * Size,
    rabbit_log:info(MessageStart, [BatchNum, Size, ProcessedLength + Length]),
    {M, F, A} = MFA,
    Keys = [ rpc:async_call(node(), M, F, [El | A]) || El <- Batch ],
    lists:foreach(fun(Key) ->
        case rpc:yield(Key) of
            {badrpc, Err} -> throw(Err);
            _             -> ok
        end
    end,
    Keys),
    rabbit_log:info(MessageEnd, [BatchNum, Size, length(Tail)]),
    in_batches(Size, BatchNum + 1, MFA, Tail, MessageStart, MessageEnd).

migrate_queue({QueueName = #resource{virtual_host = VHost, name = Name},
               RecoveryTerm},
              OldStore, NewStore) ->
    log_upgrade_verbose(
        "Migrating messages in queue ~s in vhost ~s to per-vhost message store",
        [Name, VHost]),
    OldStoreClient = get_global_store_client(OldStore),
    NewStoreClient = get_per_vhost_store_client(QueueName, NewStore),
    %% WARNING: During scan_queue_segments queue index state is being recovered
    %% and terminated. This can cause side effects!
    rabbit_queue_index:scan_queue_segments(
        %% We migrate only persistent messages which are found in message store
        %% and are not acked yet
        fun (_SeqId, MsgId, _MsgProps, true, _IsDelivered, no_ack, OldC)
            when is_binary(MsgId) ->
                migrate_message(MsgId, OldC, NewStoreClient);
            (_SeqId, _MsgId, _MsgProps,
             _IsPersistent, _IsDelivered, _IsAcked, OldC) ->
                OldC
        end,
        OldStoreClient,
        QueueName),
    rabbit_msg_store:client_terminate(OldStoreClient),
    rabbit_msg_store:client_terminate(NewStoreClient),
    NewClientRef = rabbit_msg_store:client_ref(NewStoreClient),
    case RecoveryTerm of
        non_clean_shutdown -> ok;
        Term when is_list(Term) ->
            NewRecoveryTerm = lists:keyreplace(persistent_ref, 1, RecoveryTerm,
                                               {persistent_ref, NewClientRef}),
            rabbit_queue_index:update_recovery_term(QueueName, NewRecoveryTerm)
    end,
    log_upgrade_verbose("Finished migrating queue ~s in vhost ~s", [Name, VHost]),
    {QueueName, NewClientRef}.

migrate_message(MsgId, OldC, NewC) ->
    case rabbit_msg_store:read(MsgId, OldC) of
        {{ok, Msg}, OldC1} ->
            ok = rabbit_msg_store:write(MsgId, Msg, NewC),
            OldC1;
        _ -> OldC
    end.

get_per_vhost_store_client(#resource{virtual_host = VHost}, NewStore) ->
    {VHost, StorePid} = lists:keyfind(VHost, 1, NewStore),
    rabbit_msg_store:client_init(StorePid, rabbit_guid:gen(),
                                 fun(_,_) -> ok end, fun() -> ok end).

get_global_store_client(OldStore) ->
    rabbit_msg_store:client_init(OldStore,
                                 rabbit_guid:gen(),
                                 fun(_,_) -> ok end,
                                 fun() -> ok end).

list_persistent_queues() ->
    Node = node(),
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                ?amqqueue_is_classic(Q),
                                amqqueue:qnode(Q) == Node,
                                mnesia:read(rabbit_queue, amqqueue:get_name(Q), read) =:= []]))
      end).

read_old_recovery_terms([]) ->
    {[], [], ?EMPTY_START_FUN_STATE};
read_old_recovery_terms(Queues) ->
    QueueNames = [amqqueue:get_name(Q) || Q <- Queues],
    {AllTerms, StartFunState} = rabbit_queue_index:read_global_recovery_terms(QueueNames),
    Refs = [Ref || Terms <- AllTerms,
                   Terms /= non_clean_shutdown,
                   begin
                       Ref = proplists:get_value(persistent_ref, Terms),
                       Ref =/= undefined
                   end],
    {lists:zip(QueueNames, AllTerms), Refs, StartFunState}.

run_old_persistent_store(Refs, StartFunState) ->
    OldStoreName = ?PERSISTENT_MSG_STORE,
    ok = rabbit_sup:start_child(OldStoreName, rabbit_msg_store, start_global_store_link,
                                [OldStoreName, rabbit_mnesia:dir(),
                                 Refs, StartFunState]),
    OldStoreName.

start_new_store(VHosts) ->
    %% Ensure vhost supervisor is started, so we can add vhosts to it.
    lists:map(fun(VHost) ->
        VHostDir = rabbit_vhost:msg_store_dir_path(VHost),
        {ok, Pid} = rabbit_msg_store:start_link(?PERSISTENT_MSG_STORE,
                                                VHostDir,
                                                undefined,
                                                ?EMPTY_START_FUN_STATE),
        {VHost, Pid}
    end,
    VHosts).

stop_new_store(NewStore) ->
    lists:foreach(fun({_VHost, StorePid}) ->
        unlink(StorePid),
        exit(StorePid, shutdown)
    end,
    NewStore),
    ok.

delete_old_store() ->
    log_upgrade("Removing the old message store data"),
    rabbit_file:recursive_delete(
        [filename:join([rabbit_mnesia:dir(), ?PERSISTENT_MSG_STORE])]),
    %% Delete old transient store as well
    rabbit_file:recursive_delete(
        [filename:join([rabbit_mnesia:dir(), ?TRANSIENT_MSG_STORE])]),
    ok.

log_upgrade(Msg) ->
    log_upgrade(Msg, []).

log_upgrade(Msg, Args) ->
    rabbit_log:info("message_store upgrades: " ++ Msg, Args).

log_upgrade_verbose(Msg) ->
    log_upgrade_verbose(Msg, []).

log_upgrade_verbose(Msg, Args) ->
    rabbit_log_upgrade:info(Msg, Args).

maybe_client_terminate(MSCStateP) ->
    %% Queue might have been asked to stop by the supervisor, it needs a clean
    %% shutdown in order for the supervising strategy to work - if it reaches max
    %% restarts might bring the vhost down.
    try
        rabbit_msg_store:client_terminate(MSCStateP)
    catch
        _:_ ->
            ok
    end.

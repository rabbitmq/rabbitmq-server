%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_variable_queue).

-export([init/3, terminate/2, delete_and_terminate/2, delete_crashed/1,
         purge/1, purge_acks/1,
         publish/5, publish_delivered/4,
         discard/3, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, fetch/2, drop/2, ack/2, requeue/2,
         ackfold/4, fold/3, len/1, is_empty/1, depth/1,
         update_rates/1, needs_timeout/1, timeout/1,
         handle_pre_hibernate/1, resume/1, msg_rates/1,
         info/2, invoke/3, is_duplicate/2, set_queue_mode/2,
         set_queue_version/2, zip_msgs_and_acks/4]).

-export([start/2, stop/1]).

%% This function is used by rabbit_classic_queue_index_v2
%% to convert v1 queues to v2 after an upgrade to 4.0.
-export([convert_from_v1_to_v2_loop/8]).

%% exported for testing only
-export([start_msg_store/3, stop_msg_store/1, init/5]).

-include("mc.hrl").
-include_lib("stdlib/include/qlc.hrl").

-define(QUEUE_MIGRATION_BATCH_SIZE, 100).
-define(EMPTY_START_FUN_STATE, {fun (ok) -> finished end, ok}).

%%----------------------------------------------------------------------------
%% Messages, their metadata and their position in the queue (SeqId),
%% can be in memory or on disk, or both. Persistent messages in
%% durable queues are always written to disk when they arrive.
%% Transient messages as well as persistent messages in non-durable
%% queues may be kept only in memory.
%%
%% The number of messages kept in memory is dependent on the consume
%% rate of the queue. At a minimum 1 message is kept (necessary because
%% we often need to check the expiration at the head of the queue) and
%% at a maximum the semi-arbitrary number 2048.
%%
%% Messages are never written back to disk after they have been read
%% into memory. Instead the queue is designed to avoid keeping too
%% much to begin with.
%%
%% Messages are persisted using a queue index and a message store.
%% A few different scenarios may play out depending on the message
%% size:
%%
%% - size < qi_msgs_embed_below: the metadata
%%   is stored in rabbit_classic_queue_index_v2, while the content
%%   is stored in the per-queue rabbit_classic_queue_store_v2
%%
%% - size >= qi_msgs_embed_below: the metadata
%%   is stored in rabbit_classic_queue_index_v2, while the content
%%   is stored in the per-vhost shared rabbit_msg_store
%%
%% When messages must be read from disk, message bodies will
%% also be read from disk except if the message is stored
%% in the per-vhost shared rabbit_msg_store. In that case
%% the message gets read before it needs to be sent to the
%% consumer. Messages are read from rabbit_msg_store one
%% at a time currently.
%%
%% The queue also keeps track of messages that were delivered
%% but for which the ack has not been received. Pending acks
%% are currently kept in memory although the message may be
%% on disk.
%%
%% Messages being requeued are returned to their position in
%% the queue using their SeqId value.
%%
%% In order to try to achieve as fast a start-up as possible, if a
%% clean shutdown occurs, we try to save out state to disk to reduce
%% work on startup. In rabbit_msg_store this takes the form of the
%% index_module's state, plus the file_summary ets table, and client
%% refs. In the VQ, this takes the form of the count of persistent
%% messages in the queue and references into the msg_stores. The
%% queue index adds to these terms the details of its segments and
%% stores the terms in the queue directory.
%%
%% Two rabbit_msg_store(s) are used. One is created for persistent messages
%% to durable queues that must survive restarts, and the other is used
%% for all other messages that just happen to need to be written to
%% disk. On start up we can therefore nuke the transient message
%% store, and be sure that the messages in the persistent store are
%% all that we need.
%%
%% The references to the msg_stores are there so that the msg_store
%% knows to only trust its saved state if all of the queues it was
%% previously talking to come up cleanly. Likewise, the queues
%% themselves (especially indexes) skip work in init if all the queues
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
%% messages that were written to disk.
%%
%% The queue is keeping track of delivers via the
%% next_deliver_seq_id variable. This variable gets increased
%% with every (first-time) delivery. When delivering messages
%% the seq_id of the message is checked against this variable
%% to determine whether the message is a redelivery. The variable
%% is stored in the queue terms on graceful shutdown. On dirty
%% recovery the variable becomes the seq_id of the most recent
%% message in the queue (effectively marking all messages as
%% delivered, like the v1 index was doing).
%%
%% Previous versions of classic queues had a much more complex
%% way of working. Messages were categorized into four groups,
%% and remnants of these terms remain in the code at the time
%% of writing:
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
%% Messages may have been stored in q1, q2, delta, q3 or q4 depending
%% on their location in the queue. The current version of classic
%% queues only use delta (on-disk, for the tail of the queue) or
%% q3 (in-memory, head of the queue). Messages used to move from
%% q1 -> q2 -> delta -> q3 -> q4 (and sometimes q3 -> delta or
%% q4 -> delta to reduce memory use). Now messages only move
%% from delta to q3. Full details on the old mechanisms can be
%% found in previous versions of this file (such as the 3.11 version).
%%
%% In the current version of classic queues, there is no distinction
%% between default and lazy queues. The current behavior is close to
%% lazy queues, except we avoid some write to disks when queues are
%% empty.
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(vqstate,
        { q1, %% Unused.
          q2, %% Unused.
          delta,
          q3,
          q4, %% Unused.
          next_seq_id,
          %% seq_id() of first undelivered message
          %% everything before this seq_id() was delivered at least once
          next_deliver_seq_id,
          ram_pending_ack,    %% msgs still in RAM
          disk_pending_ack,   %% msgs in store, paged out
          qi_pending_ack, %% Unused.
          index_mod, %% Unused.
          index_state,
          store_state,
          msg_store_clients,
          durable,
          transient_threshold,
          qi_embed_msgs_below,

          len,                %% w/o unacked @todo No longer needed, is delta+q3.
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
          %% There are two confirms paths: either store/index produce confirms
          %% separately (v1 and v2 with per-vhost message store) or the confirms
          %% are produced all at once while syncing/flushing (v2 with per-queue
          %% message store). The latter is more efficient as it avoids many
          %% sets operations.
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
          mode, %% Unused.
          version = 2, %% Unused.
          %% Fast path for confirms handling. Instead of having
          %% index/store keep track of confirms separately and
          %% doing intersect/subtract/union we just put the messages
          %% here and on sync move them to 'confirmed'.
          %%
          %% Note: This field used to be 'memory_reduction_run_count'.
          unconfirmed_simple,
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
-include("amqqueue.hrl").

%%----------------------------------------------------------------------------

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
             ram_pending_ack       :: map(),
             disk_pending_ack      :: map(),
             qi_pending_ack        :: undefined,
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
             msgs_on_disk          :: sets:set(),
             msg_indices_on_disk   :: sets:set(),
             unconfirmed           :: sets:set(),
             confirmed             :: sets:set(),
             ack_out_counter       :: non_neg_integer(),
             ack_in_counter        :: non_neg_integer(),
             disk_read_count       :: non_neg_integer(),
             disk_write_count      :: non_neg_integer(),

             io_batch_size         :: pos_integer(),
             mode                  :: 'default' | 'lazy',
             version               :: 2,
             unconfirmed_simple    :: sets:set()}.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count        = 0,
                              transient    = 0,
                              end_seq_id   = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
                                         count        = 0,
                                         transient    = 0,
                                         end_seq_id   = Z }).

-define(MICROS_PER_SECOND, 1000000.0).

%% We're updating rates every 5s at most; a half life that is of
%% the same order of magnitude is probably about right.
-define(RATE_AVG_HALF_LIFE, 5.0).

%% We will recalculate the #rates{} every 5 seconds,
%% or every N messages published, whichever is
%% sooner. We do this since the priority calculations in
%% rabbit_amqqueue_process need fairly fresh rates.
-define(MSGS_PER_RATE_CALC, 100).

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
    rabbit_log:info("Starting message stores for vhost '~ts'", [VHost]),
    do_start_msg_store(VHost, ?TRANSIENT_MSG_STORE, undefined, ?EMPTY_START_FUN_STATE),
    do_start_msg_store(VHost, ?PERSISTENT_MSG_STORE, Refs, StartFunState),
    ok.

do_start_msg_store(VHost, Type, Refs, StartFunState) ->
    case rabbit_vhost_msg_store:start(VHost, Type, Refs, StartFunState) of
        {ok, _} ->
            rabbit_log:info("Started message store of type ~ts for vhost '~ts'", [abbreviated_type(Type), VHost]);
        {error, {no_such_vhost, VHost}} = Err ->
            rabbit_log:error("Failed to start message store of type ~ts for vhost '~ts': the vhost no longer exists!",
                             [Type, VHost]),
            exit(Err);
        {error, Error} ->
            rabbit_log:error("Failed to start message store of type ~ts for vhost '~ts': ~tp",
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
      Queue, Recover,
      fun (MsgIds, ActionTaken) ->
              msgs_written_to_disk(Callback, MsgIds, ActionTaken)
      end,
      fun (MsgIds) -> msg_indices_written_to_disk(Callback, MsgIds) end,
      fun (MsgIds) -> msgs_and_indices_written_to_disk(Callback, MsgIds) end).

init(Q, new, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) when ?is_amqqueue(Q) ->
    QueueName = amqqueue:get_name(Q),
    IsDurable = amqqueue:is_durable(Q),
    IndexState = rabbit_classic_queue_index_v2:init(QueueName,
        MsgIdxOnDiskFun, MsgAndIdxOnDiskFun),
    StoreState = rabbit_classic_queue_store_v2:init(QueueName),
    VHost = QueueName#resource.virtual_host,
    init(IsDurable, IndexState, StoreState, 0, 0, [],
         case IsDurable of
             true  -> msg_store_client_init(?PERSISTENT_MSG_STORE,
                                            MsgOnDiskFun, VHost);
             false -> undefined
         end,
         msg_store_client_init(?TRANSIENT_MSG_STORE, undefined,
                               VHost), VHost);

%% We can be recovering a transient queue if it crashed
init(Q, Terms, MsgOnDiskFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun) when ?is_amqqueue(Q) ->
    QueueName = amqqueue:get_name(Q),
    IsDurable = amqqueue:is_durable(Q),
    {PRef, RecoveryTerms} = process_recovery_terms(Terms),
    VHost = QueueName#resource.virtual_host,
    {PersistentClient, ContainsCheckFun} =
        case IsDurable of
            true  -> C = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef,
                                               MsgOnDiskFun, VHost),
                     {C, fun (MsgId) when is_binary(MsgId) ->
                                 rabbit_msg_store:contains(MsgId, C);
                             (Msg) ->
                                 mc:is_persistent(Msg)
                         end};
            false -> {undefined, fun(_MsgId) -> false end}
        end,
    TransientClient  = msg_store_client_init(?TRANSIENT_MSG_STORE,
                                             undefined, VHost),
    {DeltaCount, DeltaBytes, IndexState} =
        rabbit_classic_queue_index_v2:recover(
          QueueName, RecoveryTerms,
          rabbit_vhost_msg_store:successfully_recovered_state(
              VHost,
              ?PERSISTENT_MSG_STORE),
          ContainsCheckFun, MsgIdxOnDiskFun, MsgAndIdxOnDiskFun,
          main),
    StoreState = rabbit_classic_queue_store_v2:init(QueueName),
    init(IsDurable, IndexState, StoreState,
         DeltaCount, DeltaBytes, RecoveryTerms,
         PersistentClient, TransientClient, VHost).

process_recovery_terms(Terms=non_clean_shutdown) ->
    {rabbit_guid:gen(), Terms};
process_recovery_terms(Terms) ->
    case proplists:get_value(persistent_ref, Terms) of
        undefined -> {rabbit_guid:gen(), []};
        PRef      -> {PRef, Terms}
    end.

terminate(_Reason, State) ->
    State1 = #vqstate { virtual_host        = VHost,
                        next_seq_id         = NextSeqId,
                        next_deliver_seq_id = NextDeliverSeqId,
                        persistent_count    = PCount,
                        persistent_bytes    = PBytes,
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
        index_state = rabbit_classic_queue_index_v2:terminate(VHost, Terms, IndexState),
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

publish(Msg, MsgProps, IsDelivered, ChPid, State) ->
    State1 =
        publish1(Msg, MsgProps, IsDelivered, ChPid,
                 fun maybe_write_to_disk/4,
                 State),
    a(maybe_update_rates(State1)).

publish_delivered(Msg, MsgProps, ChPid, State) ->
    {SeqId, State1} =
        publish_delivered1(Msg, MsgProps, ChPid,
                           fun maybe_write_to_disk/4,
                           State),
    {SeqId, a(maybe_update_rates(State1))}.

discard(_MsgId, _ChPid, State) -> State.

drain_confirmed(State = #vqstate { confirmed = C }) ->
    case sets:is_empty(C) of
        true  -> {[], State}; %% common case
        false -> {sets:to_list(C), State #vqstate {
                                        confirmed = sets:new([{version, 2}]) }}
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

%% @todo It may seem like we would benefit from avoiding reading the
%%       message content from disk. But benchmarks tell a different
%%       story. So we don't, until a better understanding is gained.
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
        {MsgStatus = #msg_status{ msg_id = MsgId },
         State1 = #vqstate{ ack_out_counter = AckOutCount }} ->
            State2 = remove_from_disk(MsgStatus, State1),
            {[MsgId],
             a(State2 #vqstate { ack_out_counter  = AckOutCount + 1 })}
    end;
ack(AckTags, State) ->
    {{IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, AllMsgIds},
     State1 = #vqstate { index_state       = IndexState,
                         store_state       = StoreState0,
                         ack_out_counter   = AckOutCount }} =
        lists:foldl(
          fun (SeqId, {Acc, State2}) ->
                  %% @todo When acking many messages we should update stats once not for every.
                  %%       Also remove the pending acks all at once instead of every.
                  case remove_pending_ack(true, SeqId, State2) of
                      {none, _} ->
                          {Acc, State2};
                      {MsgStatus, State3} ->
                          {accumulate_ack(MsgStatus, Acc), State3}
                  end
          end, {accumulate_ack_init(), State}, AckTags),
    {DeletedSegments, IndexState1} = rabbit_classic_queue_index_v2:ack(IndexOnDiskSeqIds, IndexState),
    StoreState1 = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState0),
    StoreState = lists:foldl(fun rabbit_classic_queue_store_v2:remove/2, StoreState1, SeqIdsInStore),
    State2 = remove_vhost_msgs_by_id(MsgIdsByStore, State1),
    {lists:reverse(AllMsgIds),
     a(State2 #vqstate { index_state      = IndexState1,
                         store_state      = StoreState,
                         ack_out_counter  = AckOutCount + length(AckTags) })}.

requeue(AckTags, #vqstate { delta      = Delta,
                            q3         = Q3,
                            in_counter = InCounter,
                            len        = Len } = State) ->
    %% @todo This can be heavily simplified: if the message falls into delta,
    %%       add it there. Otherwise just add it to q3 in the correct position.
    {SeqIds, Q3a, MsgIds, State1} = requeue_merge(lists:sort(AckTags), Q3, [],
                                                  delta_limit(Delta), State),
    {Delta1, MsgIds1, State2}     = delta_merge(SeqIds, Delta, MsgIds,
                                                State1),
    MsgCount = length(MsgIds1),
    {MsgIds1, a(
                  maybe_update_rates(ui(
                    State2 #vqstate { delta      = Delta1,
                                      q3         = Q3a,
                                      in_counter = InCounter + MsgCount,
                                      len        = Len + MsgCount })))}.

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
                                      ram_ack_iterator(State)]),
    ifold(Fun, Acc, Its, State#vqstate{index_state = IndexState1}).

len(#vqstate { len = Len }) -> Len.

is_empty(State) -> 0 == len(State).

depth(State) ->
    len(State) + count_pending_acks(State).

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

needs_timeout(#vqstate { index_state = IndexState,
                         unconfirmed_simple = UCS }) ->
    case {rabbit_classic_queue_index_v2:needs_sync(IndexState), sets:is_empty(UCS)} of
        {false, false} -> timed;
        {confirms, _}  -> timed;
        {false, true}  -> false
    end.

timeout(State = #vqstate { index_state = IndexState0,
                           store_state = StoreState0,
                           unconfirmed_simple = UCS,
                           confirmed   = C }) ->
    IndexState = rabbit_classic_queue_index_v2:sync(IndexState0),
    StoreState = rabbit_classic_queue_store_v2:sync(StoreState0),
    State #vqstate { index_state = IndexState,
                     store_state = StoreState,
                     unconfirmed_simple = sets:new([{version,2}]),
                     confirmed   = sets:union(C, UCS) }.

handle_pre_hibernate(State = #vqstate { index_state = IndexState0,
                                        store_state = StoreState0,
                                        msg_store_clients = MSCState0,
                                        unconfirmed_simple = UCS,
                                        confirmed   = C }) ->
    MSCState = msg_store_pre_hibernate(MSCState0),
    IndexState = rabbit_classic_queue_index_v2:flush(IndexState0),
    StoreState = rabbit_classic_queue_store_v2:sync(StoreState0),
    State #vqstate { index_state = IndexState,
                     store_state = StoreState,
                     msg_store_clients = MSCState,
                     unconfirmed_simple = sets:new([{version,2}]),
                     confirmed   = sets:union(C, UCS) }.

resume(State) -> a(timeout(State)).

msg_rates(#vqstate { rates = #rates { in  = AvgIngressRate,
                                      out = AvgEgressRate } }) ->
    {AvgIngressRate, AvgEgressRate}.

info(messages_ready_ram, #vqstate{ram_msg_count = RamMsgCount}) ->
    RamMsgCount;
info(messages_unacknowledged_ram, #vqstate{ram_pending_ack = RPA}) ->
    map_size(RPA);
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
          ram_pending_ack  = RPA}) ->
    head_message_timestamp(Q3, RPA);
info(oldest_message_received_timestamp, #vqstate{
                                           q3               = Q3,
                                           ram_pending_ack  = RPA}) ->
    oldest_message_received_timestamp(Q3, RPA);
info(disk_reads, #vqstate{disk_read_count = Count}) ->
    Count;
info(disk_writes, #vqstate{disk_write_count = Count}) ->
    Count;
info(backing_queue_status, #vqstate {
          delta = Delta, q3 = Q3,
          mode             = Mode,
          len              = Len,
          target_ram_count = TargetRamCount,
          next_seq_id      = NextSeqId,
          next_deliver_seq_id = NextDeliverSeqId,
          ram_pending_ack  = RPA,
          disk_pending_ack = DPA,
          unconfirmed      = UC,
          unconfirmed_simple = UCS,
          index_state      = IndexState,
          store_state      = StoreState,
          rates            = #rates { in      = AvgIngressRate,
                                      out     = AvgEgressRate,
                                      ack_in  = AvgAckIngressRate,
                                      ack_out = AvgAckEgressRate }}) ->
    [ {mode                , Mode},
      {version             , 2},
      {q1                  , 0},
      {q2                  , 0},
      {delta               , Delta},
      {q3                  , ?QUEUE:len(Q3)},
      {q4                  , 0},
      {len                 , Len},
      {target_ram_count    , TargetRamCount},
      {next_seq_id         , NextSeqId},
      {next_deliver_seq_id , NextDeliverSeqId},
      {num_pending_acks    , map_size(RPA) + map_size(DPA)},
      {num_unconfirmed     , sets:size(UC) + sets:size(UCS)},
      {avg_ingress_rate    , AvgIngressRate},
      {avg_egress_rate     , AvgEgressRate},
      {avg_ack_ingress_rate, AvgAckIngressRate},
      {avg_ack_egress_rate , AvgAckEgressRate} ]
    ++ rabbit_classic_queue_index_v2:info(IndexState)
    ++ rabbit_classic_queue_store_v2:info(StoreState);
info(_, _) ->
    ''.

invoke(?MODULE, Fun, State) -> Fun(?MODULE, State);
invoke(      _,   _, State) -> State.

is_duplicate(_Msg, State) -> {false, State}.

%% Queue mode has been unified.
set_queue_mode(_, State) ->
    State.

zip_msgs_and_acks(Msgs, AckTags, Accumulator, _State) ->
    lists:foldl(fun ({{Msg, _Props}, AckTag}, Acc) ->
                        Id = mc:get_annotation(id, Msg),
                        [{Id, AckTag} | Acc]
                end, Accumulator, lists:zip(Msgs, AckTags)).

%% Queue version now ignored; only v2 is available.
set_queue_version(_, State) ->
    State.

%% This function is used by rabbit_classic_queue_index_v2
%% to convert v1 queues to v2 after an upgrade to 4.0.
convert_from_v1_to_v2_loop(_, _, V2Index, V2Store, _, HiSeqId, HiSeqId, _) ->
    {V2Index, V2Store};
convert_from_v1_to_v2_loop(QueueName, V1Index0, V2Index0, V2Store0,
                           Counters = {CountersRef, CountIx, BytesIx},
                           LoSeqId, HiSeqId, SkipFun) ->
    UpSeqId = lists:min([rabbit_queue_index:next_segment_boundary(LoSeqId),
                         HiSeqId]),
    {Messages, V1Index} = rabbit_queue_index:read(LoSeqId, UpSeqId, V1Index0),
    %% We do a garbage collect immediately after the old index read
    %% because that may have created a lot of garbage.
    garbage_collect(),
    {V2Index3, V2Store3} = lists:foldl(fun
        %% Move embedded messages to the per-queue store.
        ({Msg, SeqId, rabbit_queue_index, Props, IsPersistent},
         {V2Index1, V2Store1}) ->
            MsgId = mc:get_annotation(id, Msg),
            {MsgLocation, V2Store2} = rabbit_classic_queue_store_v2:write(SeqId, Msg, Props, V2Store1),
            V2Index2 = case SkipFun(SeqId, V2Index1) of
                {skip, V2Index1a} ->
                    V2Index1a;
                {write, V2Index1a} ->
                    counters:add(CountersRef, CountIx, 1),
                    counters:add(CountersRef, BytesIx, Props#message_properties.size),
                    rabbit_classic_queue_index_v2:publish(MsgId, SeqId, MsgLocation, Props, IsPersistent, infinity, V2Index1a)
            end,
            {V2Index2, V2Store2};
        %% Keep messages in the per-vhost store where they are.
        ({MsgId, SeqId, rabbit_msg_store, Props, IsPersistent},
         {V2Index1, V2Store1}) ->
            V2Index2 = case SkipFun(SeqId, V2Index1) of
                {skip, V2Index1a} ->
                    V2Index1a;
                {write, V2Index1a} ->
                    counters:add(CountersRef, CountIx, 1),
                    counters:add(CountersRef, BytesIx, Props#message_properties.size),
                    rabbit_classic_queue_index_v2:publish(MsgId, SeqId, rabbit_msg_store, Props, IsPersistent, infinity, V2Index1a)
            end,
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
    rabbit_log:info("Queue ~ts in vhost ~ts converted ~b messages from v1 to v2",
                    [Name, VHost, length(Messages)]),
    convert_from_v1_to_v2_loop(QueueName, V1Index, V2Index, V2Store, Counters, UpSeqId, HiSeqId, SkipFun).

%% Get the Timestamp property of the first msg, if present. This is
%% the one with the oldest timestamp among the heads of the pending
%% acks and unread queues.  We can't check disk_pending_acks as these
%% are paged out - we assume some will soon be paged in rather than
%% forcing it to happen.  Pending ack msgs are included as they are
%% regarded as unprocessed until acked, this also prevents the result
%% apparently oscillating during repeated rejects.
%%
head_message_timestamp(Q3, RPA) ->
    HeadMsgs = [ HeadMsgStatus#msg_status.msg ||
                   HeadMsgStatus <-
                       [ get_q_head(Q3),
                         get_pa_head(RPA) ],
                   HeadMsgStatus /= undefined,
                   HeadMsgStatus#msg_status.msg /= undefined ],

    Timestamps =
        [Timestamp div 1000
         || HeadMsg <- HeadMsgs,
            Timestamp <- [mc:timestamp(HeadMsg)],
            Timestamp /= undefined
        ],

    case Timestamps == [] of
        true -> '';
        false -> lists:min(Timestamps)
    end.

oldest_message_received_timestamp(Q3, RPA) ->
    HeadMsgs = [ HeadMsgStatus#msg_status.msg ||
                   HeadMsgStatus <-
                       [ get_q_head(Q3),
                         get_pa_head(RPA) ],
                   HeadMsgStatus /= undefined,
                   HeadMsgStatus#msg_status.msg /= undefined ],

    Timestamps =
        [Timestamp
         || HeadMsg <- HeadMsgs,
            Timestamp <- [mc:get_annotation(?ANN_RECEIVED_AT_TIMESTAMP, HeadMsg)],
            Timestamp /= undefined
        ],

    case Timestamps == [] of
        true -> '';
        false -> lists:min(Timestamps)
    end.

get_q_head(Q) ->
    ?QUEUE:get(Q, undefined).

get_pa_head(PA) ->
    case maps:keys(PA) of
        [] -> undefined;
        Keys ->
            Smallest = lists:min(Keys),
            map_get(Smallest, PA)
    end.

a(State = #vqstate { delta = Delta, q3 = Q3,
                     len              = Len,
                     bytes            = Bytes,
                     unacked_bytes    = UnackedBytes,
                     persistent_count = PersistentCount,
                     persistent_bytes = PersistentBytes,
                     ram_msg_count    = RamMsgCount,
                     ram_bytes        = RamBytes}) ->
    ED = Delta#delta.count == 0,
    E3 = ?QUEUE:is_empty(Q3),
    LZ = Len == 0,
    L3 = ?QUEUE:len(Q3),

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

msg_status(IsPersistent, IsDelivered, SeqId,
           Msg, MsgProps, IndexMaxSize) ->
    MsgId = mc:get_annotation(id, Msg),
    #msg_status{seq_id        = SeqId,
                msg_id        = MsgId,
                msg           = Msg,
                is_persistent = IsPersistent,
                %% This value will only be correct when the message is going out.
                %% See the set_deliver_flag/2 function.
                is_delivered  = IsDelivered,
                msg_location  = memory,
                index_on_disk = false,
                persist_to    = determine_persist_to(Msg, MsgProps, IndexMaxSize),
                msg_props     = MsgProps}.

beta_msg_status({MsgId, SeqId, MsgLocation, MsgProps, IsPersistent})
  when is_binary(MsgId) orelse
       MsgId =:= undefined ->
    MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent),
    MS0#msg_status{msg_id       = MsgId,
                   msg          = undefined,
                   persist_to   = case is_tuple(MsgLocation) of
                                      true  -> queue_store; %% @todo I'm not sure this clause is triggered anymore.
                                      false -> msg_store
                                  end,
                   msg_location = MsgLocation};
beta_msg_status({Msg, SeqId, MsgLocation, MsgProps, IsPersistent}) ->
    MsgId = mc:get_annotation(id, Msg),
    MS0 = beta_msg_status0(SeqId, MsgProps, IsPersistent),
    MS0#msg_status{msg_id       = MsgId,
                   msg          = Msg,
                   persist_to   = case MsgLocation of
                                      rabbit_queue_index -> queue_index;
                                      {rabbit_classic_queue_store_v2, _, _} -> queue_store;
                                      rabbit_msg_store -> msg_store
                                  end,
                   msg_location = case MsgLocation of
                                      rabbit_queue_index -> memory;
                                      _ -> MsgLocation
                                  end}.

beta_msg_status0(SeqId, MsgProps, IsPersistent) ->
  #msg_status{seq_id        = SeqId,
              msg           = undefined,
              is_persistent = IsPersistent,
              index_on_disk = true,
              msg_props     = MsgProps}.

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

msg_store_client_init(MsgStore, MsgOnDiskFun, VHost) ->
    msg_store_client_init(MsgStore, rabbit_guid:gen(), MsgOnDiskFun,
                          VHost).

msg_store_client_init(MsgStore, Ref, MsgOnDiskFun, VHost) ->
    rabbit_vhost_msg_store:client_init(VHost, MsgStore,
                                       Ref, MsgOnDiskFun).

msg_store_pre_hibernate({undefined, MSCStateT}) ->
    {undefined,
     rabbit_msg_store:client_pre_hibernate(MSCStateT)};
msg_store_pre_hibernate({MSCStateP, MSCStateT}) ->
    {rabbit_msg_store:client_pre_hibernate(MSCStateP),
     rabbit_msg_store:client_pre_hibernate(MSCStateT)}.

msg_store_write(MSCState, IsPersistent, SeqId, MsgId, Msg) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) ->
              rabbit_msg_store:write_flow(SeqId, MsgId, Msg, MSCState1)
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
                                         disk_pending_ack = DPA }) ->
    maps:is_key(SeqId, RPA) orelse
    maps:is_key(SeqId, DPA).

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

init(IsDurable, IndexState, StoreState, DeltaCount, DeltaBytes, Terms,
     PersistentClient, TransientClient, VHost) ->
    {LowSeqId, HiSeqId, IndexState1} = rabbit_classic_queue_index_v2:bounds(IndexState),

    {NextSeqId, NextDeliverSeqId, DeltaCount1, DeltaBytes1} =
        case Terms of
            non_clean_shutdown -> {HiSeqId, HiSeqId, DeltaCount, DeltaBytes};
            _                  -> NextSeqId0 = proplists:get_value(next_seq_id,
                                                                   Terms, HiSeqId),
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
                                    end_seq_id   = NextSeqId })
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
      ram_pending_ack     = #{},
      disk_pending_ack    = #{},
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
      msgs_on_disk        = sets:new([{version,2}]),
      msg_indices_on_disk = sets:new([{version,2}]),
      unconfirmed         = sets:new([{version,2}]),
      unconfirmed_simple  = sets:new([{version,2}]),
      confirmed           = sets:new([{version,2}]),
      ack_out_counter     = 0,
      ack_in_counter      = 0,
      disk_read_count     = 0,
      disk_write_count    = 0,

      io_batch_size       = IoBatchSize,

      mode                = default,
      virtual_host        = VHost},
    a(maybe_deltas_to_betas(State)).

blank_rates(Now) ->
    #rates { in        = 0.0,
             out       = 0.0,
             ack_in    = 0.0,
             ack_out   = 0.0,
             timestamp = Now}.

in_r(MsgStatus = #msg_status {}, State = #vqstate { q3 = Q3 }) ->
    State #vqstate { q3 = ?QUEUE:in_r(MsgStatus, Q3) }.

queue_out(State) ->
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
    {{ok, Msg}, MSCState1} =
        msg_store_read(MSCState, IsPersistent, MsgId),
    {Msg, State #vqstate {msg_store_clients = MSCState1,
                          disk_read_count   = Count + 1}}.

%% Helper macros to make the code as obvious as possible.
%% It's OK to call msg_size/1 for Inc because it gets inlined.
-define(UP(A, Inc),
    A = St#vqstate.A + Inc).
-define(UP(A, B, Inc),
    A = St#vqstate.A + Inc,
    B = St#vqstate.B + Inc).
-define(UP(A, B, C, Inc),
    A = St#vqstate.A + Inc,
    B = St#vqstate.B + Inc,
    C = St#vqstate.C + Inc).

%% When publishing to memory, transient messages do not get written to disk.
%% On the other hand, persistent messages are kept in memory as well as disk.
stats_published_memory(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(len, ram_msg_count, persistent_count, +1),
               ?UP(bytes, ram_bytes, persistent_bytes, +msg_size(MS))};
stats_published_memory(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(len, ram_msg_count, +1),
               ?UP(bytes, ram_bytes, +msg_size(MS))}.

%% Messages published directly to disk are not kept in memory.
stats_published_disk(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(len, persistent_count, +1),
               ?UP(bytes, persistent_bytes, +msg_size(MS))};
stats_published_disk(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(len, +1),
               ?UP(bytes, delta_transient_bytes, +msg_size(MS))}.

%% Pending acks do not add to len. Messages are kept in memory.
stats_published_pending_acks(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(persistent_count, +1),
               ?UP(persistent_bytes, unacked_bytes, ram_bytes, +msg_size(MS))};
stats_published_pending_acks(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(unacked_bytes, ram_bytes, +msg_size(MS))}.

%% Messages are moved from memory to pending acks. They may have
%% the message body either in memory or on disk depending on how
%% the message got to memory in the first place (if the message
%% was fully on disk the content will not be read immediately).
%% The contents stay where they are during this operation.
stats_pending_acks(MS = #msg_status{msg = undefined}, St) ->
    St#vqstate{?UP(len, -1),
               ?UP(bytes, -msg_size(MS)), ?UP(unacked_bytes, +msg_size(MS))};
stats_pending_acks(MS, St) ->
    St#vqstate{?UP(len, ram_msg_count, -1),
               ?UP(bytes, -msg_size(MS)), ?UP(unacked_bytes, +msg_size(MS))}.

%% Message may or may not be persistent and the contents
%% may or may not be in memory.
%%
%% Removal from delta_transient_bytes is done by maybe_deltas_to_betas.
stats_removed(MS = #msg_status{is_persistent = true, msg = undefined}, St) ->
    St#vqstate{?UP(len, persistent_count, -1),
               ?UP(bytes, persistent_bytes, -msg_size(MS))};
stats_removed(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(len, ram_msg_count, persistent_count, -1),
               ?UP(bytes, ram_bytes, persistent_bytes, -msg_size(MS))};
stats_removed(MS = #msg_status{is_persistent = false, msg = undefined}, St) ->
    St#vqstate{?UP(len, -1), ?UP(bytes, -msg_size(MS))};
stats_removed(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(len, ram_msg_count, -1),
               ?UP(bytes, ram_bytes, -msg_size(MS))}.

%% @todo Very confusing that ram_msg_count is without unacked but ram_bytes is with.
%%       Rename the fields to make these things obvious. Fields are internal
%%       so that should be OK.

stats_acked_pending(MS = #msg_status{is_persistent = true, msg = undefined}, St) ->
    St#vqstate{?UP(persistent_count, -1),
               ?UP(persistent_bytes, unacked_bytes, -msg_size(MS))};
stats_acked_pending(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(persistent_count, -1),
               ?UP(persistent_bytes, unacked_bytes, ram_bytes, -msg_size(MS))};
stats_acked_pending(MS = #msg_status{is_persistent = false,
                                     msg           = undefined}, St) ->
    St#vqstate{?UP(unacked_bytes, -msg_size(MS))};
stats_acked_pending(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(unacked_bytes, ram_bytes, -msg_size(MS))}.

%% Notice that this is the reverse of stats_pending_acks.
stats_requeued_memory(MS = #msg_status{msg = undefined}, St) ->
    St#vqstate{?UP(len, +1),
               ?UP(bytes, +msg_size(MS)), ?UP(unacked_bytes, -msg_size(MS))};
stats_requeued_memory(MS, St) ->
    St#vqstate{?UP(len, ram_msg_count, +1),
               ?UP(bytes, +msg_size(MS)), ?UP(unacked_bytes, -msg_size(MS))}.

%% @todo For v2 since we don't remove from disk until we ack, we don't need
%%       to write to disk again on requeue. If the message falls within delta
%%       we can just drop the MsgStatus. Otherwise we just put it in q3 and
%%       we don't do any disk writes.
%%
%%       For v1 I'm not sure? I don't think we need to write to the index
%%       at least, but maybe we need to write the message if not embedded?
%%       I don't think we need to...
%%
%%       So we don't need to change anything except how we count stats as
%%       well as delta stats if the message falls within delta.
stats_requeued_disk(MS = #msg_status{is_persistent = true}, St) ->
    St#vqstate{?UP(len, +1),
               ?UP(bytes, +msg_size(MS)), ?UP(unacked_bytes, -msg_size(MS))};
stats_requeued_disk(MS = #msg_status{is_persistent = false}, St) ->
    St#vqstate{?UP(len, +1),
               ?UP(bytes, delta_transient_bytes, +msg_size(MS)),
               ?UP(unacked_bytes, -msg_size(MS))}.

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

    State2 = stats_pending_acks(MsgStatus, State1),

    {SeqId, maybe_update_rates(
              State2 #vqstate {next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                               out_counter         = OutCount + 1,
                               index_state         = IndexState1})};

%% This function body has the same behaviour as remove_queue_entries/3
%% but instead of removing messages based on a ?QUEUE, this removes
%% just one message, the one referenced by the MsgStatus provided.
remove(false, MsgStatus = #msg_status{ seq_id = SeqId },
              State = #vqstate{ next_deliver_seq_id = NextDeliverSeqId,
                                out_counter = OutCount }) ->
    State1 = remove_from_disk(MsgStatus, State),

    State2 = stats_removed(MsgStatus, State1),

    {undefined, maybe_update_rates(
                  State2 #vqstate {next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                                   out_counter         = OutCount + 1 })}.

remove_from_disk(#msg_status {
                seq_id        = SeqId,
                msg_id        = MsgId,
                is_persistent = IsPersistent,
                msg_location  = MsgLocation,
                index_on_disk = IndexOnDisk },
       State = #vqstate {index_state         = IndexState1,
                         store_state         = StoreState0,
                         msg_store_clients   = MSCState}) ->
    {DeletedSegments, IndexState2} =
        case IndexOnDisk of
            true  -> rabbit_classic_queue_index_v2:ack([SeqId], IndexState1);
            false -> {[], IndexState1}
        end,
    {StoreState1, State1} = case MsgLocation of
        ?IN_SHARED_STORE  ->
            case msg_store_remove(MSCState, IsPersistent, [{SeqId, MsgId}]) of
                {ok, []} ->
                    {StoreState0, State};
                {ok, [_]} ->
                    {StoreState0, record_confirms(sets:add_element(MsgId, sets:new([{version,2}])), State)}
            end;
        ?IN_QUEUE_STORE   -> {rabbit_classic_queue_store_v2:remove(SeqId, StoreState0), State};
        ?IN_QUEUE_INDEX   -> {StoreState0, State};
        ?IN_MEMORY        -> {StoreState0, State}
    end,
    StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState1),
    State1#vqstate{
        index_state = IndexState2,
        store_state = StoreState
    }.

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
%%
%% @todo We cannot just read the metadata to drop the messages because
%%       there are messages we are not going to remove. Those messages
%%       will later on be consumed and their content read; only with
%%       a less efficient operation. This results in a drop in performance
%%       for long queues.
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
                   State = #vqstate { out_counter = OutCount}) ->
    {MsgProps, QAcc, State1} =
        collect_by_predicate(Pred, ?QUEUE:new(), State),

    {NextDeliverSeqId, FetchAcc1, State2} =
        process_queue_entries(QAcc, Fun, FetchAcc, State1),

    {MsgProps, FetchAcc1, maybe_update_rates(
                            State2 #vqstate {
                              next_deliver_seq_id = NextDeliverSeqId,
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
    ?QUEUE:fold(fun (MsgStatus, Acc) ->
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
     stats_pending_acks(MsgStatus, State2)}.

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
    a(reset_qi_state(State1)).

%% This function removes messages from each of delta and q3.
%%
%% purge_betas_and_deltas/2 loads messages from the queue index,
%% filling up q3. The messages loaded into q3 are removed by calling
%% remove_queue_entries/3 until there are no more messages to be read
%% from the queue index. Messages are read in batches from the queue
%% index.
purge1(AfterFun, State) ->
    a(purge_betas_and_deltas(AfterFun, State)).

reset_qi_state(State = #vqstate{ index_state = IndexState0,
                                 store_state = StoreState0 }) ->
    StoreState = rabbit_classic_queue_store_v2:terminate(StoreState0),
    IndexState = rabbit_classic_queue_index_v2:reset_state(IndexState0),
    State#vqstate{ index_state = IndexState,
                   store_state = StoreState }.

is_pending_ack_empty(State) ->
    count_pending_acks(State) =:= 0.

is_unconfirmed_empty(#vqstate { unconfirmed = UC, unconfirmed_simple = UCS }) ->
    sets:is_empty(UC) andalso sets:is_empty(UCS).

count_pending_acks(#vqstate { ram_pending_ack   = RPA,
                              disk_pending_ack  = DPA }) ->
    map_size(RPA) + map_size(DPA).

%% @todo When doing maybe_deltas_to_betas stats are updated. Then stats
%%       are updated again in remove_queue_entries1. All unnecessary since
%%       we are purging anyway?
purge_betas_and_deltas(DelsAndAcksFun, State) ->
    %% We use the maximum memory limit when purging to get greater performance.
    MemoryLimit = 2048,
    State0 = #vqstate { q3 = Q3 } = maybe_deltas_to_betas(DelsAndAcksFun, State, MemoryLimit, metadata_only),

    case ?QUEUE:is_empty(Q3) of
        true  -> State0;
        false -> State1 = remove_queue_entries(Q3, DelsAndAcksFun, State0),
                 purge_betas_and_deltas(DelsAndAcksFun, State1#vqstate{q3 = ?QUEUE:new()})
    end.

remove_queue_entries(Q, DelsAndAcksFun,
                     State = #vqstate{next_deliver_seq_id = NextDeliverSeqId0}) ->
    {MsgIdsByStore, NextDeliverSeqId, Acks, State1} =
        ?QUEUE:fold(fun remove_queue_entries1/2,
                    {maps:new(), NextDeliverSeqId0, [], State}, Q),
    State2 = remove_vhost_msgs_by_id(MsgIdsByStore, State1),
    DelsAndAcksFun(NextDeliverSeqId, Acks, State2).

remove_queue_entries1(
  #msg_status { msg_id = MsgId, seq_id = SeqId,
                msg_location = MsgLocation, index_on_disk = IndexOnDisk,
                is_persistent = IsPersistent} = MsgStatus,
  {MsgIdsByStore, NextDeliverSeqId, Acks, State}) ->
    {case MsgLocation of
         ?IN_SHARED_STORE -> rabbit_misc:maps_cons(IsPersistent, {SeqId, MsgId}, MsgIdsByStore);
         _ -> MsgIdsByStore
     end,
     next_deliver_seq_id(SeqId, NextDeliverSeqId),
     cons_if(IndexOnDisk, SeqId, Acks),
     %% @todo Probably don't do this on a per-message basis...
     stats_removed(MsgStatus, State)}.

process_delivers_and_acks_fun(deliver_and_ack) ->
    %% @todo Make a clause for empty Acks list?
    fun (NextDeliverSeqId, Acks, State = #vqstate { index_state = IndexState,
                                                    store_state = StoreState0}) ->
            {DeletedSegments, IndexState1} = rabbit_classic_queue_index_v2:ack(Acks, IndexState),

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

publish1(Msg,
         MsgProps = #message_properties { needs_confirming = NeedsConfirming },
         IsDelivered, _ChPid, PersistFun,
         State = #vqstate { q3 = Q3, delta = Delta = #delta { count = DeltaCount },
                            len                 = Len,
                            qi_embed_msgs_below = IndexMaxSize,
                            next_seq_id         = SeqId,
                            next_deliver_seq_id = NextDeliverSeqId,
                            in_counter          = InCount,
                            durable             = IsDurable,
                            unconfirmed         = UC,
                            unconfirmed_simple  = UCS,
                            rates               = #rates{ out = OutRate }}) ->
    MsgId = mc:get_annotation(id, Msg),
    IsPersistent = mc:is_persistent(Msg),
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(IsPersistent1, IsDelivered, SeqId, Msg, MsgProps, IndexMaxSize),
    %% We allow from 1 to 2048 messages in memory depending on the consume rate. The lower
    %% limit is at 1 because the queue process will need to access this message to know
    %% expiration information.
    MemoryLimit = min(1 + floor(2 * OutRate), 2048),
    State3 = case DeltaCount of
                 %% Len is the same as Q3Len when DeltaCount =:= 0.
                 0 when Len < MemoryLimit ->
                     {MsgStatus1, State1} = PersistFun(false, false, MsgStatus, State),
                     State2 = State1 #vqstate { q3 = ?QUEUE:in(m(MsgStatus1), Q3) },
                     stats_published_memory(MsgStatus1, State2);
                 _ ->
                     {MsgStatus1, State1} = PersistFun(true, true, MsgStatus, State),
                     Delta1 = expand_delta(SeqId, Delta, IsPersistent),
                     State2 = State1 #vqstate { delta = Delta1 },
                     stats_published_disk(MsgStatus1, State2)
             end,
    {UC1, UCS1} = maybe_needs_confirming(NeedsConfirming, persist_to(MsgStatus),
                                         MsgId, UC, UCS),
    State3#vqstate{ next_seq_id         = SeqId + 1,
                    next_deliver_seq_id = maybe_next_deliver_seq_id(SeqId, NextDeliverSeqId, IsDelivered),
                    in_counter          = InCount + 1,
                    unconfirmed         = UC1,
                    unconfirmed_simple  = UCS1 }.

%% Only attempt to increase the next_deliver_seq_id for delivered messages.
maybe_next_deliver_seq_id(SeqId, NextDeliverSeqId, true) ->
    next_deliver_seq_id(SeqId, NextDeliverSeqId);
maybe_next_deliver_seq_id(_, NextDeliverSeqId, false) ->
    NextDeliverSeqId.

publish_delivered1(Msg,
                   MsgProps = #message_properties {
                                 needs_confirming = NeedsConfirming },
                   _ChPid, PersistFun,
                   State = #vqstate { qi_embed_msgs_below = IndexMaxSize,
                                      next_seq_id         = SeqId,
                                      next_deliver_seq_id = NextDeliverSeqId,
                                      in_counter          = InCount,
                                      out_counter         = OutCount,
                                      durable             = IsDurable,
                                      unconfirmed         = UC,
                                      unconfirmed_simple  = UCS }) ->
    MsgId = mc:get_annotation(id, Msg),
    IsPersistent = mc:is_persistent(Msg),
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = msg_status(IsPersistent1, true, SeqId, Msg, MsgProps, IndexMaxSize),
    {MsgStatus1, State1} = PersistFun(false, false, MsgStatus, State),
    State2 = record_pending_ack(m(MsgStatus1), State1),
    {UC1, UCS1} = maybe_needs_confirming(NeedsConfirming, persist_to(MsgStatus),
                                         MsgId, UC, UCS),
    {SeqId,
     stats_published_pending_acks(MsgStatus1,
           State2#vqstate{ next_seq_id         = SeqId + 1,
                           next_deliver_seq_id = next_deliver_seq_id(SeqId, NextDeliverSeqId),
                           out_counter         = OutCount + 1,
                           in_counter          = InCount + 1,
                           unconfirmed         = UC1,
                           unconfirmed_simple  = UCS1 })}.

maybe_needs_confirming(false, _, _, UC, UCS) ->
    {UC, UCS};
%% When storing to the v2 queue store we take the simple confirms
%% path because we don't need to track index and store separately.
maybe_needs_confirming(true, queue_store, MsgId, UC, UCS) ->
    {UC, sets:add_element(MsgId, UCS)};
%% Otherwise we keep tracking as it used to be.
maybe_needs_confirming(true, _, MsgId, UC, UCS) ->
    {sets:add_element(MsgId, UC), UCS}.

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
        msg_store   -> ok = msg_store_write(MSCState, IsPersistent, SeqId, MsgId,
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
                                           index_state      = IndexState})
  when Force orelse IsPersistent ->
    {MsgOrId, DiskWriteCount1} =
        case persist_to(MsgStatus) of
            msg_store   -> {MsgId, DiskWriteCount};
            queue_store -> {MsgId, DiskWriteCount};
            queue_index -> {prepare_to_store(Msg), DiskWriteCount + 1}
        end,
    IndexState1 = rabbit_classic_queue_index_v2:pre_publish(
        MsgOrId, SeqId, MsgLocation, MsgProps,
        IsPersistent, TargetRamCount, IndexState),
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
                                           index_state      = IndexState})
  when Force orelse IsPersistent ->
    {MsgOrId, DiskWriteCount1} =
        case persist_to(MsgStatus) of
            msg_store   -> {MsgId, DiskWriteCount};
            queue_store -> {MsgId, DiskWriteCount};
            queue_index -> {prepare_to_store(Msg), DiskWriteCount + 1}
        end,
    IndexState2 = rabbit_classic_queue_index_v2:publish(
                    MsgOrId, SeqId, MsgLocation, MsgProps, IsPersistent,
                    persist_to(MsgStatus) =:= msg_store, TargetRamCount,
                    IndexState),
    {MsgStatus#msg_status{index_on_disk = true},
     State#vqstate{index_state      = IndexState2,
                   disk_write_count = DiskWriteCount1}};

maybe_write_index_to_disk(_Force, MsgStatus, State) ->
    {MsgStatus, State}.

maybe_write_to_disk(ForceMsg, ForceIndex, MsgStatus, State) ->
    {MsgStatus1, State1} = maybe_write_msg_to_disk(ForceMsg, MsgStatus, State),
    maybe_write_index_to_disk(ForceIndex, MsgStatus1, State1).

maybe_prepare_write_to_disk(ForceMsg, ForceIndex0, MsgStatus, State) ->
    {MsgStatus1, State1} = maybe_write_msg_to_disk(ForceMsg, MsgStatus, State),
    %% We want messages written to the v2 per-queue store to also
    %% be written to the index for proper accounting. The situation
    %% where a message can be in the store but not in the index can
    %% only occur when going through this function (not via maybe_write_to_disk).
    ForceIndex = case persist_to(MsgStatus) of
        queue_store -> true;
        _ -> ForceIndex0
    end,
    maybe_batch_write_index_to_disk(ForceIndex, MsgStatus1, State1).

determine_persist_to(Msg,
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

    {MetaSize, _BodySize} = mc:size(Msg),
     case BodySize >= IndexMaxSize of
         true  -> msg_store;
         false ->
             Est = MetaSize + BodySize,
             case Est >= IndexMaxSize of
                 true  -> msg_store;
                 false -> queue_store
             end
     end.

persist_to(#msg_status{persist_to = To}) -> To.

prepare_to_store(Msg) ->
    mc:prepare(store, Msg).

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#msg_status { seq_id = SeqId } = MsgStatus,
                   State = #vqstate { ram_pending_ack  = RPA,
                                      disk_pending_ack = DPA,
                                      ack_in_counter   = AckInCount}) ->
    {RPA1, DPA1} =
        case msg_in_ram(MsgStatus) of
            false -> {RPA, maps:put(SeqId, MsgStatus, DPA)};
            _     -> {maps:put(SeqId, MsgStatus, RPA), DPA}
        end,
    State #vqstate { ram_pending_ack  = RPA1,
                     disk_pending_ack = DPA1,
                     ack_in_counter   = AckInCount + 1}.

lookup_pending_ack(SeqId, #vqstate { ram_pending_ack  = RPA,
                                     disk_pending_ack = DPA}) ->
    case maps:get(SeqId, RPA, none) of
        none -> maps:get(SeqId, DPA);
        V    -> V
    end.

%% First parameter = UpdateStats
%% @todo Do the stats updating outside of this function.
remove_pending_ack(true, SeqId, State) ->
    case remove_pending_ack(false, SeqId, State) of
        {none, _} ->
            {none, State};
        {MsgStatus, State1} ->
            {MsgStatus, stats_acked_pending(MsgStatus, State1)}
    end;
remove_pending_ack(false, SeqId, State = #vqstate{ram_pending_ack  = RPA,
                                                  disk_pending_ack = DPA}) ->
    case maps:get(SeqId, RPA, none) of
        none -> case maps:get(SeqId, DPA, none) of
                     none ->
                         {none, State};
                     V ->
                         DPA1 = maps:remove(SeqId, DPA),
                         {V, State#vqstate{disk_pending_ack = DPA1}}
                 end;
        V    -> RPA1 = maps:remove(SeqId, RPA),
                {V, State #vqstate { ram_pending_ack = RPA1 }}
    end.

purge_pending_ack(KeepPersistent,
                  State = #vqstate { index_state       = IndexState,
                                     store_state       = StoreState0 }) ->
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, State1} = purge_pending_ack1(State),
    case KeepPersistent of
        true  -> remove_transient_msgs_by_id(MsgIdsByStore, State1);
        false -> {DeletedSegments, IndexState1} =
                     rabbit_classic_queue_index_v2:ack(IndexOnDiskSeqIds, IndexState),
                 StoreState1 = lists:foldl(fun rabbit_classic_queue_store_v2:remove/2, StoreState0, SeqIdsInStore),
                 StoreState = rabbit_classic_queue_store_v2:delete_segments(DeletedSegments, StoreState1),
                 State2 = remove_vhost_msgs_by_id(MsgIdsByStore, State1),
                 State2 #vqstate { index_state = IndexState1,
                                   store_state = StoreState }
    end.

purge_pending_ack_delete_and_terminate(
  State = #vqstate { index_state       = IndexState,
                     store_state       = StoreState }) ->
    {_, MsgIdsByStore, _SeqIdsInStore, State1} = purge_pending_ack1(State),
    StoreState1 = rabbit_classic_queue_store_v2:terminate(StoreState),
    IndexState1 = rabbit_classic_queue_index_v2:delete_and_terminate(IndexState),
    State2 = remove_vhost_msgs_by_id(MsgIdsByStore, State1),
    State2 #vqstate { index_state = IndexState1,
                      store_state = StoreState1 }.

purge_pending_ack1(State = #vqstate { ram_pending_ack   = RPA,
                                      disk_pending_ack  = DPA }) ->
    F = fun (_SeqId, MsgStatus, Acc) -> accumulate_ack(MsgStatus, Acc) end,
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, _AllMsgIds} =
        maps:fold(F, maps:fold(F, accumulate_ack_init(), RPA), DPA),
    State1 = State #vqstate{ram_pending_ack  = #{},
                            disk_pending_ack = #{}},
    {IndexOnDiskSeqIds, MsgIdsByStore, SeqIdsInStore, State1}.

%% MsgIdsByStore is an map with two keys:
%%
%% true: holds a list of Persistent Message Ids.
%% false: holds a list of Transient Message Ids.
%%
%% The msg_store_remove/3 function takes this boolean flag to determine
%% from which store the messages should be removed from.
remove_vhost_msgs_by_id(MsgIdsByStore,
                        State = #vqstate{ msg_store_clients = MSCState }) ->
    maps:fold(fun(IsPersistent, MsgIds, StateF) ->
        case msg_store_remove(MSCState, IsPersistent, MsgIds) of
            {ok, []} ->
                StateF;
            {ok, ConfirmMsgIds} ->
                record_confirms(sets:from_list(ConfirmMsgIds, [{version, 2}]), StateF)
        end
    end, State, MsgIdsByStore).

remove_transient_msgs_by_id(MsgIdsByStore, State) ->
    remove_vhost_msgs_by_id(maps:with([false], MsgIdsByStore), State).

accumulate_ack_init() -> {[], maps:new(), [], []}.

accumulate_ack(#msg_status { seq_id        = SeqId,
                             msg_id        = MsgId,
                             is_persistent = IsPersistent,
                             msg_location  = MsgLocation,
                             index_on_disk = IndexOnDisk },
               {IndexOnDiskSeqIdsAcc, MsgIdsByStore, SeqIdsInStore, AllMsgIds}) ->
    {cons_if(IndexOnDisk, SeqId, IndexOnDiskSeqIdsAcc),
     case MsgLocation of
         ?IN_SHARED_STORE -> rabbit_misc:maps_cons(IsPersistent, {SeqId, MsgId}, MsgIdsByStore);
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
      msgs_on_disk        = sets_subtract(MOD,  MsgIdSet),
      msg_indices_on_disk = sets_subtract(MIOD, MsgIdSet),
      unconfirmed         = sets_subtract(UC,   MsgIdSet),
      confirmed           = sets:union(C, MsgIdSet) }.

%% Function defined in both rabbit_msg_store and rabbit_variable_queue.
sets_subtract(Set1, Set2) ->
    case sets:size(Set2) of
        1 -> sets:del_element(hd(sets:to_list(Set2)), Set1);
        _ -> sets:subtract(Set1, Set2)
    end.

msgs_written_to_disk(Callback, MsgIdSet, ignored) ->
    %% The message was already acked so it doesn't matter if it was never written
    %% to the index, we can process the confirm.
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
                     Confirmed = sets:intersection(UC, MsgIdSet),
                     record_confirms(sets:intersection(MsgIdSet, MIOD),
                                     State #vqstate {
                                       msgs_on_disk =
                                           sets:union(MOD, Confirmed) })
             end).

msg_indices_written_to_disk(Callback, MsgIdSet) ->
    Callback(?MODULE,
             fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
                                              msg_indices_on_disk = MIOD,
                                              unconfirmed         = UC }) ->
                     Confirmed = sets:intersection(UC, MsgIdSet),
                     record_confirms(sets:intersection(MsgIdSet, MOD),
                                     State #vqstate {
                                       msg_indices_on_disk =
                                           sets:union(MIOD, Confirmed) })
             end).

%% @todo Having to call run_backing_queue is probably reducing performance...
msgs_and_indices_written_to_disk(Callback, MsgIdSet) ->
    Callback(?MODULE,
             fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end).

%%----------------------------------------------------------------------------
%% Internal plumbing for requeue
%%----------------------------------------------------------------------------

%% Rebuild queue, inserting sequence ids to maintain ordering
requeue_merge(SeqIds, Q, MsgIds, Limit, State) ->
    requeue_merge(SeqIds, Q, ?QUEUE:new(), MsgIds,
                Limit, State).

requeue_merge([SeqId | Rest] = SeqIds, Q, Front, MsgIds,
            Limit, State)
  when Limit == undefined orelse SeqId < Limit ->
    case ?QUEUE:out(Q) of
        {{value, #msg_status { seq_id = SeqIdQ } = MsgStatus}, Q1}
          when SeqIdQ < SeqId ->
            %% enqueue from the remaining queue
            requeue_merge(SeqIds, Q1, ?QUEUE:in(MsgStatus, Front), MsgIds,
                        Limit, State);
        {_, _Q1} ->
            %% enqueue from the remaining list of sequence ids
            case msg_from_pending_ack(SeqId, State) of
                {none, _} ->
                    requeue_merge(Rest, Q, Front, MsgIds, Limit, State);
                {#msg_status { msg_id = MsgId } = MsgStatus, State1} ->
                    State2 = stats_requeued_memory(MsgStatus, State1),
                    requeue_merge(Rest, Q, ?QUEUE:in(MsgStatus, Front), [MsgId | MsgIds],
                                Limit, State2)
            end
    end;
requeue_merge(SeqIds, Q, Front, MsgIds,
            _Limit, State) ->
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
                                 stats_requeued_disk(MsgStatus, State2)}
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

delta_limit(?BLANK_DELTA_PATTERN(_))              -> undefined;
delta_limit(#delta { start_seq_id = StartSeqId }) -> StartSeqId.

%%----------------------------------------------------------------------------
%% Iterator
%%----------------------------------------------------------------------------

ram_ack_iterator(State) ->
    {ack, maps:iterator(State#vqstate.ram_pending_ack)}.

disk_ack_iterator(State) ->
    {ack, maps:iterator(State#vqstate.disk_pending_ack)}.

msg_iterator(State) -> istate(start, State).

istate(start, State) -> {q3,    State#vqstate.q3,    State};
istate(q3,    State) -> {delta, State#vqstate.delta, State};
istate(delta, _State) -> done.

next({ack, It}, IndexState) ->
    case maps:next(It) of
        none                     -> {empty, IndexState};
        {_SeqId, MsgStatus, It1} -> Next = {ack, It1},
                                    {value, MsgStatus, true, Next, IndexState}
    end;
next(done, IndexState) -> {empty, IndexState};
next({delta, #delta{start_seq_id = SeqId,
                    end_seq_id   = SeqId}, State}, IndexState) ->
    next(istate(delta, State), IndexState);
next({delta, #delta{start_seq_id = SeqId,
                    end_seq_id   = SeqIdEnd} = Delta, State}, IndexState) ->
    SeqIdB = rabbit_classic_queue_index_v2:next_segment_boundary(SeqId),
    %% It may make sense to limit this based on rate. But this
    %% is not called outside of CMQs so I will leave it alone
    %% for the time being.
    SeqId1 = lists:min([SeqIdB,
                        %% We must limit the number of messages read at once
                        %% otherwise the queue will attempt to read up to segment_entry_count()
                        %% messages from the index each time. The value
                        %% chosen here is arbitrary.
                        SeqId + 2048,
                        SeqIdEnd]),
    {List, IndexState1} = rabbit_classic_queue_index_v2:read(SeqId, SeqId1, IndexState),
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

fetch_from_q3(State = #vqstate { delta = #delta { count = DeltaCount },
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

%% Thresholds for doing multi-read against the shared message
%% stores. The values have been obtained through numerous
%% experiments. Be careful when editing these values: after a
%% certain size the performance drops and it becomes no longer
%% interesting to keep the extra data in memory.
-define(SHARED_READ_MANY_SIZE_THRESHOLD, 12000).
-define(SHARED_READ_MANY_COUNT_THRESHOLD, 10).

maybe_deltas_to_betas(State = #vqstate { rates = #rates{ out = OutRate }}) ->
    AfterFun = process_delivers_and_acks_fun(deliver_and_ack),
    %% We allow from 1 to 2048 messages in memory depending on the consume rate.
    MemoryLimit = min(1 + floor(2 * OutRate), 2048),
    maybe_deltas_to_betas(AfterFun, State, MemoryLimit, messages).

maybe_deltas_to_betas(_DelsAndAcksFun,
                      State = #vqstate {delta = ?BLANK_DELTA_PATTERN(X) },
                      _MemoryLimit, _WhatToRead) ->
    State;
maybe_deltas_to_betas(DelsAndAcksFun,
                      State = #vqstate {
                        delta                = Delta,
                        q3                   = Q3,
                        index_state          = IndexState,
                        store_state          = StoreState,
                        msg_store_clients    = {MCStateP, MCStateT},
                        ram_msg_count        = RamMsgCount,
                        ram_bytes            = RamBytes,
                        disk_read_count      = DiskReadCount,
                        delta_transient_bytes = DeltaTransientBytes,
                        transient_threshold  = TransientThreshold },
                      MemoryLimit, WhatToRead) ->
    #delta { start_seq_id = DeltaSeqId,
             count        = DeltaCount,
             transient    = Transient,
             end_seq_id   = DeltaSeqIdEnd } = Delta,
    %% For v2 we want to limit the number of messages read at once to lower
    %% the memory footprint. We use the consume rate to determine how many
    %% messages we read.
    DeltaSeqLimit = DeltaSeqId + MemoryLimit,
    DeltaSeqId1 =
        lists:min([rabbit_classic_queue_index_v2:next_segment_boundary(DeltaSeqId),
                   DeltaSeqLimit, DeltaSeqIdEnd]),
    {List0, IndexState1} = rabbit_classic_queue_index_v2:read(DeltaSeqId, DeltaSeqId1, IndexState),
    {List, StoreState3, MCStateP3, MCStateT3} = case WhatToRead of
        messages ->
            %% We try to read messages from disk all at once instead of
            %% 1 by 1 at fetch time. When v1 is used and messages are
            %% embedded, then the message content is already read from
            %% disk at this point. For v2 embedded we must do a separate
            %% call to obtain the contents and then merge the contents
            %% back into the #msg_status records.
            %%
            %% For shared message store messages we do the same but only
            %% for messages < ?SHARED_READ_MANY_SIZE_THRESHOLD bytes and
            %% when there are at least ?SHARED_READ_MANY_COUNT_THRESHOLD
            %% messages to fetch from that store. Other messages will be
            %% fetched one by one right before sending the messages.
            %%
            %% Since we have two different shared stores for persistent
            %% and transient messages they are treated separately when
            %% deciding whether to read_many from either of them.
            %%
            %% Because v2 and shared stores function differently we
            %% must keep different information for performing the reads.
            {V2Reads0, ShPersistReads, ShTransientReads} = lists:foldl(fun
                ({_, SeqId, MsgLocation, _, _}, {V2ReadsAcc, ShPReadsAcc, ShTReadsAcc}) when is_tuple(MsgLocation) ->
                    {[{SeqId, MsgLocation}|V2ReadsAcc], ShPReadsAcc, ShTReadsAcc};
                ({MsgId, _, rabbit_msg_store, #message_properties{size = Size}, true},
                 {V2ReadsAcc, ShPReadsAcc, ShTReadsAcc}) when Size =< ?SHARED_READ_MANY_SIZE_THRESHOLD ->
                    {V2ReadsAcc, [MsgId|ShPReadsAcc], ShTReadsAcc};
                ({MsgId, _, rabbit_msg_store, #message_properties{size = Size}, false},
                 {V2ReadsAcc, ShPReadsAcc, ShTReadsAcc}) when Size =< ?SHARED_READ_MANY_SIZE_THRESHOLD ->
                    {V2ReadsAcc, ShPReadsAcc, [MsgId|ShTReadsAcc]};
                (_, Acc) ->
                    Acc
            end, {[], [], []}, List0),
            %% In order to properly read and merge V2 messages we want them
            %% in the older->younger order.
            V2Reads = lists:reverse(V2Reads0),
            %% We do read_many for v2 store unconditionally.
            {V2Msgs, StoreState2} = rabbit_classic_queue_store_v2:read_many(V2Reads, StoreState),
            List1 = merge_read_msgs(List0, V2Reads, V2Msgs),
            %% We read from the shared message store only if there are multiple messages
            %% (10+ as we wouldn't get much benefits from smaller number of messages)
            %% otherwise we wait and read later.
            %%
            %% Because read_many does not use FHC we do not get an updated MCState
            %% like with normal reads.
            {List2, MCStateP2} = case length(ShPersistReads) < ?SHARED_READ_MANY_COUNT_THRESHOLD of
                true ->
                    {List1, MCStateP};
                false ->
                    {ShPersistMsgs, MCStateP1} = rabbit_msg_store:read_many(ShPersistReads, MCStateP),
                    case map_size(ShPersistMsgs) of
                        0 -> {List1, MCStateP1};
                        _ -> {merge_sh_read_msgs(List1, ShPersistMsgs), MCStateP1}
                    end
            end,
            {List3, MCStateT2} = case length(ShTransientReads) < ?SHARED_READ_MANY_COUNT_THRESHOLD of
                true ->
                    {List2, MCStateT};
                false ->
                    {ShTransientMsgs, MCStateT1} = rabbit_msg_store:read_many(ShTransientReads, MCStateT),
                    case map_size(ShTransientMsgs) of
                        0 -> {List2, MCStateT1};
                        _ -> {merge_sh_read_msgs(List2, ShTransientMsgs), MCStateT1}
                    end
            end,
            {List3, StoreState2, MCStateP2, MCStateT2};
        metadata_only ->
            {List0, StoreState, MCStateP, MCStateT}
    end,
    {Q3a, RamCountsInc, RamBytesInc, State1, TransientCount, TransientBytes} =
        betas_from_index_entries(List, TransientThreshold,
                                 DelsAndAcksFun,
                                 State #vqstate { index_state = IndexState1,
                                                  store_state = StoreState3,
                                                  msg_store_clients = {MCStateP3, MCStateT3}}),
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
                delta = d(Delta #delta { start_seq_id = DeltaSeqId1 })},
              MemoryLimit, WhatToRead);
        Q3aLen ->
            Q3b = ?QUEUE:join(Q3, Q3a),
            case DeltaCount - Q3aLen of
                0 ->
                    %% delta is now empty
                    State2 #vqstate { delta = ?BLANK_DELTA,
                                      q3    = Q3b,
                                      delta_transient_bytes = 0};
                N when N > 0 ->
                    Delta1 = d(#delta { start_seq_id = DeltaSeqId1,
                                        count        = N,
                                        %% @todo Probably something wrong, seen it become negative...
                                        transient    = Transient - TransientCount,
                                        end_seq_id   = DeltaSeqIdEnd }),
                    State2 #vqstate { delta = Delta1,
                                      q3    = Q3b,
                                      delta_transient_bytes = DeltaTransientBytes - TransientBytes }
            end
    end.

merge_read_msgs([M = {_, SeqId, _, _, _}|MTail], [{SeqId, _}|RTail], [Msg|MsgTail]) ->
    [setelement(1, M, Msg)|merge_read_msgs(MTail, RTail, MsgTail)];
merge_read_msgs([M|MTail], RTail, MsgTail) ->
    [M|merge_read_msgs(MTail, RTail, MsgTail)];
%% @todo We probably don't need to unwrap until the end.
merge_read_msgs([], [], []) ->
    [].

%% We may not get as many messages as we tried reading.
merge_sh_read_msgs([M = {MsgId, _, _, _, _}|MTail], Reads) ->
    case Reads of
        #{MsgId := Msg} ->
            [setelement(1, M, Msg)|merge_sh_read_msgs(MTail, Reads)];
        _ ->
            [M|merge_sh_read_msgs(MTail, Reads)]
    end;
merge_sh_read_msgs(MTail, _Reads) ->
    MTail.

%% Flushes queue index batch caches and updates queue index state.
ui(#vqstate{index_state      = IndexState,
            target_ram_count = TargetRamCount} = State) ->
    IndexState1 = rabbit_classic_queue_index_v2:flush_pre_publish_cache(
                    TargetRamCount, IndexState),
    State#vqstate{index_state = IndexState1}.

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

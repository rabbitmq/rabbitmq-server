%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_variable_queue).

-export([init/2, terminate/1, publish/2, publish_delivered/3,
         set_ram_duration_target/2, ram_duration/1, fetch/2, ack/2, len/1,
         is_empty/1, purge/1, delete_and_terminate/1, requeue/2, tx_publish/3,
         tx_ack/3, tx_rollback/2, tx_commit/3, sync_callback/1,
         handle_pre_hibernate/1, status/1]).

-export([start/1]).

%%----------------------------------------------------------------------------
%% Definitions:

%% alpha: this is a message where both the message itself, and its
%%        position within the queue are held in RAM
%%
%% beta: this is a message where the message itself is only held on
%%        disk, but its position within the queue is held in RAM.
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

%% Also note that within this code, the term gamma never
%% appears. Instead, gammas are defined by betas who have had their
%% queue position recorded on disk.

%% In general, messages move q1 -> q2 -> delta -> q3 -> q4, though
%% many of these steps are frequently skipped. q1 and q4 only hold
%% alphas, q2 and q3 hold both betas and gammas (as queues of queues,
%% using the bpqueue module where the block prefix determines whether
%% they're betas or gammas). When a message arrives, its
%% classification is determined. It is then added to the rightmost
%% appropriate queue.

%% If a new message is determined to be a beta or gamma, q1 is
%% empty. If a new message is determined to be a delta, q1 and q2 are
%% empty (and actually q4 too).

%% When removing messages from a queue, if q4 is empty then q3 is read
%% directly. If q3 becomes empty then the next segment's worth of
%% messages from delta are read into q3, reducing the size of
%% delta. If the queue is non empty, either q4 or q3 contain
%% entries. It is never permitted for delta to hold all the messages
%% in the queue.

%% The duration indicated to us by the memory_monitor is used to
%% calculate, given our current ingress and egress rates, how many
%% messages we should hold in RAM. When we need to push alphas to
%% betas or betas to gammas, we favour writing out messages that are
%% further from the head of the queue. This minimises writes to disk,
%% as the messages closer to the tail of the queue stay in the queue
%% for longer, thus do not need to be replaced as quickly by sending
%% other messages to disk.

%% Whilst messages are pushed to disk and forgotten from RAM as soon
%% as requested by a new setting of the queue RAM duration, the
%% inverse is not true: we only load messages back into RAM as
%% demanded as the queue is read from. Thus only publishes to the
%% queue will take up available spare capacity.

%% If a queue is full of transient messages, then the transition from
%% betas to deltas will be potentially very expensive as millions of
%% entries must be written to disk by the queue_index module. This can
%% badly stall the queue. In order to avoid this, the proportion of
%% gammas / (betas+gammas) must not be lower than (betas+gammas) /
%% (alphas+betas+gammas). Thus as the queue grows, and the proportion
%% of alphas shrink, the proportion of gammas will grow, thus at the
%% point at which betas and gammas must be converted to deltas, there
%% should be very few betas remaining, thus the transition is fast (no
%% work needs to be done for the gamma -> delta transition).

%% The conversion of betas to gammas is done on publish, in batches of
%% exactly ?RAM_INDEX_BATCH_SIZE. This value should not be too small,
%% otherwise the frequent operations on the queues of q2 and q3 will
%% not be effectively amortised, nor should it be too big, otherwise a
%% publish will take too long as it attempts to do too much work and
%% thus stalls the queue. Therefore, it must be just right. This
%% approach is preferable to doing work on a new queue-duration
%% because converting all the indicated betas to gammas at that point
%% can be far too expensive, thus requiring batching and segmented
%% work anyway, and furthermore, if we're not getting any publishes
%% anyway then the queue is either being drained or has no
%% consumers. In the latter case, an expensive beta to delta
%% transition doesn't matter, and in the former case the queue's
%% shrinking length makes it unlikely (though not impossible) that the
%% duration will become 0.

%% In the queue we only keep track of messages that are pending
%% delivery. This is fine for queue purging, but can be expensive for
%% queue deletion: for queue deletion we must scan all the way through
%% all remaining segments in the queue index (we start by doing a
%% purge) and delete messages from the msg_store that we find in the
%% queue index.

%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(vqstate,
        { q1,
          q2,
          delta,
          q3,
          q4,
          duration_target,
          target_ram_msg_count,
          ram_msg_count,
          ram_msg_count_prev,
          ram_index_count,
          index_state,
          next_seq_id,
          out_counter,
          in_counter,
          egress_rate,
          avg_egress_rate,
          ingress_rate,
          avg_ingress_rate,
          rate_timestamp,
          len,
          on_sync,
          msg_store_clients,
          persistent_store,
          persistent_count,
          transient_threshold,
          pending_ack
        }).

-record(msg_status,
        { seq_id,
          guid,
          msg,
          is_persistent,
          is_delivered,
          msg_on_disk,
          index_on_disk
        }).

-record(delta,
        { start_seq_id,
          count,
          end_seq_id %% note the end_seq_id is always >, not >=
        }).

-record(tx, { pending_messages, pending_acks }).

%% When we discover, on publish, that we should write some indices to
%% disk for some betas, the RAM_INDEX_BATCH_SIZE sets the number of
%% betas that we must be due to write indices for before we do any
%% work at all. This is both a minimum and a maximum - we don't write
%% fewer than RAM_INDEX_BATCH_SIZE indices out in one go, and we don't
%% write more - we can always come back on the next publish to do
%% more.
-define(RAM_INDEX_BATCH_SIZE, 64).
-define(PERSISTENT_MSG_STORE,     msg_store_persistent).
-define(TRANSIENT_MSG_STORE,      msg_store_transient).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(bpqueue() :: any()).
-type(seq_id()  :: non_neg_integer()).
-type(ack()     :: seq_id() | 'blank_ack').

-type(delta() :: #delta { start_seq_id :: non_neg_integer(),
                          count :: non_neg_integer (),
                          end_seq_id :: non_neg_integer() }).

-type(state() :: #vqstate {
               q1                    :: queue(),
               q2                    :: bpqueue(),
               delta                 :: delta(),
               q3                    :: bpqueue(),
               q4                    :: queue(),
               duration_target       :: non_neg_integer(),
               target_ram_msg_count  :: non_neg_integer(),
               ram_msg_count         :: non_neg_integer(),
               ram_msg_count_prev    :: non_neg_integer(),
               ram_index_count       :: non_neg_integer(),
               index_state           :: any(),
               next_seq_id           :: seq_id(),
               out_counter           :: non_neg_integer(),
               in_counter            :: non_neg_integer(),
               egress_rate           :: {{integer(), integer(), integer()}, non_neg_integer()},
               avg_egress_rate       :: float(),
               ingress_rate          :: {{integer(), integer(), integer()}, non_neg_integer()},
               avg_ingress_rate      :: float(),
               rate_timestamp        :: {integer(), integer(), integer()},
               len                   :: non_neg_integer(),
               on_sync               :: {[[ack()]], [[guid()]], [fun (() -> any())]},
               msg_store_clients     :: 'undefined' | {{any(), binary()}, {any(), binary()}},
               persistent_store      :: pid() | atom(),
               persistent_count      :: non_neg_integer(),
               transient_threshold   :: non_neg_integer(),
               pending_ack           :: dict()
              }).

-spec(tx_commit_post_msg_store/5 ::
        (boolean(), [guid()], [ack()], {pid(), any()}, state()) -> state()).
-spec(tx_commit_index/1 :: (state()) -> state()).

-include("rabbit_backing_queue_spec.hrl").

-endif.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count = 0,
                              end_seq_id = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
                                         count = 0,
                                         end_seq_id = Z }).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(DurableQueues) ->
    ok = rabbit_msg_store:clean(?TRANSIENT_MSG_STORE, rabbit_mnesia:dir()),
    {{TransRefs, TransStartFunState}, {PersistRefs, PersistStartFunState}}
        = rabbit_queue_index:prepare_msg_store_seed_funs(DurableQueues),
    ok = rabbit_sup:start_child(?TRANSIENT_MSG_STORE, rabbit_msg_store,
                                [?TRANSIENT_MSG_STORE, rabbit_mnesia:dir(),
                                 TransRefs, TransStartFunState]),
    ok = rabbit_sup:start_child(?PERSISTENT_MSG_STORE, rabbit_msg_store,
                                [?PERSISTENT_MSG_STORE, rabbit_mnesia:dir(),
                                 PersistRefs, PersistStartFunState]).

init(QueueName, IsDurable) ->
    PersistentStore = case IsDurable of
                          true  -> ?PERSISTENT_MSG_STORE;
                          false -> ?TRANSIENT_MSG_STORE
                      end,
    MsgStoreRecovered =
        rabbit_msg_store:successfully_recovered_state(PersistentStore),
    ContainsCheckFun =
        fun (Guid) ->
                rabbit_msg_store:contains(?PERSISTENT_MSG_STORE, Guid)
        end,
    {DeltaCount, PRef, TRef, Terms, IndexState} =
        rabbit_queue_index:init(QueueName, MsgStoreRecovered, ContainsCheckFun),
    {DeltaSeqId, NextSeqId, IndexState1} =
        rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(IndexState),

    DeltaCount1 = proplists:get_value(persistent_count, Terms, DeltaCount),
    Delta = case DeltaCount1 == 0 andalso DeltaCount /= undefined of
                true  -> ?BLANK_DELTA;
                false -> #delta { start_seq_id = DeltaSeqId,
                                  count = DeltaCount1,
                                  end_seq_id = NextSeqId }
            end,
    Now = now(),
    State =
        #vqstate { q1 = queue:new(), q2 = bpqueue:new(),
                   delta = Delta,
                   q3 = bpqueue:new(), q4 = queue:new(),
                   duration_target = undefined,
                   target_ram_msg_count = undefined,
                   ram_msg_count = 0,
                   ram_msg_count_prev = 0,
                   ram_index_count = 0,
                   index_state = IndexState1,
                   next_seq_id = NextSeqId,
                   out_counter = 0,
                   in_counter = 0,
                   egress_rate = {Now, 0},
                   avg_egress_rate = 0,
                   ingress_rate = {Now, DeltaCount1},
                   avg_ingress_rate = 0,
                   rate_timestamp = Now,
                   len = DeltaCount1,
                   on_sync = {[], [], []},
                   msg_store_clients = {
                     {rabbit_msg_store:client_init(PersistentStore, PRef), PRef},
                     {rabbit_msg_store:client_init(?TRANSIENT_MSG_STORE, TRef), TRef}},
                   persistent_store = PersistentStore,
                   persistent_count = DeltaCount1,
                   transient_threshold = NextSeqId,
                   pending_ack = dict:new()
                 },
    maybe_deltas_to_betas(State).

terminate(State) ->
    State1 = #vqstate {
      persistent_count = PCount, index_state = IndexState,
      msg_store_clients = {{MSCStateP, PRef}, {MSCStateT, TRef}} } =
        remove_pending_ack(true, tx_commit_index(State)),
    rabbit_msg_store:client_terminate(MSCStateP),
    rabbit_msg_store:client_terminate(MSCStateT),
    Terms = [{persistent_ref, PRef}, {transient_ref, TRef},
             {persistent_count, PCount}],
    State1 #vqstate { index_state =
                          rabbit_queue_index:terminate(Terms, IndexState),
                      msg_store_clients = undefined }.

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete_and_terminate(State) ->
    {_PurgeCount, State1} = purge(State),
    State2 = #vqstate {
      index_state = IndexState,
      msg_store_clients = {{MSCStateP, PRef}, {MSCStateT, TRef}},
      persistent_store = PersistentStore,
      transient_threshold = TransientThreshold } =
        remove_pending_ack(false, State1),
    %% flushing here is good because it deletes all full segments,
    %% leaving only partial segments around.
    IndexState1 = rabbit_queue_index:flush_journal(IndexState),
    IndexState2 =
        case rabbit_queue_index:find_lowest_seq_id_seg_and_next_seq_id(
               IndexState1) of
            {N, N, IndexState3} ->
                IndexState3;
            {DeltaSeqId, NextSeqId, IndexState3} ->
                {_DeleteCount, IndexState4} =
                    delete1(PersistentStore, TransientThreshold, NextSeqId, 0,
                            DeltaSeqId, IndexState3),
                IndexState4
    end,
    IndexState5 = rabbit_queue_index:terminate_and_erase(IndexState2),
    rabbit_msg_store:delete_client(PersistentStore, PRef),
    rabbit_msg_store:delete_client(?TRANSIENT_MSG_STORE, TRef),
    rabbit_msg_store:client_terminate(MSCStateP),
    rabbit_msg_store:client_terminate(MSCStateT),
    State2 #vqstate { index_state = IndexState5,
                      msg_store_clients = undefined }.

purge(State = #vqstate { q4 = Q4, index_state = IndexState, len = Len,
                         persistent_store = PersistentStore }) ->
    {Q4Count, IndexState1} =
        remove_queue_entries(PersistentStore, fun rabbit_misc:queue_fold/3,
                             Q4, IndexState),
    {Len, State1} =
        purge1(Q4Count, State #vqstate { index_state = IndexState1,
                                         q4 = queue:new() }),
    {Len, State1 #vqstate { len = 0, ram_msg_count = 0, ram_index_count = 0,
                            persistent_count = 0 }}.

publish(Msg, State) ->
    State1 = limit_ram_index(State),
    {_SeqId, State2} = publish(Msg, false, false, State1),
    State2.

publish_delivered(false, _Msg, State = #vqstate { len = 0 }) ->
    {blank_ack, State};
publish_delivered(true, Msg = #basic_message { guid = Guid,
                                               is_persistent = IsPersistent },
                  State = #vqstate { len = 0, index_state = IndexState,
                                     next_seq_id = SeqId,
                                     out_counter = OutCount,
                                     in_counter = InCount,
                                     msg_store_clients = MSCState,
                                     persistent_store = PersistentStore,
                                     persistent_count = PCount,
                                     pending_ack = PA }) ->
    MsgStatus = #msg_status {
      msg = Msg, guid = Guid, seq_id = SeqId, is_persistent = IsPersistent,
      is_delivered = true, msg_on_disk = false, index_on_disk = false },
    {MsgStatus1, MSCState1} = maybe_write_msg_to_disk(PersistentStore, false,
                                                      MsgStatus, MSCState),
    State1 = State #vqstate { msg_store_clients = MSCState1,
                              persistent_count = PCount + case IsPersistent of
                                                              true  -> 1;
                                                              false -> 0
                                                          end,
                              next_seq_id = SeqId + 1,
                              out_counter = OutCount + 1,
                              in_counter = InCount + 1 },
    {SeqId,
     case MsgStatus1 #msg_status.msg_on_disk of
         true ->
             {#msg_status { index_on_disk = true }, IndexState1} =
                 maybe_write_index_to_disk(false, MsgStatus1, IndexState),
             State1 #vqstate { index_state = IndexState1,
                               pending_ack = dict:store(SeqId, {true, Guid},
                                                        PA) };
         false ->
             State1 #vqstate { pending_ack = dict:store(SeqId, MsgStatus1, PA) }
     end}.

fetch(AckRequired, State =
      #vqstate { q4 = Q4, ram_msg_count = RamMsgCount, out_counter = OutCount,
                 index_state = IndexState, len = Len, persistent_count = PCount,
                 persistent_store = PersistentStore, pending_ack = PA }) ->
    case queue:out(Q4) of
        {empty, _Q4} ->
            case fetch_from_q3_or_delta(State) of
                {empty, _State1} = Result -> Result;
                {loaded, State1}          -> fetch(AckRequired, State1)
            end;
        {{value, MsgStatus = #msg_status {
            msg = Msg, guid = Guid, seq_id = SeqId,
            is_persistent = IsPersistent, is_delivered = IsDelivered,
            msg_on_disk = MsgOnDisk, index_on_disk = IndexOnDisk }},
         Q4a} ->

            AckTag = case AckRequired of
                         true  -> SeqId;
                         false -> blank_ack
                     end,

            %% 1. Mark it delivered if necessary
            IndexState1 = case IndexOnDisk andalso not IsDelivered of
                              true  -> rabbit_queue_index:write_delivered(
                                         SeqId, IndexState);
                              false -> IndexState
                          end,

            %% 2. If it's on disk and there's no Ack required, remove it
            MsgStore = find_msg_store(IsPersistent, PersistentStore),
            IndexState2 =
                case MsgOnDisk andalso not AckRequired of
                    true -> %% Remove from disk now
                        ok = case MsgOnDisk of
                                 true ->
                                     rabbit_msg_store:remove(MsgStore, [Guid]);
                                 false ->
                                     ok
                             end,
                        case IndexOnDisk of
                            true ->
                                rabbit_queue_index:write_acks([SeqId],
                                                              IndexState1);
                            false ->
                                IndexState1
                        end;
                    false ->
                        IndexState1
                end,

            %% 3. If it's on disk, not persistent and an ack's
            %% required then remove it from the queue index only.
            IndexState3 =
                case IndexOnDisk andalso AckRequired andalso not IsPersistent of
                    true -> rabbit_queue_index:write_acks([SeqId], IndexState2);
                    false -> IndexState2
                end,

            %% 4. If an ack is required, add something sensible to PA
            PA1 = case AckRequired of
                      true  ->
                          Entry =
                              case MsgOnDisk of
                                  true  -> {IsPersistent, Guid};
                                  false -> MsgStatus #msg_status {
                                             is_delivered = true }
                              end,
                          dict:store(SeqId, Entry, PA);
                      false -> PA
                  end,

            PCount1 = case IsPersistent andalso not AckRequired of
                          true  -> PCount - 1;
                          false -> PCount
                      end,
            Len1 = Len - 1,
            {{Msg, IsDelivered, AckTag, Len1},
             State #vqstate { q4 = Q4a, out_counter = OutCount + 1,
                              ram_msg_count = RamMsgCount - 1,
                              index_state = IndexState3, len = Len1,
                              pending_ack = PA1, persistent_count = PCount1 }}
    end.

ack([], State) ->
    State;
ack(AckTags, State = #vqstate { index_state = IndexState,
                                persistent_count = PCount,
                                persistent_store = PersistentStore,
                                pending_ack = PA }) ->
    {GuidsByStore, SeqIds, PA1} =
        lists:foldl(
          fun (SeqId, {Dict, SeqIds, PAN}) ->
                  PAN1 = dict:erase(SeqId, PAN),
                  case dict:find(SeqId, PAN) of
                      {ok, #msg_status { index_on_disk = false, %% ASSERTIONS
                                         msg_on_disk = false,
                                         is_persistent = false }} ->
                          {Dict, SeqIds, PAN1};
                      {ok, {false, Guid}} ->
                          {rabbit_misc:dict_cons(?TRANSIENT_MSG_STORE, Guid,
                                                 Dict), SeqIds, PAN1};
                      {ok, {true, Guid}} ->
                          {rabbit_misc:dict_cons(PersistentStore, Guid, Dict),
                           [SeqId | SeqIds], PAN1}
                  end
          end, {dict:new(), [], PA}, AckTags),
    IndexState1 = rabbit_queue_index:write_acks(SeqIds, IndexState),
    ok = dict:fold(fun (MsgStore, Guids, ok) ->
                           rabbit_msg_store:remove(MsgStore, Guids)
                   end, ok, GuidsByStore),
    PCount1 = PCount - case dict:find(PersistentStore, GuidsByStore) of
                           error        -> 0;
                           {ok, Guids} -> length(Guids)
                       end,
    State #vqstate { index_state = IndexState1, persistent_count = PCount1,
                     pending_ack = PA1 }.

tx_publish(Txn,
           Msg = #basic_message { is_persistent = true, guid = Guid },
           State = #vqstate { msg_store_clients = MSCState,
                              persistent_store = PersistentStore }) ->
    MsgStatus = #msg_status {
      msg = Msg, guid = Guid, seq_id = undefined, is_persistent = true,
      is_delivered = false, msg_on_disk = false, index_on_disk = false },
    {#msg_status { msg_on_disk = true }, MSCState1} =
        maybe_write_msg_to_disk(PersistentStore, false, MsgStatus, MSCState),
    publish_in_tx(Txn, Msg),
    State #vqstate { msg_store_clients = MSCState1 };
tx_publish(Txn, Msg, State) ->
    publish_in_tx(Txn, Msg),
    State.

tx_ack(Txn, AckTags, State) ->
    ack_in_tx(Txn, AckTags),
    State.

tx_rollback(Txn, State = #vqstate { persistent_store = PersistentStore }) ->
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    ok = rabbit_msg_store:remove(PersistentStore, persistent_guids(Pubs)),
    {lists:flatten(AckTags), State}.

tx_commit(Txn, Fun, State = #vqstate { persistent_store = PersistentStore }) ->
    %% If we are a non-durable queue, or we have no persistent pubs,
    %% we can skip the msg_store loop.
    #tx { pending_acks = AckTags, pending_messages = Pubs } = lookup_tx(Txn),
    erase_tx(Txn),
    PubsOrdered = lists:reverse(Pubs),
    AckTags1 = lists:flatten(AckTags),
    PersistentGuids = persistent_guids(PubsOrdered),
    IsTransientPubs = [] == PersistentGuids,
    {AckTags1,
     case IsTransientPubs orelse
         ?TRANSIENT_MSG_STORE == PersistentStore of
         true ->
             tx_commit_post_msg_store(
               IsTransientPubs, PubsOrdered, AckTags1, Fun, State);
         false ->
             ok = rabbit_msg_store:sync(
                    ?PERSISTENT_MSG_STORE, PersistentGuids,
                    msg_store_callback(PersistentGuids, IsTransientPubs,
                                       PubsOrdered, AckTags1, Fun)),
             State
     end}.

requeue(AckTags, State = #vqstate { persistent_store = PersistentStore }) ->
    {SeqIds, GuidsByStore,
     State1 = #vqstate { index_state = IndexState,
                         persistent_count = PCount }} =
        lists:foldl(
          fun (SeqId, {SeqIdsAcc, Dict, StateN =
                           #vqstate { msg_store_clients = MSCStateN,
                                      pending_ack = PAN}}) ->
                  PAN1 = dict:erase(SeqId, PAN),
                  StateN1 = StateN #vqstate { pending_ack = PAN1 },
                  case dict:find(SeqId, PAN) of
                      {ok, #msg_status { index_on_disk = false,
                                         msg_on_disk = false,
                                         is_persistent = false,
                                         msg = Msg }} ->
                          {_SeqId, StateN2} = publish(Msg, true, false, StateN1),
                          {SeqIdsAcc, Dict, StateN2};
                      {ok, {IsPersistent, Guid}} ->
                          {{ok, Msg = #basic_message{}}, MSCStateN1} =
                              read_from_msg_store(
                                PersistentStore, MSCStateN, IsPersistent, Guid),
                          StateN2 = StateN1 #vqstate {
                                      msg_store_clients = MSCStateN1 },
                          {_SeqId, StateN3} = publish(Msg, true, true, StateN2),
                          {SeqIdsAcc1, MsgStore} =
                              case IsPersistent of
                                  true ->
                                      {[SeqId | SeqIdsAcc], PersistentStore};
                                  false ->
                                      {SeqIdsAcc, ?TRANSIENT_MSG_STORE}
                              end,
                           {SeqIdsAcc1,
                            rabbit_misc:dict_cons(MsgStore, Guid, Dict),
                            StateN3}
                  end
          end, {[], dict:new(), State}, AckTags),
    IndexState1 = rabbit_queue_index:write_acks(SeqIds, IndexState),
    ok = dict:fold(fun (MsgStore, Guids, ok) ->
                           rabbit_msg_store:release(MsgStore, Guids)
                   end, ok, GuidsByStore),
    PCount1 = PCount - case dict:find(PersistentStore, GuidsByStore) of
                           error        -> 0;
                           {ok, Guids} -> length(Guids)
                       end,
    State1 #vqstate { index_state = IndexState1,
                      persistent_count = PCount1 }.

len(#vqstate { len = Len }) ->
    Len.

is_empty(State) ->
    0 == len(State).

set_ram_duration_target(
  DurationTarget, State = #vqstate { avg_egress_rate = AvgEgressRate,
                                     avg_ingress_rate = AvgIngressRate,
                                     target_ram_msg_count = TargetRamMsgCount
                                   }) ->
    Rate = AvgEgressRate + AvgIngressRate,
    TargetRamMsgCount1 =
        case DurationTarget of
            infinity -> undefined;
            undefined -> undefined;
            _ -> trunc(DurationTarget * Rate) %% msgs = sec * msgs/sec
        end,
    State1 = State #vqstate { target_ram_msg_count = TargetRamMsgCount1,
                              duration_target = DurationTarget },
    case TargetRamMsgCount1 == undefined orelse
        TargetRamMsgCount1 >= TargetRamMsgCount of
        true  -> State1;
        false -> reduce_memory_use(State1)
    end.

ram_duration(State = #vqstate { egress_rate = Egress,
                                ingress_rate = Ingress,
                                rate_timestamp = Timestamp,
                                in_counter = InCount,
                                out_counter = OutCount,
                                ram_msg_count = RamMsgCount,
                                duration_target = DurationTarget,
                                ram_msg_count_prev = RamMsgCountPrev }) ->
    Now = now(),
    {AvgEgressRate, Egress1} = update_rate(Now, Timestamp, OutCount, Egress),
    {AvgIngressRate, Ingress1} = update_rate(Now, Timestamp, InCount, Ingress),

    Duration = %% msgs / (msgs/sec) == sec
        case AvgEgressRate == 0 andalso AvgIngressRate == 0 of
            true  -> infinity;
            false -> (RamMsgCountPrev + RamMsgCount) /
                         (2 * (AvgEgressRate + AvgIngressRate))
        end,

    {Duration, set_ram_duration_target(
                 DurationTarget,
                 State #vqstate { egress_rate = Egress1,
                                  avg_egress_rate = AvgEgressRate,
                                  ingress_rate = Ingress1,
                                  avg_ingress_rate = AvgIngressRate,
                                  rate_timestamp = Now,
                                  ram_msg_count_prev = RamMsgCount,
                                  out_counter = 0, in_counter = 0 })}.

sync_callback(#vqstate { on_sync = {_, _, []} }) -> undefined;
sync_callback(_)                                 -> fun tx_commit_index/1.

handle_pre_hibernate(State = #vqstate { index_state = IndexState }) ->
    State #vqstate { index_state =
                     rabbit_queue_index:flush_journal(IndexState) }.

status(#vqstate { q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
                  len = Len, on_sync = {_, _, From},
                  target_ram_msg_count = TargetRamMsgCount,
                  ram_msg_count = RamMsgCount,
                  ram_index_count = RamIndexCount,
                  avg_egress_rate = AvgEgressRate,
                  avg_ingress_rate = AvgIngressRate,
                  next_seq_id = NextSeqId }) ->
    [ {q1, queue:len(Q1)},
      {q2, bpqueue:len(Q2)},
      {delta, Delta},
      {q3, bpqueue:len(Q3)},
      {q4, queue:len(Q4)},
      {len, Len},
      {outstanding_txns, length(From)},
      {target_ram_msg_count, TargetRamMsgCount},
      {ram_msg_count, RamMsgCount},
      {ram_index_count, RamIndexCount},
      {avg_egress_rate, AvgEgressRate},
      {avg_ingress_rate, AvgIngressRate},
      {next_seq_id, NextSeqId} ].

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

remove_pending_ack(KeepPersistent,
                   State = #vqstate { pending_ack = PA,
                                      persistent_store = PersistentStore,
                                      index_state = IndexState }) ->
    {SeqIds, GuidsByStore, PA1} =
        dict:fold(
          fun (SeqId, {IsPersistent, Guid}, {SeqIdsAcc, Dict, PAN}) ->
                  PAN1 = case KeepPersistent andalso IsPersistent of
                             true  -> PAN;
                             false -> dict:erase(SeqId, PAN)
                         end,
                  case IsPersistent of
                      true  -> {[SeqId | SeqIdsAcc],
                                rabbit_misc:dict_cons(
                                  PersistentStore, Guid, Dict), PAN1};
                      false -> {SeqIdsAcc,
                                rabbit_misc:dict_cons(
                                  ?TRANSIENT_MSG_STORE, Guid, Dict), PAN1}
                  end;
              (SeqId, #msg_status {}, {SeqIdsAcc, Dict, PAN}) ->
                  {SeqIdsAcc, Dict, dict:erase(SeqId, PAN)}
          end, {[], dict:new(), PA}, PA),
    case KeepPersistent of
        true ->
            State1 = State #vqstate { pending_ack = PA1 },
            case dict:find(?TRANSIENT_MSG_STORE, GuidsByStore) of
                error       -> State1;
                {ok, Guids} -> ok = rabbit_msg_store:remove(
                                      ?TRANSIENT_MSG_STORE, Guids),
                               State1
            end;
        false ->
            IndexState1 = rabbit_queue_index:write_acks(SeqIds, IndexState),
            ok = dict:fold(fun (MsgStore, Guids, ok) ->
                                   rabbit_msg_store:remove(MsgStore, Guids)
                           end, ok, GuidsByStore),
            State #vqstate { pending_ack = dict:new(),
                             index_state = IndexState1 }
    end.

lookup_tx(Txn) ->
    case get({txn, Txn}) of
        undefined -> #tx { pending_messages = [],
                           pending_acks     = [] };
        V         -> V
    end.

store_tx(Txn, Tx) ->
    put({txn, Txn}, Tx).

erase_tx(Txn) ->
    erase({txn, Txn}).

publish_in_tx(Txn, Msg) ->
    Tx = #tx { pending_messages = Pubs } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_messages = [Msg | Pubs] }).

ack_in_tx(Txn, AckTags) ->
    Tx = #tx { pending_acks = Acks } = lookup_tx(Txn),
    store_tx(Txn, Tx #tx { pending_acks = [AckTags | Acks] }).

update_rate(Now, Then, Count, {OThen, OCount}) ->
    %% form the avg over the current period and the previous
    Avg = 1000000 * ((Count + OCount) / timer:now_diff(Now, OThen)),
    {Avg, {Then, Count}}.

persistent_guids(Pubs) ->
    [Guid || Obj = #basic_message { guid = Guid } <- Pubs,
             Obj #basic_message.is_persistent].

betas_from_segment_entries(List, SeqIdLimit, TransientThreshold, IndexState) ->
    {Filtered, IndexState1} =
        lists:foldr(
          fun ({Guid, SeqId, IsPersistent, IsDelivered},
               {FilteredAcc, IndexStateAcc}) ->
                  case SeqId < TransientThreshold andalso not IsPersistent of
                      true ->
                          IndexStateAcc1 =
                              case IsDelivered of
                                  false -> rabbit_queue_index:write_delivered(
                                             SeqId, IndexStateAcc);
                                  true  -> IndexStateAcc
                              end,
                          {FilteredAcc, rabbit_queue_index:write_acks(
                                          [SeqId], IndexStateAcc1)};
                      false ->
                          case SeqId < SeqIdLimit of
                              true ->
                                  {[#msg_status { msg           = undefined,
                                                  guid          = Guid,
                                                  seq_id        = SeqId,
                                                  is_persistent = IsPersistent,
                                                  is_delivered  = IsDelivered,
                                                  msg_on_disk   = true,
                                                  index_on_disk = true
                                                } | FilteredAcc],
                                   IndexStateAcc};
                              false ->
                                  {FilteredAcc, IndexStateAcc}
                          end
                  end
          end, {[], IndexState}, List),
    {bpqueue:from_list([{true, Filtered}]), IndexState1}.

read_index_segment(SeqId, IndexState) ->
    SeqId1 = SeqId + rabbit_queue_index:segment_size(),
    case rabbit_queue_index:read_segment_entries(SeqId, IndexState) of
        {[], IndexState1} -> read_index_segment(SeqId1, IndexState1);
        {List, IndexState1} -> {List, IndexState1, SeqId1}
    end.

ensure_binary_properties(Msg = #basic_message { content = Content }) ->
    Msg #basic_message {
      content = rabbit_binary_parser:clear_decoded_content(
                  rabbit_binary_generator:ensure_content_encoded(Content)) }.

%% the first arg is the older delta
combine_deltas(?BLANK_DELTA_PATTERN(X), ?BLANK_DELTA_PATTERN(Y)) ->
    ?BLANK_DELTA;
combine_deltas(?BLANK_DELTA_PATTERN(X),
               #delta { start_seq_id = Start, count = Count,
                        end_seq_id = End } = B) ->
    true = Start + Count =< End, %% ASSERTION
    B;
combine_deltas(#delta { start_seq_id = Start, count = Count,
                        end_seq_id = End } = A, ?BLANK_DELTA_PATTERN(Y)) ->
    true = Start + Count =< End, %% ASSERTION
    A;
combine_deltas(#delta { start_seq_id = StartLow,  count = CountLow,
                        end_seq_id = EndLow },
               #delta { start_seq_id = StartHigh, count = CountHigh,
                        end_seq_id = EndHigh }) ->
    Count = CountLow + CountHigh,
    true = (StartLow =< StartHigh) %% ASSERTIONS
        andalso ((StartLow + CountLow) =< EndLow)
        andalso ((StartHigh + CountHigh) =< EndHigh)
        andalso ((StartLow + Count) =< EndHigh),
    #delta { start_seq_id = StartLow, count = Count, end_seq_id = EndHigh }.

beta_fold_no_index_on_disk(Fun, Init, Q) ->
    bpqueue:foldr(fun (_Prefix, Value, Acc) ->
                          Fun(Value, Acc)
                  end, Init, Q).

permitted_ram_index_count(#vqstate { len = 0 }) ->
    undefined;
permitted_ram_index_count(#vqstate { len = Len, q2 = Q2, q3 = Q3,
                                     delta = #delta { count = DeltaCount } }) ->
    AlphaBetaLen = Len - DeltaCount,
    case AlphaBetaLen == 0 of
        true ->
            undefined;
        false ->
            BetaLen = bpqueue:len(Q2) + bpqueue:len(Q3),
            %% the fraction of the alphas+betas that are betas
            BetaFrac =  BetaLen / AlphaBetaLen,
            BetaLen - trunc(BetaFrac * BetaLen)
    end.


should_force_index_to_disk(State =
                           #vqstate { ram_index_count = RamIndexCount }) ->
    case permitted_ram_index_count(State) of
        undefined -> false;
        Permitted -> RamIndexCount >= Permitted
    end.

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

msg_store_callback(PersistentGuids, IsTransientPubs, Pubs, AckTags, Fun) ->
    Self = self(),
    fun() ->
            spawn(
              fun() ->
                      ok = rabbit_misc:with_exit_handler(
                             fun() -> rabbit_msg_store:remove(
                                        ?PERSISTENT_MSG_STORE,
                                        PersistentGuids)
                             end,
                             fun() -> rabbit_amqqueue:maybe_run_queue_via_backing_queue(
                                        Self, fun (StateN) ->
                                                      tx_commit_post_msg_store(
                                                        IsTransientPubs, Pubs,
                                                        AckTags, Fun, StateN)
                                              end)
                             end)
              end)
    end.

tx_commit_post_msg_store(IsTransientPubs, Pubs, AckTags, Fun, State =
                             #vqstate { on_sync = OnSync = {SAcks, SPubs, SFuns},
                                        persistent_store = PersistentStore,
                                        pending_ack = PA }) ->
    %% If we are a non-durable queue, or (no persisent pubs, and no
    %% persistent acks) then we can skip the queue_index loop.
    case PersistentStore == ?TRANSIENT_MSG_STORE orelse
        (IsTransientPubs andalso
         lists:foldl(
           fun (AckTag,  true ) ->
                   case dict:find(AckTag, PA) of
                       {ok, #msg_status{}}         -> true;
                       {ok, {IsPersistent, _Guid}} -> not IsPersistent
                   end;
               (_AckTag, false) -> false
           end, true, AckTags)) of
        true  -> State1 = tx_commit_index(State #vqstate {
                                            on_sync = {[], [Pubs], [Fun]} }),
                 State1 #vqstate { on_sync = OnSync };
        false -> State #vqstate { on_sync = { [AckTags | SAcks],
                                              [Pubs | SPubs],
                                              [Fun | SFuns] }}
    end.

tx_commit_index(State = #vqstate { on_sync = {_, _, []} }) ->
    State;
tx_commit_index(State = #vqstate { on_sync = {SAcks, SPubs, SFuns},
                                   persistent_store = PersistentStore }) ->
    Acks = lists:flatten(SAcks),
    State1 = ack(Acks, State),
    IsPersistentStore = ?PERSISTENT_MSG_STORE == PersistentStore,
    Pubs = lists:flatten(lists:reverse(SPubs)),
    {SeqIds, State2 = #vqstate { index_state = IndexState }} =
        lists:foldl(
          fun (Msg = #basic_message { is_persistent = IsPersistent },
               {SeqIdsAcc, StateN}) ->
                  {SeqId, StateN1} =
                      publish(Msg, false, IsPersistent, StateN),
                  {case IsPersistentStore andalso IsPersistent of
                       true  -> [SeqId | SeqIdsAcc];
                       false -> SeqIdsAcc
                   end, StateN1}
          end, {Acks, State1}, Pubs),
    IndexState1 =
        rabbit_queue_index:sync_seq_ids(SeqIds, IndexState),
    [ Fun() || Fun <- lists:reverse(SFuns) ],
    State2 #vqstate { index_state = IndexState1, on_sync = {[], [], []} }.

delete1(_PersistentStore, _TransientThreshold, NextSeqId, Count, DeltaSeqId,
        IndexState) when DeltaSeqId >= NextSeqId ->
    {Count, IndexState};
delete1(PersistentStore, TransientThreshold, NextSeqId, Count, DeltaSeqId,
        IndexState) ->
    Delta1SeqId = DeltaSeqId + rabbit_queue_index:segment_size(),
    case rabbit_queue_index:read_segment_entries(DeltaSeqId, IndexState) of
        {[], IndexState1} ->
            delete1(PersistentStore, TransientThreshold, NextSeqId, Count,
                    Delta1SeqId, IndexState1);
        {List, IndexState1} ->
            {Q, IndexState2} =
                betas_from_segment_entries(
                  List, Delta1SeqId, TransientThreshold, IndexState1),
            {QCount, IndexState3} =
                remove_queue_entries(
                  PersistentStore, fun beta_fold_no_index_on_disk/3,
                  Q, IndexState2),
            delete1(PersistentStore, TransientThreshold, NextSeqId,
                    Count + QCount, Delta1SeqId, IndexState3)
    end.

purge1(Count, State = #vqstate { q3 = Q3, index_state = IndexState,
                                 persistent_store = PersistentStore }) ->
    case bpqueue:is_empty(Q3) of
        true ->
            {Q1Count, IndexState1} =
                remove_queue_entries(
                  PersistentStore, fun rabbit_misc:queue_fold/3,
                  State #vqstate.q1, IndexState),
            {Count + Q1Count, State #vqstate { q1 = queue:new(),
                                               index_state = IndexState1 }};
        false ->
            {Q3Count, IndexState1} =
                remove_queue_entries(
                  PersistentStore, fun beta_fold_no_index_on_disk/3,
                  Q3, IndexState),
            purge1(Count + Q3Count,
                   maybe_deltas_to_betas(
                     State #vqstate { index_state = IndexState1,
                                      q3 = bpqueue:new() }))
    end.

remove_queue_entries(PersistentStore, Fold, Q, IndexState) ->
    {_PersistentStore, Count, GuidsByStore, SeqIds, IndexState1} =
        Fold(fun remove_queue_entries1/2,
             {PersistentStore, 0, dict:new(), [], IndexState}, Q),
    ok = dict:fold(fun (MsgStore, Guids, ok) ->
                           rabbit_msg_store:remove(MsgStore, Guids)
                   end, ok, GuidsByStore),
    IndexState2 =
        case SeqIds of
            [] -> IndexState1;
            _  -> rabbit_queue_index:write_acks(SeqIds, IndexState1)
        end,
    {Count, IndexState2}.

remove_queue_entries1(
  #msg_status { guid = Guid, seq_id = SeqId,
                is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
                index_on_disk = IndexOnDisk, is_persistent = IsPersistent },
  {PersistentStore, CountN, GuidsByStore, SeqIdsAcc, IndexStateN}) ->
    GuidsByStore1 =
        case {MsgOnDisk, IsPersistent} of
            {true,  true}  ->
                rabbit_misc:dict_cons(PersistentStore, Guid, GuidsByStore);
            {true,  false} ->
                rabbit_misc:dict_cons(?TRANSIENT_MSG_STORE, Guid, GuidsByStore);
            {false, _}     ->
                GuidsByStore
        end,
    SeqIdsAcc1 = case IndexOnDisk of
                     true  -> [SeqId | SeqIdsAcc];
                     false -> SeqIdsAcc
                 end,
    IndexStateN1 = case IndexOnDisk andalso not IsDelivered of
                       true -> rabbit_queue_index:write_delivered(
                                 SeqId, IndexStateN);
                       false -> IndexStateN
                   end,
    {PersistentStore, CountN + 1, GuidsByStore1, SeqIdsAcc1, IndexStateN1}.

fetch_from_q3_or_delta(State = #vqstate {
                         q1 = Q1, q2 = Q2, delta = #delta { count = DeltaCount },
                         q3 = Q3, q4 = Q4, ram_msg_count = RamMsgCount,
                         ram_index_count = RamIndexCount,
                         msg_store_clients = MSCState,
                         persistent_store = PersistentStore }) ->
    case bpqueue:out(Q3) of
        {empty, _Q3} ->
            0 = DeltaCount, %% ASSERTION
            true = bpqueue:is_empty(Q2), %% ASSERTION
            true = queue:is_empty(Q1), %% ASSERTION
            {empty, State};
        {{value, IndexOnDisk, MsgStatus = #msg_status {
                                msg = undefined, guid = Guid,
                                is_persistent = IsPersistent }}, Q3a} ->
            {{ok, Msg = #basic_message { is_persistent = IsPersistent,
                                         guid = Guid }}, MSCState1} =
                read_from_msg_store(
                  PersistentStore, MSCState, IsPersistent, Guid),
            Q4a = queue:in(MsgStatus #msg_status { msg = Msg }, Q4),
            RamIndexCount1 = case IndexOnDisk of
                                 true  -> RamIndexCount;
                                 false -> RamIndexCount - 1
                             end,
            true = RamIndexCount1 >= 0, %% ASSERTION
            State1 = State #vqstate { q3 = Q3a, q4 = Q4a,
                                      ram_msg_count = RamMsgCount + 1,
                                      ram_index_count = RamIndexCount1,
                                      msg_store_clients = MSCState1 },
            State2 =
                case {bpqueue:is_empty(Q3a), 0 == DeltaCount} of
                    {true, true} ->
                        %% q3 is now empty, it wasn't before; delta is
                        %% still empty. So q2 must be empty, and q1
                        %% can now be joined onto q4
                        true = bpqueue:is_empty(Q2), %% ASSERTION
                        State1 #vqstate { q1 = queue:new(),
                                          q4 = queue:join(Q4a, Q1) };
                    {true, false} ->
                        maybe_deltas_to_betas(State1);
                    {false, _} ->
                        %% q3 still isn't empty, we've not touched
                        %% delta, so the invariants between q1, q2,
                        %% delta and q3 are maintained
                        State1
                end,
            {loaded, State2}
    end.

reduce_memory_use(State = #vqstate { ram_msg_count = RamMsgCount,
                                     target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount == undefined orelse TargetRamMsgCount >= RamMsgCount ->
    State;
reduce_memory_use(State =
                  #vqstate { target_ram_msg_count = TargetRamMsgCount }) ->
    State1 = maybe_push_q4_to_betas(maybe_push_q1_to_betas(State)),
    case TargetRamMsgCount of
        0 -> push_betas_to_deltas(State1);
        _ -> State1
    end.

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

test_keep_msg_in_ram(SeqId, #vqstate { target_ram_msg_count = TargetRamMsgCount,
                                       ram_msg_count = RamMsgCount,
                                       q1 = Q1, q3 = Q3 }) ->
    case TargetRamMsgCount of
        undefined ->
            msg;
        0 ->
            case bpqueue:out(Q3) of
                {empty, _Q3} ->
                    %% if TargetRamMsgCount == 0, we know we have no
                    %% alphas. If q3 is empty then delta must be empty
                    %% too, so create a beta, which should end up in
                    %% q3
                    index;
                {{value, _IndexOnDisk, #msg_status { seq_id = OldSeqId }},
                 _Q3a} ->
                    %% Don't look at the current delta as it may be
                    %% empty. If the SeqId is still within the current
                    %% segment, it'll be a beta, else it'll go into
                    %% delta
                    case SeqId >= rabbit_queue_index:next_segment_boundary(
                                    OldSeqId) of
                        true  -> neither;
                        false -> index
                    end
            end;
        _ when TargetRamMsgCount > RamMsgCount ->
            msg;
        _ ->
            case queue:is_empty(Q1) of
                true  -> index;
                %% Can push out elders (in q1) to disk. This may also
                %% result in the msg itself going to disk and q2/q3.
                false -> msg
            end
    end.

publish(Msg = #basic_message { is_persistent = IsPersistent, guid = Guid },
        IsDelivered, MsgOnDisk, State =
        #vqstate { next_seq_id = SeqId, len = Len, in_counter = InCount,
                   persistent_count = PCount }) ->
    MsgStatus = #msg_status {
      msg = Msg, guid = Guid, seq_id = SeqId, is_persistent = IsPersistent,
      is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
      index_on_disk = false },
    PCount1 = PCount + case IsPersistent of
                           true  -> 1;
                           false -> 0
                       end,
    {SeqId, publish(test_keep_msg_in_ram(SeqId, State), MsgStatus,
                    State #vqstate { next_seq_id = SeqId + 1, len = Len + 1,
                                     in_counter = InCount + 1,
                                     persistent_count = PCount1 })}.

publish(msg, MsgStatus, #vqstate {
               index_state = IndexState, ram_msg_count = RamMsgCount,
               msg_store_clients = MSCState,
               persistent_store = PersistentStore } = State) ->
    {MsgStatus1, MSCState1} =
        maybe_write_msg_to_disk(PersistentStore, false, MsgStatus, MSCState),
    {MsgStatus2, IndexState1} =
        maybe_write_index_to_disk(false, MsgStatus1, IndexState),
    State1 = State #vqstate { ram_msg_count = RamMsgCount + 1,
                              index_state = IndexState1,
                              msg_store_clients = MSCState1 },
    store_alpha_entry(MsgStatus2, State1);

publish(index, MsgStatus, #vqstate {
                 index_state = IndexState, q1 = Q1,
                 ram_index_count = RamIndexCount, msg_store_clients = MSCState,
                 persistent_store = PersistentStore } = State) ->
    {MsgStatus1 = #msg_status { msg_on_disk = true }, MSCState1} =
        maybe_write_msg_to_disk(PersistentStore, true, MsgStatus, MSCState),
    ForceIndex = should_force_index_to_disk(State),
    {MsgStatus2, IndexState1} =
        maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
    RamIndexCount1 = case MsgStatus2 #msg_status.index_on_disk of
                         true  -> RamIndexCount;
                         false -> RamIndexCount + 1
                     end,
    State1 = State #vqstate { index_state = IndexState1,
                              ram_index_count = RamIndexCount1,
                              msg_store_clients = MSCState1 },
    true = queue:is_empty(Q1), %% ASSERTION
    store_beta_entry(MsgStatus2, State1);

publish(neither, MsgStatus = #msg_status { seq_id = SeqId }, State =
            #vqstate { index_state = IndexState, q1 = Q1, q2 = Q2,
                       delta = Delta, msg_store_clients = MSCState,
                       persistent_store = PersistentStore }) ->
    {MsgStatus1 = #msg_status { msg_on_disk = true }, MSCState1} =
        maybe_write_msg_to_disk(PersistentStore, true, MsgStatus, MSCState),
    {#msg_status { index_on_disk = true }, IndexState1} =
        maybe_write_index_to_disk(true, MsgStatus1, IndexState),
    true = queue:is_empty(Q1) andalso bpqueue:is_empty(Q2), %% ASSERTION
    %% delta may be empty, seq_id > next_segment_boundary from q3
    %% head, so we need to find where the segment boundary is before
    %% or equal to seq_id
    DeltaSeqId = rabbit_queue_index:next_segment_boundary(SeqId) -
        rabbit_queue_index:segment_size(),
    Delta1 = #delta { start_seq_id = DeltaSeqId, count = 1,
                      end_seq_id = SeqId + 1 },
    State #vqstate { index_state = IndexState1,
                     delta = combine_deltas(Delta, Delta1),
                     msg_store_clients = MSCState1 }.

store_alpha_entry(MsgStatus, State =
                  #vqstate { q1 = Q1, q2 = Q2,
                             delta = #delta { count = DeltaCount },
                             q3 = Q3, q4 = Q4 }) ->
    case bpqueue:is_empty(Q2) andalso 0 == DeltaCount andalso
        bpqueue:is_empty(Q3) of
        true  -> true = queue:is_empty(Q1), %% ASSERTION
                 State #vqstate { q4 = queue:in(MsgStatus, Q4) };
        false -> maybe_push_q1_to_betas(
                   State #vqstate { q1 = queue:in(MsgStatus, Q1) })
    end.

store_beta_entry(MsgStatus = #msg_status { msg_on_disk = true,
                                           index_on_disk = IndexOnDisk },
                 State = #vqstate { q2 = Q2,
                                    delta = #delta { count = DeltaCount },
                                    q3 = Q3 }) ->
    MsgStatus1 = MsgStatus #msg_status { msg = undefined },
    case DeltaCount == 0 of
        true ->
            State #vqstate { q3 = bpqueue:in(IndexOnDisk, MsgStatus1, Q3) };
        false ->
            State #vqstate { q2 = bpqueue:in(IndexOnDisk, MsgStatus1, Q2) }
    end.

find_msg_store(true, PersistentStore)   -> PersistentStore;
find_msg_store(false, _PersistentStore) -> ?TRANSIENT_MSG_STORE.

with_msg_store_state(PersistentStore, {{MSCStateP, PRef}, MSCStateT}, true,
                     Fun) ->
    {Result, MSCStateP1} = Fun(PersistentStore, MSCStateP),
    {Result, {{MSCStateP1, PRef}, MSCStateT}};
with_msg_store_state(_PersistentStore, {MSCStateP, {MSCStateT, TRef}}, false,
                     Fun) ->
    {Result, MSCStateT1} = Fun(?TRANSIENT_MSG_STORE, MSCStateT),
    {Result, {MSCStateP, {MSCStateT1, TRef}}}.

read_from_msg_store(PersistentStore, MSCState, IsPersistent, Guid) ->
    with_msg_store_state(
      PersistentStore, MSCState, IsPersistent,
      fun (MsgStore, MSCState1) ->
              rabbit_msg_store:read(MsgStore, Guid, MSCState1)
      end).

maybe_write_msg_to_disk(_PersistentStore, _Force, MsgStatus =
                        #msg_status { msg_on_disk = true }, MSCState) ->
    {MsgStatus, MSCState};
maybe_write_msg_to_disk(PersistentStore, Force,
                        MsgStatus = #msg_status {
                          msg = Msg, guid = Guid,
                          is_persistent = IsPersistent }, MSCState)
  when Force orelse IsPersistent ->
    {ok, MSCState1} =
        with_msg_store_state(
          PersistentStore, MSCState, IsPersistent,
          fun (MsgStore, MSCState2) ->
                  rabbit_msg_store:write(
                    MsgStore, Guid, ensure_binary_properties(Msg), MSCState2)
          end),
    {MsgStatus #msg_status { msg_on_disk = true }, MSCState1};
maybe_write_msg_to_disk(_PersistentStore, _Force, MsgStatus, MSCState) ->
    {MsgStatus, MSCState}.

maybe_write_index_to_disk(_Force, MsgStatus =
                          #msg_status { index_on_disk = true }, IndexState) ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    {MsgStatus, IndexState};
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
                                   guid = Guid, seq_id = SeqId,
                                   is_persistent = IsPersistent,
                                   is_delivered = IsDelivered }, IndexState)
  when Force orelse IsPersistent ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    IndexState1 = rabbit_queue_index:write_published(
                    Guid, SeqId, IsPersistent, IndexState),
    {MsgStatus #msg_status { index_on_disk = true },
     case IsDelivered of
         true  -> rabbit_queue_index:write_delivered(SeqId, IndexState1);
         false -> IndexState1
     end};
maybe_write_index_to_disk(_Force, MsgStatus, IndexState) ->
    {MsgStatus, IndexState}.

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

limit_ram_index(State = #vqstate { ram_index_count = RamIndexCount }) ->
    case permitted_ram_index_count(State) of
        undefined ->
            State;
        Permitted when RamIndexCount > Permitted ->
            Reduction = lists:min([RamIndexCount - Permitted,
                                   ?RAM_INDEX_BATCH_SIZE]),
            case Reduction < ?RAM_INDEX_BATCH_SIZE of
                true ->
                    State;
                false ->
                    {Reduction1, State1} = limit_q2_ram_index(Reduction, State),
                    {_Red2, State2} = limit_q3_ram_index(Reduction1, State1),
                    State2
            end;
        _ ->
            State
    end.

limit_q2_ram_index(Reduction, State = #vqstate { q2 = Q2 })
  when Reduction > 0 ->
    {Q2a, Reduction1, State1} = limit_ram_index(fun bpqueue:map_fold_filter_l/4,
                                                Q2, Reduction, State),
    {Reduction1, State1 #vqstate { q2 = Q2a }};
limit_q2_ram_index(Reduction, State) ->
    {Reduction, State}.

limit_q3_ram_index(Reduction, State = #vqstate { q3 = Q3 })
  when Reduction > 0 ->
    %% use the _r version so that we prioritise the msgs closest to
    %% delta, and least soon to be delivered
    {Q3a, Reduction1, State1} = limit_ram_index(fun bpqueue:map_fold_filter_r/4,
                                                Q3, Reduction, State),
    {Reduction1, State1 #vqstate { q3 = Q3a }};
limit_q3_ram_index(Reduction, State) ->
    {Reduction, State}.

limit_ram_index(MapFoldFilterFun, Q, Reduction, State =
                #vqstate { ram_index_count = RamIndexCount,
                           index_state = IndexState }) ->
    {Qa, {Reduction1, IndexState1}} =
        MapFoldFilterFun(
          fun erlang:'not'/1,
          fun (MsgStatus, {0, _IndexStateN}) ->
                  false = MsgStatus #msg_status.index_on_disk, %% ASSERTION
                  stop;
              (MsgStatus, {N, IndexStateN}) when N > 0 ->
                  false = MsgStatus #msg_status.index_on_disk, %% ASSERTION
                  {MsgStatus1, IndexStateN1} =
                      maybe_write_index_to_disk(true, MsgStatus, IndexStateN),
                  {true, MsgStatus1, {N-1, IndexStateN1}}
          end, {Reduction, IndexState}, Q),
    RamIndexCount1 = RamIndexCount - (Reduction - Reduction1),
    {Qa, Reduction1, State #vqstate { index_state = IndexState1,
                                      ram_index_count = RamIndexCount1 }}.

maybe_deltas_to_betas(State = #vqstate { delta = ?BLANK_DELTA_PATTERN(X) }) ->
    State;
maybe_deltas_to_betas(
  State = #vqstate { index_state = IndexState, q2 = Q2, q3 = Q3,
                     target_ram_msg_count = TargetRamMsgCount,
                     delta = Delta = #delta { start_seq_id = DeltaSeqId,
                                              count = DeltaCount,
                                              end_seq_id = DeltaSeqIdEnd },
                     transient_threshold = TransientThreshold}) ->
    case (not bpqueue:is_empty(Q3)) andalso (0 == TargetRamMsgCount) of
        true ->
            State;
        false ->
            %% either q3 is empty, in which case we load at least one
            %% segment, or TargetRamMsgCount > 0, meaning we should
            %% really be holding all the betas in memory.
            {List, IndexState1, Delta1SeqId} =
                read_index_segment(DeltaSeqId, IndexState),
            %% length(List) may be < segment_size because of acks.  It
            %% could be [] if we ignored every message in the segment
            %% due to it being transient and below the threshold
            {Q3a, IndexState2} =
                betas_from_segment_entries(
                  List, DeltaSeqIdEnd, TransientThreshold, IndexState1),
            State1 = State #vqstate { index_state = IndexState2 },
            case bpqueue:len(Q3a) of
                0 ->
                    maybe_deltas_to_betas(
                      State #vqstate {
                        delta = Delta #delta { start_seq_id = Delta1SeqId }});
                Q3aLen ->
                    Q3b = bpqueue:join(Q3, Q3a),
                    case DeltaCount - Q3aLen of
                        0 ->
                            %% delta is now empty, but it wasn't
                            %% before, so can now join q2 onto q3
                            State1 #vqstate { delta = ?BLANK_DELTA,
                                              q2 = bpqueue:new(),
                                              q3 = bpqueue:join(Q3b, Q2) };
                        N when N > 0 ->
                            State1 #vqstate {
                              q3 = Q3b,
                              delta = #delta { start_seq_id = Delta1SeqId,
                                               count = N,
                                               end_seq_id = DeltaSeqIdEnd } }
                    end
            end
    end.

maybe_push_q1_to_betas(State = #vqstate { q1 = Q1 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out/1,
      fun (MsgStatus, Q1a, State1) ->
              %% these could legally go to q3 if delta and q2 are empty
              store_beta_entry(MsgStatus, State1 #vqstate { q1 = Q1a })
      end, Q1, State).

maybe_push_q4_to_betas(State = #vqstate { q4 = Q4 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out_r/1,
      fun (MsgStatus = #msg_status { index_on_disk = IndexOnDisk },
           Q4a, State1 = #vqstate { q3 = Q3 }) ->
              MsgStatus1 = MsgStatus #msg_status { msg = undefined },
              %% these must go to q3
              State1 #vqstate { q3 = bpqueue:in_r(IndexOnDisk, MsgStatus1, Q3),
                                q4 = Q4a }
      end, Q4, State).

maybe_push_alphas_to_betas(_Generator, _Consumer, _Q, State =
                           #vqstate { ram_msg_count = RamMsgCount,
                                      target_ram_msg_count = TargetRamMsgCount })
  when TargetRamMsgCount == undefined orelse TargetRamMsgCount >= RamMsgCount ->
    State;
maybe_push_alphas_to_betas(
  Generator, Consumer, Q, State =
  #vqstate { ram_msg_count = RamMsgCount, ram_index_count = RamIndexCount,
             index_state = IndexState, msg_store_clients = MSCState,
             persistent_store = PersistentStore }) ->
    case Generator(Q) of
        {empty, _Q} -> State;
        {{value, MsgStatus}, Qa} ->
            {MsgStatus1, MSCState1} =
                maybe_write_msg_to_disk(
                  PersistentStore, true, MsgStatus, MSCState),
            ForceIndex = should_force_index_to_disk(State),
            {MsgStatus2, IndexState1} =
                maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
            RamIndexCount1 = case MsgStatus2 #msg_status.index_on_disk of
                                 true  -> RamIndexCount;
                                 false -> RamIndexCount + 1
                             end,
            State1 = State #vqstate { ram_msg_count = RamMsgCount - 1,
                                      ram_index_count = RamIndexCount1,
                                      index_state = IndexState1,
                                      msg_store_clients = MSCState1 },
            maybe_push_alphas_to_betas(Generator, Consumer, Qa,
                                       Consumer(MsgStatus2, Qa, State1))
    end.

push_betas_to_deltas(State = #vqstate { q2 = Q2, delta = Delta, q3 = Q3,
                                        ram_index_count = RamIndexCount,
                                        index_state = IndexState }) ->
    %% HighSeqId is high in the sense that it must be higher than the
    %% seq_id in Delta, but it's also the lowest of the betas that we
    %% transfer from q2 to delta.
    {HighSeqId, Len1, Q2a, RamIndexCount1, IndexState1} =
        push_betas_to_deltas(
          fun bpqueue:out/1, undefined, Q2, RamIndexCount, IndexState),
    true = bpqueue:is_empty(Q2a), %% ASSERTION
    EndSeqId =
        case bpqueue:out_r(Q2) of
            {empty, _Q2} ->
                undefined;
            {{value, _IndexOnDisk, #msg_status { seq_id = EndSeqId1 }}, _Q2} ->
                EndSeqId1 + 1
        end,
    Delta1 = #delta { start_seq_id = Delta1SeqId } =
        combine_deltas(Delta, #delta { start_seq_id = HighSeqId,
                                       count = Len1,
                                       end_seq_id = EndSeqId }),
    State1 = State #vqstate { q2 = bpqueue:new(), delta = Delta1,
                              index_state = IndexState1,
                              ram_index_count = RamIndexCount1 },
    case bpqueue:out(Q3) of
        {empty, _Q3} ->
            State1;
        {{value, _IndexOnDisk1, #msg_status { seq_id = SeqId }}, _Q3} ->
            {{value, _IndexOnDisk2, #msg_status { seq_id = SeqIdMax }}, _Q3a} =
                bpqueue:out_r(Q3),
            Limit = rabbit_queue_index:next_segment_boundary(SeqId),
            %% ASSERTION
            true = Delta1SeqId == undefined orelse Delta1SeqId > SeqIdMax,
            case SeqIdMax < Limit of
                true -> %% already only holding LTE one segment indices in q3
                    State1;
                false ->
                    %% ASSERTION
                    %% This says that if Delta1SeqId /= undefined then
                    %% the gap from Limit to Delta1SeqId is an integer
                    %% multiple of segment_size
                    0 = case Delta1SeqId of
                            undefined -> 0;
                            _ -> (Delta1SeqId - Limit) rem
                                     rabbit_queue_index:segment_size()
                        end,
                    %% SeqIdMax is low in the sense that it must be
                    %% lower than the seq_id in delta1, in fact either
                    %% delta1 has undefined as its seq_id or there
                    %% does not exist a seq_id X s.t. X > SeqIdMax and
                    %% X < delta1's seq_id (would be +1 if it wasn't
                    %% for the possibility of gaps in the seq_ids).
                    %% But because we use queue:out_r, SeqIdMax is
                    %% actually also the highest seq_id of the betas we
                    %% transfer from q3 to deltas.
                    {SeqIdMax, Len2, Q3a, RamIndexCount2, IndexState2} =
                        push_betas_to_deltas(fun bpqueue:out_r/1, Limit, Q3,
                                             RamIndexCount1, IndexState1),
                    Delta2 = combine_deltas(#delta { start_seq_id = Limit,
                                                     count = Len2,
                                                     end_seq_id = SeqIdMax+1 },
                                            Delta1),
                    State1 #vqstate { q3 = Q3a, delta = Delta2,
                                      index_state = IndexState2,
                                      ram_index_count = RamIndexCount2 }
            end
    end.

push_betas_to_deltas(Generator, Limit, Q, RamIndexCount, IndexState) ->
    case Generator(Q) of
        {empty, Qa} -> {undefined, 0, Qa, RamIndexCount, IndexState};
        {{value, _IndexOnDisk, #msg_status { seq_id = SeqId }}, _Qa} ->
            {Count, Qb, RamIndexCount1, IndexState1} =
                push_betas_to_deltas(
                  Generator, Limit, Q, 0, RamIndexCount, IndexState),
            {SeqId, Count, Qb, RamIndexCount1, IndexState1}
    end.

push_betas_to_deltas(Generator, Limit, Q, Count, RamIndexCount, IndexState) ->
    case Generator(Q) of
        {empty, Qa} ->
            {Count, Qa, RamIndexCount, IndexState};
        {{value, _IndexOnDisk, #msg_status { seq_id = SeqId }}, _Qa}
        when Limit =/= undefined andalso SeqId < Limit ->
            {Count, Q, RamIndexCount, IndexState};
        {{value, IndexOnDisk, MsgStatus}, Qa} ->
            {RamIndexCount1, IndexState1} =
                case IndexOnDisk of
                    true -> {RamIndexCount, IndexState};
                    false ->
                        {#msg_status { index_on_disk = true }, IndexState2} =
                            maybe_write_index_to_disk(true, MsgStatus,
                                                      IndexState),
                        {RamIndexCount - 1, IndexState2}
                end,
            push_betas_to_deltas(
              Generator, Limit, Qa, Count + 1, RamIndexCount1, IndexState1)
    end.

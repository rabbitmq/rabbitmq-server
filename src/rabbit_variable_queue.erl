%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_variable_queue).

-export([init/3, terminate/2, delete_and_terminate/2,
         purge/1, publish/4, publish_delivered/5, drain_confirmed/1,
         dropwhile/2, fetch/2, ack/2, requeue/3, len/1, is_empty/1,
         set_ram_duration_target/2, ram_duration/1,
         needs_timeout/1, timeout/1, handle_pre_hibernate/1,
         status/1, invoke/3, is_duplicate/2, discard/3,
         multiple_routing_keys/0]).

-export([start/1, stop/0]).

%% exported for testing only
-export([start_msg_store/2, stop_msg_store/0, init/5]).

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
%%
%% Also note that within this code, the term gamma never
%% appears. Instead, gammas are defined by betas who have had their
%% queue position recorded on disk.
%%
%% In general, messages move q1 -> q2 -> delta -> q3 -> q4, though
%% many of these steps are frequently skipped. q1 and q4 only hold
%% alphas, q2 and q3 hold both betas and gammas (as queues of queues,
%% using the bpqueue module where the block prefix determines whether
%% they're betas or gammas). When a message arrives, its
%% classification is determined. It is then added to the rightmost
%% appropriate queue.
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
%% messages we should hold in RAM. We track the ingress and egress
%% rates for both messages and pending acks and rates for both are
%% considered when calculating the number of messages to hold in
%% RAM. When we need to push alphas to betas or betas to gammas, we
%% favour writing out messages that are further from the head of the
%% queue. This minimises writes to disk, as the messages closer to the
%% tail of the queue stay in the queue for longer, thus do not need to
%% be replaced as quickly by sending other messages to disk.
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
%% If a queue is full of transient messages, then the transition from
%% betas to deltas will be potentially very expensive as millions of
%% entries must be written to disk by the queue_index module. This can
%% badly stall the queue. In order to avoid this, the proportion of
%% gammas / (betas+gammas) must not be lower than (betas+gammas) /
%% (alphas+betas+gammas). As the queue grows or available memory
%% shrinks, the latter ratio increases, requiring the conversion of
%% more gammas to betas in order to maintain the invariant. At the
%% point at which betas and gammas must be converted to deltas, there
%% should be very few betas remaining, thus the transition is fast (no
%% work needs to be done for the gamma -> delta transition).
%%
%% The conversion of betas to gammas is done in batches of exactly
%% ?IO_BATCH_SIZE. This value should not be too small, otherwise the
%% frequent operations on the queues of q2 and q3 will not be
%% effectively amortised (switching the direction of queue access
%% defeats amortisation), nor should it be too big, otherwise
%% converting a batch stalls the queue for too long. Therefore, it
%% must be just right. ram_index_count is used here and is the number
%% of betas.
%%
%% The conversion from alphas to betas is also chunked, but only to
%% ensure no more than ?IO_BATCH_SIZE alphas are converted to betas at
%% any one time. This further smooths the effects of changes to the
%% target_ram_count and ensures the queue remains responsive
%% even when there is a large amount of IO work to do. The
%% timeout callback is utilised to ensure that conversions are
%% done as promptly as possible whilst ensuring the queue remains
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
%% Pending acks are recorded in memory either as the tuple {SeqId,
%% MsgId, MsgProps} (tuple-form) or as the message itself (message-
%% form). Acks for persistent messages are always stored in the tuple-
%% form. Acks for transient messages are also stored in tuple-form if
%% the message has been sent to disk as part of the memory reduction
%% process. For transient messages that haven't already been written
%% to disk, acks are stored in message-form.
%%
%% During memory reduction, acks stored in message-form are converted
%% to tuple-form, and the corresponding messages are pushed out to
%% disk.
%%
%% The order in which alphas are pushed to betas and message-form acks
%% are pushed to disk is determined dynamically. We always prefer to
%% push messages for the source (alphas or acks) that is growing the
%% fastest (with growth measured as avg. ingress - avg. egress). In
%% each round of memory reduction a chunk of messages at most
%% ?IO_BATCH_SIZE in size is allocated to be pushed to disk. The
%% fastest growing source will be reduced by as much of this chunk as
%% possible. If there is any remaining allocation in the chunk after
%% the first source has been reduced to zero, the second source will
%% be reduced by as much of the remaining chunk as possible.
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
%%----------------------------------------------------------------------------

-behaviour(rabbit_backing_queue).

-record(vqstate,
        { q1,
          q2,
          delta,
          q3,
          q4,
          next_seq_id,
          pending_ack,
          pending_ack_index,
          ram_ack_index,
          index_state,
          msg_store_clients,
          durable,
          transient_threshold,

          async_callback,

          len,
          persistent_count,

          target_ram_count,
          ram_msg_count,
          ram_msg_count_prev,
          ram_ack_count_prev,
          ram_index_count,
          out_counter,
          in_counter,
          rates,
          msgs_on_disk,
          msg_indices_on_disk,
          unconfirmed,
          confirmed,
          ack_out_counter,
          ack_in_counter,
          ack_rates
        }).

-record(rates, { egress, ingress, avg_egress, avg_ingress, timestamp }).

-record(delta,
        { start_seq_id, %% start_seq_id is inclusive
          count,
          end_seq_id    %% end_seq_id is exclusive
        }).

%% When we discover, on publish, that we should write some indices to
%% disk for some betas, the IO_BATCH_SIZE sets the number of betas
%% that we must be due to write indices for before we do any work at
%% all. This is both a minimum and a maximum - we don't write fewer
%% than IO_BATCH_SIZE indices out in one go, and we don't write more -
%% we can always come back on the next publish to do more.
-define(IO_BATCH_SIZE, 64).
-define(PERSISTENT_MSG_STORE, msg_store_persistent).
-define(TRANSIENT_MSG_STORE,  msg_store_transient).

-include("rabbit.hrl").
-include("rabbit_backing_queue.hrl").

%%----------------------------------------------------------------------------

-rabbit_upgrade({multiple_routing_keys, local, []}).

-ifdef(use_specs).

-type(timestamp() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}).
-type(seq_id()  :: non_neg_integer()).
-type(ack()     :: seq_id()).

-type(rates() :: #rates { egress      :: {timestamp(), non_neg_integer()},
                          ingress     :: {timestamp(), non_neg_integer()},
                          avg_egress  :: float(),
                          avg_ingress :: float(),
                          timestamp   :: timestamp() }).

-type(delta() :: #delta { start_seq_id :: non_neg_integer(),
                          count        :: non_neg_integer(),
                          end_seq_id   :: non_neg_integer() }).

-type(state() :: #vqstate {
             q1                    :: queue(),
             q2                    :: bpqueue:bpqueue(),
             delta                 :: delta(),
             q3                    :: gb_tree(),
             q4                    :: gb_tree(),
             next_seq_id           :: seq_id(),
             pending_ack           :: dict(),
             ram_ack_index         :: gb_tree(),
             index_state           :: any(),
             msg_store_clients     :: 'undefined' | {{any(), binary()},
                                                    {any(), binary()}},
             durable               :: boolean(),
             transient_threshold   :: non_neg_integer(),

             async_callback        :: async_callback(),

             len                   :: non_neg_integer(),
             persistent_count      :: non_neg_integer(),

             target_ram_count      :: non_neg_integer() | 'infinity',
             ram_msg_count         :: non_neg_integer(),
             ram_msg_count_prev    :: non_neg_integer(),
             ram_index_count       :: non_neg_integer(),
             out_counter           :: non_neg_integer(),
             in_counter            :: non_neg_integer(),
             rates                 :: rates(),
             msgs_on_disk          :: gb_set(),
             msg_indices_on_disk   :: gb_set(),
             unconfirmed           :: gb_set(),
             confirmed             :: gb_set(),
             ack_out_counter       :: non_neg_integer(),
             ack_in_counter        :: non_neg_integer(),
             ack_rates             :: rates() }).

-include("rabbit_backing_queue_spec.hrl").

-spec(multiple_routing_keys/0 :: () -> 'ok').

-endif.

-define(BLANK_DELTA, #delta { start_seq_id = undefined,
                              count        = 0,
                              end_seq_id   = undefined }).
-define(BLANK_DELTA_PATTERN(Z), #delta { start_seq_id = Z,
                                         count        = 0,
                                         end_seq_id   = Z }).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start(DurableQueues) ->
    {AllTerms, StartFunState} = rabbit_queue_index:recover(DurableQueues),
    start_msg_store(
      [Ref || Terms <- AllTerms,
              begin
                  Ref = proplists:get_value(persistent_ref, Terms),
                  Ref =/= undefined
              end],
      StartFunState).

stop() -> stop_msg_store().

start_msg_store(Refs, StartFunState) ->
    ok = rabbit_sup:start_child(?TRANSIENT_MSG_STORE, rabbit_msg_store,
                                [?TRANSIENT_MSG_STORE, rabbit_mnesia:dir(),
                                 undefined,  {fun (ok) -> finished end, ok}]),
    ok = rabbit_sup:start_child(?PERSISTENT_MSG_STORE, rabbit_msg_store,
                                [?PERSISTENT_MSG_STORE, rabbit_mnesia:dir(),
                                 Refs, StartFunState]).

stop_msg_store() ->
    ok = rabbit_sup:stop_child(?PERSISTENT_MSG_STORE),
    ok = rabbit_sup:stop_child(?TRANSIENT_MSG_STORE).

init(Queue, Recover, AsyncCallback) ->
    init(Queue, Recover, AsyncCallback,
         fun (MsgIds, ActionTaken) ->
                 msgs_written_to_disk(AsyncCallback, MsgIds, ActionTaken)
         end,
         fun (MsgIds) -> msg_indices_written_to_disk(AsyncCallback, MsgIds) end).

init(#amqqueue { name = QueueName, durable = IsDurable }, false,
     AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun) ->
    IndexState = rabbit_queue_index:init(QueueName, MsgIdxOnDiskFun),
    init(IsDurable, IndexState, 0, [], AsyncCallback,
         case IsDurable of
             true  -> msg_store_client_init(?PERSISTENT_MSG_STORE,
                                            MsgOnDiskFun, AsyncCallback);
             false -> undefined
         end,
         msg_store_client_init(?TRANSIENT_MSG_STORE, undefined, AsyncCallback));

init(#amqqueue { name = QueueName, durable = true }, true,
     AsyncCallback, MsgOnDiskFun, MsgIdxOnDiskFun) ->
    Terms = rabbit_queue_index:shutdown_terms(QueueName),
    {PRef, TRef, Terms1} =
        case [persistent_ref, transient_ref] -- proplists:get_keys(Terms) of
            [] -> {proplists:get_value(persistent_ref, Terms),
                   proplists:get_value(transient_ref, Terms),
                   Terms};
            _  -> {rabbit_guid:guid(), rabbit_guid:guid(), []}
        end,
    PersistentClient = msg_store_client_init(?PERSISTENT_MSG_STORE, PRef,
                                             MsgOnDiskFun, AsyncCallback),
    TransientClient  = msg_store_client_init(?TRANSIENT_MSG_STORE, TRef,
                                             undefined, AsyncCallback),
    {DeltaCount, IndexState} =
        rabbit_queue_index:recover(
          QueueName, Terms1,
          rabbit_msg_store:successfully_recovered_state(?PERSISTENT_MSG_STORE),
          fun (MsgId) ->
                  rabbit_msg_store:contains(MsgId, PersistentClient)
          end,
          MsgIdxOnDiskFun),
    init(true, IndexState, DeltaCount, Terms1, AsyncCallback,
         PersistentClient, TransientClient).

terminate(_Reason, State) ->
    State1 = #vqstate { persistent_count  = PCount,
                        index_state       = IndexState,
                        msg_store_clients = {MSCStateP, MSCStateT} } =
        remove_pending_ack(true, State),
    PRef = case MSCStateP of
               undefined -> undefined;
               _         -> ok = rabbit_msg_store:client_terminate(MSCStateP),
                            rabbit_msg_store:client_ref(MSCStateP)
           end,
    ok = rabbit_msg_store:client_terminate(MSCStateT),
    TRef = rabbit_msg_store:client_ref(MSCStateT),
    Terms = [{persistent_ref, PRef},
             {transient_ref, TRef},
             {persistent_count, PCount}],
    a(State1 #vqstate { index_state       = rabbit_queue_index:terminate(
                                              Terms, IndexState),
                        msg_store_clients = undefined }).

%% the only difference between purge and delete is that delete also
%% needs to delete everything that's been delivered and not ack'd.
delete_and_terminate(_Reason, State) ->
    %% TODO: there is no need to interact with qi at all - which we do
    %% as part of 'purge' and 'remove_pending_ack', other than
    %% deleting it.
    {_PurgeCount, State1} = purge(State),
    State2 = #vqstate { index_state         = IndexState,
                        msg_store_clients   = {MSCStateP, MSCStateT} } =
        remove_pending_ack(false, State1),
    IndexState1 = rabbit_queue_index:delete_and_terminate(IndexState),
    case MSCStateP of
        undefined -> ok;
        _         -> rabbit_msg_store:client_delete_and_terminate(MSCStateP)
    end,
    rabbit_msg_store:client_delete_and_terminate(MSCStateT),
    a(State2 #vqstate { index_state       = IndexState1,
                        msg_store_clients = undefined }).

purge(State = #vqstate { q4                = Q4,
                         index_state       = IndexState,
                         msg_store_clients = MSCState,
                         len               = Len,
                         persistent_count  = PCount }) ->
    %% TODO: when there are no pending acks, which is a common case,
    %% we could simply wipe the qi instead of issuing delivers and
    %% acks for all the messages.
    {LensByStore, IndexState1} = remove_queue_entries(
                                   fun q4_fold/3, Q4,
                                   orddict:new(), IndexState, MSCState),
    {LensByStore1, State1 = #vqstate { q1                = Q1,
                                       index_state       = IndexState2,
                                       msg_store_clients = MSCState1 }} =
        purge_betas_and_deltas(LensByStore,
                               State #vqstate { q4          = gb_trees:empty(),
                                                index_state = IndexState1 }),
    {LensByStore2, IndexState3} = remove_queue_entries(
                                    fun rabbit_misc:queue_fold/3, Q1,
                                    LensByStore1, IndexState2, MSCState1),
    PCount1 = PCount - find_persistent_count(LensByStore2),
    {Len, a(State1 #vqstate { q1                = queue:new(),
                              index_state       = IndexState3,
                              len               = 0,
                              ram_msg_count     = 0,
                              ram_index_count   = 0,
                              persistent_count  = PCount1 })}.

publish(Msg, MsgProps, _ChPid, State) ->
    {_SeqId, State1} = publish(Msg, MsgProps, false, false, State),
    a(reduce_memory_use(State1)).

publish_delivered(false, #basic_message { id = MsgId },
                  #message_properties { needs_confirming = NeedsConfirming },
                  _ChPid, State = #vqstate { async_callback = Callback,
                                             len = 0 }) ->
    case NeedsConfirming of
        true  -> blind_confirm(Callback, gb_sets:singleton(MsgId));
        false -> ok
    end,
    {undefined, a(State)};
publish_delivered(true, Msg = #basic_message { is_persistent = IsPersistent,
                                               id = MsgId },
                  MsgProps = #message_properties {
                    needs_confirming = NeedsConfirming },
                  _ChPid, State = #vqstate { len              = 0,
                                             next_seq_id      = SeqId,
                                             out_counter      = OutCount,
                                             in_counter       = InCount,
                                             persistent_count = PCount,
                                             durable          = IsDurable,
                                             unconfirmed      = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = (msg_status(IsPersistent1, SeqId, Msg, MsgProps))
        #msg_status { is_delivered = true },
    {MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
    State2 = record_pending_ack(m(MsgStatus1), State1),
    PCount1 = PCount + one_if(IsPersistent1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    {SeqId, a(reduce_memory_use(
                State2 #vqstate { next_seq_id      = SeqId    + 1,
                                  out_counter      = OutCount + 1,
                                  in_counter       = InCount  + 1,
                                  persistent_count = PCount1,
                                  unconfirmed      = UC1 }))}.

drain_confirmed(State = #vqstate { confirmed = C }) ->
    {gb_sets:to_list(C), State #vqstate { confirmed = gb_sets:new() }}.

dropwhile(Pred, State) ->
    case queue_out(State) of
        {empty, State1} ->
            a(State1);
        {{value, MsgStatus = #msg_status { msg_props = MsgProps }}, State1} ->
            case Pred(MsgProps) of
                true ->  {_, State2} = internal_fetch(false, MsgStatus, State1),
                         dropwhile(Pred, State2);
                false -> a(in_r(MsgStatus, State1))
            end
    end.

fetch(AckRequired, State) ->
    case queue_out(State) of
        {empty, State1} ->
            {empty, a(State1)};
        {{value, MsgStatus}, State1} ->
            %% it is possible that the message wasn't read from disk
            %% at this point, so read it in.
            {MsgStatus1, State2} = read_msg(MsgStatus, State1),
            {Res, State3} = internal_fetch(AckRequired, MsgStatus1, State2),
            {Res, a(State3)}
    end.

ack(AckTags, State) ->
    {MsgIds, State1} = ack(fun msg_store_remove/3,
                           fun (_, State0) -> State0 end,
                           AckTags, State),
    {MsgIds, a(State1)}.

requeue(AckTags, MsgPropsFun, State) ->
    MsgPropsFun1 = fun (MsgProps) ->
                           (MsgPropsFun(MsgProps)) #message_properties {
                             needs_confirming = false }
                   end,
    {MsgIds, State1} =
        ack(fun (_, _, _) -> ok end,
            fun (#msg_status { msg = Msg, msg_props = MsgProps }, State1) ->
                    {_SeqId, State2} = publish(Msg, MsgPropsFun1(MsgProps),
                                               true, false, State1),
                    State2;
                ({IsPersistent, MsgId, MsgProps}, State1) ->
                    #vqstate { msg_store_clients = MSCState } = State1,
                    {{ok, Msg = #basic_message{}}, MSCState1} =
                        msg_store_read(MSCState, IsPersistent, MsgId),
                    State2 = State1 #vqstate { msg_store_clients = MSCState1 },
                    {_SeqId, State3} = publish(Msg, MsgPropsFun1(MsgProps),
                                               true, true, State2),
                    State3
            end,
            AckTags, State),
    {MsgIds, a(reduce_memory_use(State1))}.

len(#vqstate { len = Len }) -> Len.

is_empty(State) -> 0 == len(State).

set_ram_duration_target(
  DurationTarget, State = #vqstate {
                    rates     = #rates { avg_egress  = AvgEgressRate,
                                         avg_ingress = AvgIngressRate },
                    ack_rates = #rates { avg_egress  = AvgAckEgressRate,
                                         avg_ingress = AvgAckIngressRate },
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

ram_duration(State = #vqstate {
               rates              = #rates { timestamp = Timestamp,
                                             egress    = Egress,
                                             ingress   = Ingress } = Rates,
               ack_rates          = #rates { timestamp = AckTimestamp,
                                             egress    = AckEgress,
                                             ingress   = AckIngress } = ARates,
               in_counter         = InCount,
               out_counter        = OutCount,
               ack_in_counter     = AckInCount,
               ack_out_counter    = AckOutCount,
               ram_msg_count      = RamMsgCount,
               ram_msg_count_prev = RamMsgCountPrev,
               ram_ack_index      = RamAckIndex,
               ram_ack_count_prev = RamAckCountPrev }) ->
    Now = now(),
    {AvgEgressRate,   Egress1} = update_rate(Now, Timestamp, OutCount, Egress),
    {AvgIngressRate, Ingress1} = update_rate(Now, Timestamp, InCount, Ingress),

    {AvgAckEgressRate,   AckEgress1} =
        update_rate(Now, AckTimestamp, AckOutCount, AckEgress),
    {AvgAckIngressRate, AckIngress1} =
        update_rate(Now, AckTimestamp, AckInCount, AckIngress),

    RamAckCount = gb_trees:size(RamAckIndex),

    Duration = %% msgs+acks / (msgs+acks/sec) == sec
        case (AvgEgressRate == 0 andalso AvgIngressRate == 0 andalso
              AvgAckEgressRate == 0 andalso AvgAckIngressRate == 0) of
            true  -> infinity;
            false -> (RamMsgCountPrev + RamMsgCount +
                          RamAckCount + RamAckCountPrev) /
                         (4 * (AvgEgressRate + AvgIngressRate +
                                   AvgAckEgressRate + AvgAckIngressRate))
        end,

    {Duration, State #vqstate {
                 rates              = Rates #rates {
                                        egress      = Egress1,
                                        ingress     = Ingress1,
                                        avg_egress  = AvgEgressRate,
                                        avg_ingress = AvgIngressRate,
                                        timestamp   = Now },
                 ack_rates          = ARates #rates {
                                        egress      = AckEgress1,
                                        ingress     = AckIngress1,
                                        avg_egress  = AvgAckEgressRate,
                                        avg_ingress = AvgAckIngressRate,
                                        timestamp   = Now },
                 in_counter         = 0,
                 out_counter        = 0,
                 ack_in_counter     = 0,
                 ack_out_counter    = 0,
                 ram_msg_count_prev = RamMsgCount,
                 ram_ack_count_prev = RamAckCount }}.

needs_timeout(State) ->
    case needs_index_sync(State) of
        false -> case reduce_memory_use(
                        fun (_Quota, State1) -> {0, State1} end,
                        fun (_Quota, State1) -> State1 end,
                        fun (State1)         -> State1 end,
                        fun (_Quota, State1) -> {0, State1} end,
                        State) of
                     {true,  _State} -> idle;
                     {false, _State} -> false
                 end;
        true  -> timed
    end.

timeout(State) ->
    a(reduce_memory_use(confirm_commit_index(State))).

handle_pre_hibernate(State = #vqstate { index_state = IndexState }) ->
    State #vqstate { index_state = rabbit_queue_index:flush(IndexState) }.

status(#vqstate {
          q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
          len              = Len,
          pending_ack      = PA,
          ram_ack_index    = RAI,
          target_ram_count = TargetRamCount,
          ram_msg_count    = RamMsgCount,
          ram_index_count  = RamIndexCount,
          next_seq_id      = NextSeqId,
          persistent_count = PersistentCount,
          rates            = #rates { avg_egress  = AvgEgressRate,
                                      avg_ingress = AvgIngressRate },
          ack_rates        = #rates { avg_egress  = AvgAckEgressRate,
                                      avg_ingress = AvgAckIngressRate } }) ->
    [ {q1                  , queue:len(Q1)},
      {q2                  , bpqueue:len(Q2)},
      {delta               , Delta},
      {q3                  , q3tree:len(Q3)},
      {q4                  , gb_trees:size(Q4)},
      {len                 , Len},
      {pending_acks        , dict:size(PA)},
      {target_ram_count    , TargetRamCount},
      {ram_msg_count       , RamMsgCount},
      {ram_ack_count       , gb_trees:size(RAI)},
      {ram_index_count     , RamIndexCount},
      {next_seq_id         , NextSeqId},
      {persistent_count    , PersistentCount},
      {avg_ingress_rate    , AvgIngressRate},
      {avg_egress_rate     , AvgEgressRate},
      {avg_ack_ingress_rate, AvgAckIngressRate},
      {avg_ack_egress_rate , AvgAckEgressRate} ].

invoke(?MODULE, Fun, State) -> Fun(?MODULE, State).

is_duplicate(_Msg, State) -> {false, State}.

discard(_Msg, _ChPid, State) -> State.

%%----------------------------------------------------------------------------
%% Minor helpers
%%----------------------------------------------------------------------------

a(State = #vqstate { q1 = Q1, q2 = Q2, delta = Delta, q3 = Q3, q4 = Q4,
                     len                  = Len,
                     persistent_count     = PersistentCount,
                     ram_msg_count        = RamMsgCount,
                     ram_index_count      = RamIndexCount }) ->
    E1 = queue:is_empty(Q1),
    E2 = bpqueue:is_empty(Q2),
    ED = Delta#delta.count == 0,
    E3 = q3tree:is_empty(Q3),
    E4 = gb_trees:is_empty(Q4),
    LZ = Len == 0,

    true = E1 or not E3,
    true = E2 or not ED,
    true = ED or not E3,
    true = LZ == (E3 and E4),

    true = Len             >= 0,
    true = PersistentCount >= 0,
    true = RamMsgCount     >= 0,
    true = RamIndexCount   >= 0,

    State.

m(MsgStatus = #msg_status { msg           = Msg,
                            is_persistent = IsPersistent,
                            msg_on_disk   = MsgOnDisk,
                            index_on_disk = IndexOnDisk }) ->
    true = (not IsPersistent) or IndexOnDisk,
    true = (not IndexOnDisk) or MsgOnDisk,
    true = (Msg =/= undefined) or MsgOnDisk,

    MsgStatus.

one_if(true ) -> 1;
one_if(false) -> 0.

cons_if(true,   E, L) -> [E | L];
cons_if(false, _E, L) -> L.

gb_sets_maybe_insert(false, _Val, Set) -> Set;
%% when requeueing, we re-add a msg_id to the unconfirmed set
gb_sets_maybe_insert(true,  Val,  Set) -> gb_sets:add(Val, Set).

q4_from_q1(Q1) ->
    rabbit_misc:queue_fold(fun (#msg_status { seq_id = SeqId } = M, Acc) ->
                               gb_trees:insert(SeqId, M, Acc)
                           end, gb_trees:empty(), Q1).

q4_out_r(Q4) ->
    case gb_trees:is_empty(Q4) of
        true  -> {empty, Q4};
        false -> {_SeqId, MsgStatus, Q4a} = gb_trees:take_largest(Q4),
                 {{value, MsgStatus}, Q4a}
    end.

q4_fold(Fun, Init, Q4) ->
    q4_fold1(Fun, Init, gb_trees:iterator(Q4)).
q4_fold1(Fun, Init, Iter) ->
    case gb_trees:next(Iter) of
        none             -> Init;
        {_Key, M, Iter1} -> q4_fold1(Fun, Fun(M, Init), Iter1)
    end.

msg_status(IsPersistent, SeqId, Msg = #basic_message { id = MsgId },
           MsgProps) ->
    #msg_status { seq_id = SeqId, msg_id = MsgId, msg = Msg,
                  is_persistent = IsPersistent, is_delivered = false,
                  msg_on_disk = false, index_on_disk = false,
                  msg_props = MsgProps }.

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

msg_store_client_init(MsgStore, MsgOnDiskFun, Callback) ->
    msg_store_client_init(MsgStore, rabbit_guid:guid(), MsgOnDiskFun, Callback).

msg_store_client_init(MsgStore, Ref, MsgOnDiskFun, Callback) ->
    CloseFDsFun = msg_store_close_fds_fun(MsgStore =:= ?PERSISTENT_MSG_STORE),
    rabbit_msg_store:client_init(MsgStore, Ref, MsgOnDiskFun,
                                 fun () -> Callback(?MODULE, CloseFDsFun) end).

msg_store_write(MSCState, IsPersistent, MsgId, Msg) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:write(MsgId, Msg, MSCState1) end).

msg_store_read(MSCState, IsPersistent, MsgId) ->
    with_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:read(MsgId, MSCState1) end).

msg_store_remove(MSCState, IsPersistent, MsgIds) ->
    with_immutable_msg_store_state(
      MSCState, IsPersistent,
      fun (MCSState1) -> rabbit_msg_store:remove(MsgIds, MCSState1) end).

msg_store_close_fds(MSCState, IsPersistent) ->
    with_msg_store_state(
      MSCState, IsPersistent,
      fun (MSCState1) -> rabbit_msg_store:close_all_indicated(MSCState1) end).

msg_store_close_fds_fun(IsPersistent) ->
    fun (?MODULE, State = #vqstate { msg_store_clients = MSCState }) ->
            {ok, MSCState1} = msg_store_close_fds(MSCState, IsPersistent),
            State #vqstate { msg_store_clients = MSCState1 }
    end.

maybe_write_delivered(false, _SeqId, IndexState) ->
    IndexState;
maybe_write_delivered(true, SeqId, IndexState) ->
    rabbit_queue_index:deliver([SeqId], IndexState).

betas_from_index_entries(List, TransientThreshold, IndexState) ->
    {Filtered, Delivers, Acks} =
        lists:foldr(
          fun ({MsgId, SeqId, MsgProps, IsPersistent, IsDelivered},
               {Filtered1, Delivers1, Acks1}) ->
                  case SeqId < TransientThreshold andalso not IsPersistent of
                      true  -> {Filtered1,
                                cons_if(not IsDelivered, SeqId, Delivers1),
                                [SeqId | Acks1]};
                      false -> {[m(#msg_status { msg           = undefined,
                                                 msg_id        = MsgId,
                                                 seq_id        = SeqId,
                                                 is_persistent = IsPersistent,
                                                 is_delivered  = IsDelivered,
                                                 msg_on_disk   = true,
                                                 index_on_disk = true,
                                                 msg_props     = MsgProps
                                               }) | Filtered1],
                                Delivers1,
                                Acks1}
                  end
          end, {[], [], []}, List),
    {q3tree:from_batch({true, Filtered}),
     rabbit_queue_index:ack(Acks,
                            rabbit_queue_index:deliver(Delivers, IndexState))}.

%% the first arg is the older delta
combine_deltas(?BLANK_DELTA_PATTERN(X), ?BLANK_DELTA_PATTERN(Y)) ->
    ?BLANK_DELTA;
combine_deltas(?BLANK_DELTA_PATTERN(X), #delta { start_seq_id = Start,
                                                 count        = Count,
                                                 end_seq_id   = End } = B) ->
    true = Start + Count =< End, %% ASSERTION
    B;
combine_deltas(#delta { start_seq_id = Start,
                        count        = Count,
                        end_seq_id   = End } = A, ?BLANK_DELTA_PATTERN(Y)) ->
    true = Start + Count =< End, %% ASSERTION
    A;
combine_deltas(#delta { start_seq_id = StartLow,
                        count        = CountLow,
                        end_seq_id   = EndLow },
               #delta { start_seq_id = StartHigh,
                        count        = CountHigh,
                        end_seq_id   = EndHigh }) ->
    Count = CountLow + CountHigh,
    true = (StartLow =< StartHigh) %% ASSERTIONS
        andalso ((StartLow + CountLow) =< EndLow)
        andalso ((StartHigh + CountHigh) =< EndHigh)
        andalso ((StartLow + Count) =< EndHigh),
    #delta { start_seq_id = StartLow, count = Count, end_seq_id = EndHigh }.

beta_fold(Fun, Init, Q) ->
    q3tree:foldr(fun ({_SeqID, Value}, Acc) -> Fun(Value, Acc) end, Init, Q).

update_rate(Now, Then, Count, {OThen, OCount}) ->
    %% avg over the current period and the previous
    {1000000.0 * (Count + OCount) / timer:now_diff(Now, OThen), {Then, Count}}.

%%----------------------------------------------------------------------------
%% Internal major helpers for Public API
%%----------------------------------------------------------------------------

init(IsDurable, IndexState, DeltaCount, Terms, AsyncCallback,
     PersistentClient, TransientClient) ->
    {LowSeqId, NextSeqId, IndexState1} = rabbit_queue_index:bounds(IndexState),

    DeltaCount1 = proplists:get_value(persistent_count, Terms, DeltaCount),
    Delta = case DeltaCount1 == 0 andalso DeltaCount /= undefined of
                true  -> ?BLANK_DELTA;
                false -> #delta { start_seq_id = LowSeqId,
                                  count        = DeltaCount1,
                                  end_seq_id   = NextSeqId }
            end,
    Now = now(),
    State = #vqstate {
      q1                  = queue:new(),
      q2                  = bpqueue:new(),
      delta               = Delta,
      q3                  = q3tree:new(),
      q4                  = gb_trees:empty(),
      next_seq_id         = NextSeqId,
      pending_ack         = dict:new(),
      ram_ack_index       = gb_trees:empty(),
      index_state         = IndexState1,
      msg_store_clients   = {PersistentClient, TransientClient},
      durable             = IsDurable,
      transient_threshold = NextSeqId,

      async_callback      = AsyncCallback,

      len                 = DeltaCount1,
      persistent_count    = DeltaCount1,

      target_ram_count    = infinity,
      ram_msg_count       = 0,
      ram_msg_count_prev  = 0,
      ram_ack_count_prev  = 0,
      ram_index_count     = 0,
      out_counter         = 0,
      in_counter          = 0,
      rates               = blank_rate(Now, DeltaCount1),
      msgs_on_disk        = gb_sets:new(),
      msg_indices_on_disk = gb_sets:new(),
      unconfirmed         = gb_sets:new(),
      confirmed           = gb_sets:new(),
      ack_out_counter     = 0,
      ack_in_counter      = 0,
      ack_rates           = blank_rate(Now, 0) },
    a(maybe_deltas_to_betas(State)).

blank_rate(Timestamp, IngressLength) ->
    #rates { egress      = {Timestamp, 0},
             ingress     = {Timestamp, IngressLength},
             avg_egress  = 0.0,
             avg_ingress = 0.0,
             timestamp   = Timestamp }.

in_r(MsgStatus = #msg_status { msg = undefined, index_on_disk = IndexOnDisk },
     State = #vqstate { q3 = Q3, q4 = Q4, ram_index_count = RamIndexCount }) ->
    case gb_trees:is_empty(Q4) of
        true  -> State #vqstate {
                   q3              = q3tree:in_r(IndexOnDisk, MsgStatus, Q3),
                   ram_index_count = RamIndexCount + one_if(not IndexOnDisk) };
        false -> {MsgStatus1 = #msg_status{ seq_id = SeqId},
                  State1  = #vqstate { q4 = Q4a }} = read_msg(MsgStatus, State),
                 State1 #vqstate { q4 = gb_trees:insert(SeqId, MsgStatus1, Q4a)}
    end;
in_r(MsgStatus = #msg_status { seq_id = SeqId },
     State = #vqstate { q4 = Q4 }) ->
    State #vqstate { q4 = gb_trees:insert(SeqId, MsgStatus, Q4) }.

queue_out(State = #vqstate { q4 = Q4 }) ->
    case gb_trees:is_empty(Q4) of
        true ->
            case fetch_from_q3(State) of
                {empty, _State1} = Result     -> Result;
                {loaded, {MsgStatus, State1}} -> {{value, MsgStatus}, State1}
            end;
        false -> {_SeqId, MsgStatus, Q4a} = gb_trees:take_smallest(Q4),
                 {{value, MsgStatus}, State #vqstate { q4 = Q4a }}
    end.

read_msg(MsgStatus = #msg_status { msg           = undefined,
                                   msg_id        = MsgId,
                                   is_persistent = IsPersistent },
         State = #vqstate { ram_msg_count     = RamMsgCount,
                            msg_store_clients = MSCState}) ->
    {{ok, Msg = #basic_message {}}, MSCState1} =
        msg_store_read(MSCState, IsPersistent, MsgId),
    {MsgStatus #msg_status { msg = Msg },
     State #vqstate { ram_msg_count     = RamMsgCount + 1,
                      msg_store_clients = MSCState1 }};
read_msg(MsgStatus, State) ->
    {MsgStatus, State}.

internal_fetch(AckRequired, MsgStatus = #msg_status {
                              seq_id        = SeqId,
                              msg_id        = MsgId,
                              msg           = Msg,
                              is_persistent = IsPersistent,
                              is_delivered  = IsDelivered,
                              msg_on_disk   = MsgOnDisk,
                              index_on_disk = IndexOnDisk },
               State = #vqstate {ram_msg_count     = RamMsgCount,
                                 out_counter       = OutCount,
                                 index_state       = IndexState,
                                 msg_store_clients = MSCState,
                                 len               = Len,
                                 persistent_count  = PCount }) ->
    %% 1. Mark it delivered if necessary
    IndexState1 = maybe_write_delivered(
                    IndexOnDisk andalso not IsDelivered,
                    SeqId, IndexState),

    %% 2. Remove from msg_store and queue index, if necessary
    Rem = fun () ->
                  ok = msg_store_remove(MSCState, IsPersistent, [MsgId])
          end,
    Ack = fun () -> rabbit_queue_index:ack([SeqId], IndexState1) end,
    IndexState2 =
        case {AckRequired, MsgOnDisk, IndexOnDisk, IsPersistent} of
            {false, true, false,     _} -> Rem(), IndexState1;
            {false, true,  true,     _} -> Rem(), Ack();
            { true, true,  true, false} -> Ack();
            _                           -> IndexState1
        end,

    %% 3. If an ack is required, add something sensible to PA
    {AckTag, State1} = case AckRequired of
                           true  -> StateN = record_pending_ack(
                                               MsgStatus #msg_status {
                                                 is_delivered = true }, State),
                                    {SeqId, StateN};
                           false -> {undefined, State}
                       end,

    PCount1 = PCount - one_if(IsPersistent andalso not AckRequired),
    Len1 = Len - 1,
    RamMsgCount1 = RamMsgCount - one_if(Msg =/= undefined),

    {{Msg, IsDelivered, AckTag, Len1},
     State1 #vqstate { ram_msg_count    = RamMsgCount1,
                       out_counter      = OutCount + 1,
                       index_state      = IndexState2,
                       len              = Len1,
                       persistent_count = PCount1 }}.

purge_betas_and_deltas(LensByStore,
                       State = #vqstate { q3                = Q3,
                                          index_state       = IndexState,
                                          msg_store_clients = MSCState }) ->
    case q3tree:is_empty(Q3) of
        true  -> {LensByStore, State};
        false -> {LensByStore1, IndexState1} =
                     remove_queue_entries(fun beta_fold/3, Q3,
                                          LensByStore, IndexState, MSCState),
                 purge_betas_and_deltas(LensByStore1,
                                        maybe_deltas_to_betas(
                                          State #vqstate {
                                            q3          = q3tree:new(),
                                            index_state = IndexState1 }))
    end.

remove_queue_entries(Fold, Q, LensByStore, IndexState, MSCState) ->
    {MsgIdsByStore, Delivers, Acks} =
        Fold(fun remove_queue_entries1/2, {orddict:new(), [], []}, Q),
    ok = orddict:fold(fun (IsPersistent, MsgIds, ok) ->
                              msg_store_remove(MSCState, IsPersistent, MsgIds)
                      end, ok, MsgIdsByStore),
    {sum_msg_ids_by_store_to_len(LensByStore, MsgIdsByStore),
     rabbit_queue_index:ack(Acks,
                            rabbit_queue_index:deliver(Delivers, IndexState))}.

remove_queue_entries1(
  #msg_status { msg_id = MsgId, seq_id = SeqId,
                is_delivered = IsDelivered, msg_on_disk = MsgOnDisk,
                index_on_disk = IndexOnDisk, is_persistent = IsPersistent },
  {MsgIdsByStore, Delivers, Acks}) ->
    {case MsgOnDisk of
         true  -> rabbit_misc:orddict_cons(IsPersistent, MsgId, MsgIdsByStore);
         false -> MsgIdsByStore
     end,
     cons_if(IndexOnDisk andalso not IsDelivered, SeqId, Delivers),
     cons_if(IndexOnDisk, SeqId, Acks)}.

sum_msg_ids_by_store_to_len(LensByStore, MsgIdsByStore) ->
    orddict:fold(
      fun (IsPersistent, MsgIds, LensByStore1) ->
              orddict:update_counter(IsPersistent, length(MsgIds), LensByStore1)
      end, LensByStore, MsgIdsByStore).

%%----------------------------------------------------------------------------
%% Internal gubbins for publishing
%%----------------------------------------------------------------------------

publish(Msg = #basic_message { is_persistent = IsPersistent, id = MsgId },
        MsgProps = #message_properties { needs_confirming = NeedsConfirming },
        IsDelivered, MsgOnDisk,
        State = #vqstate { q1 = Q1, q3 = Q3, q4 = Q4,
                           next_seq_id      = SeqId,
                           len              = Len,
                           in_counter       = InCount,
                           persistent_count = PCount,
                           durable          = IsDurable,
                           ram_msg_count    = RamMsgCount,
                           unconfirmed      = UC }) ->
    IsPersistent1 = IsDurable andalso IsPersistent,
    MsgStatus = (msg_status(IsPersistent1, SeqId, Msg, MsgProps))
        #msg_status { is_delivered = IsDelivered, msg_on_disk = MsgOnDisk},
    {MsgStatus1, State1} = maybe_write_to_disk(false, false, MsgStatus, State),
    State2 = case q3tree:is_empty(Q3) of
                 false -> State1 #vqstate { q1 = queue:in(m(MsgStatus1), Q1) };
                 true  -> State1 #vqstate { q4 = gb_trees:insert(SeqId,
                                                          m(MsgStatus1), Q4) }
             end,
    PCount1 = PCount + one_if(IsPersistent1),
    UC1 = gb_sets_maybe_insert(NeedsConfirming, MsgId, UC),
    {SeqId, State2 #vqstate { next_seq_id      = SeqId   + 1,
                              len              = Len     + 1,
                              in_counter       = InCount + 1,
                              persistent_count = PCount1,
                              ram_msg_count    = RamMsgCount + 1,
                              unconfirmed      = UC1 }}.

maybe_write_msg_to_disk(_Force, MsgStatus = #msg_status {
                                  msg_on_disk = true }, _MSCState) ->
    MsgStatus;
maybe_write_msg_to_disk(Force, MsgStatus = #msg_status {
                                 msg = Msg, msg_id = MsgId,
                                 is_persistent = IsPersistent }, MSCState)
  when Force orelse IsPersistent ->
    Msg1 = Msg #basic_message {
             %% don't persist any recoverable decoded properties
             content = rabbit_binary_parser:clear_decoded_content(
                         Msg #basic_message.content)},
    ok = msg_store_write(MSCState, IsPersistent, MsgId, Msg1),
    MsgStatus #msg_status { msg_on_disk = true };
maybe_write_msg_to_disk(_Force, MsgStatus, _MSCState) ->
    MsgStatus.

maybe_write_index_to_disk(_Force, MsgStatus = #msg_status {
                                    index_on_disk = true }, IndexState) ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    {MsgStatus, IndexState};
maybe_write_index_to_disk(Force, MsgStatus = #msg_status {
                                   msg_id        = MsgId,
                                   seq_id        = SeqId,
                                   is_persistent = IsPersistent,
                                   is_delivered  = IsDelivered,
                                   msg_props     = MsgProps}, IndexState)
  when Force orelse IsPersistent ->
    true = MsgStatus #msg_status.msg_on_disk, %% ASSERTION
    IndexState1 = rabbit_queue_index:publish(
                    MsgId, SeqId, MsgProps, IsPersistent, IndexState),
    {MsgStatus #msg_status { index_on_disk = true },
     maybe_write_delivered(IsDelivered, SeqId, IndexState1)};
maybe_write_index_to_disk(_Force, MsgStatus, IndexState) ->
    {MsgStatus, IndexState}.

maybe_write_to_disk(ForceMsg, ForceIndex, MsgStatus,
                    State = #vqstate { index_state       = IndexState,
                                       msg_store_clients = MSCState }) ->
    MsgStatus1 = maybe_write_msg_to_disk(ForceMsg, MsgStatus, MSCState),
    {MsgStatus2, IndexState1} =
        maybe_write_index_to_disk(ForceIndex, MsgStatus1, IndexState),
    {MsgStatus2, State #vqstate { index_state = IndexState1 }}.

%%----------------------------------------------------------------------------
%% Internal gubbins for acks
%%----------------------------------------------------------------------------

record_pending_ack(#msg_status { seq_id        = SeqId,
                                 msg_id        = MsgId,
                                 is_persistent = IsPersistent,
                                 msg_on_disk   = MsgOnDisk,
                                 msg_props     = MsgProps } = MsgStatus,
                   State = #vqstate { pending_ack     = PA,
                                      ram_ack_index   = RAI,
                                      ack_in_counter  = AckInCount}) ->
    {AckEntry, RAI1} =
        case MsgOnDisk of
            true  -> {{IsPersistent, MsgId, MsgProps}, RAI};
            false -> {MsgStatus, gb_trees:insert(SeqId, MsgId, RAI)}
        end,
    PA1 = dict:store(SeqId, AckEntry, PA),
    State #vqstate { pending_ack    = PA1,
                     ram_ack_index  = RAI1,
                     ack_in_counter = AckInCount + 1}.

remove_pending_ack(KeepPersistent,
                   State = #vqstate { pending_ack       = PA,
                                      index_state       = IndexState,
                                      msg_store_clients = MSCState }) ->
    {PersistentSeqIds, MsgIdsByStore, _AllMsgIds} =
        dict:fold(fun accumulate_ack/3, accumulate_ack_init(), PA),
    State1 = State #vqstate { pending_ack   = dict:new(),
                              ram_ack_index = gb_trees:empty() },
    case KeepPersistent of
        true  -> case orddict:find(false, MsgIdsByStore) of
                     error        -> State1;
                     {ok, MsgIds} -> ok = msg_store_remove(MSCState, false,
                                                           MsgIds),
                                    State1
                 end;
        false -> IndexState1 =
                     rabbit_queue_index:ack(PersistentSeqIds, IndexState),
                 [ok = msg_store_remove(MSCState, IsPersistent, MsgIds)
                  || {IsPersistent, MsgIds} <- orddict:to_list(MsgIdsByStore)],
                 State1 #vqstate { index_state = IndexState1 }
    end.

ack(_MsgStoreFun, _Fun, [], State) ->
    {[], State};
ack(MsgStoreFun, Fun, AckTags, State) ->
    {{PersistentSeqIds, MsgIdsByStore, AllMsgIds},
     State1 = #vqstate { index_state       = IndexState,
                         msg_store_clients = MSCState,
                         persistent_count  = PCount,
                         ack_out_counter   = AckOutCount }} =
        lists:foldl(
          fun (SeqId, {Acc, State2 = #vqstate { pending_ack   = PA,
                                                ram_ack_index = RAI }}) ->
                  AckEntry = dict:fetch(SeqId, PA),
                  {accumulate_ack(SeqId, AckEntry, Acc),
                   Fun(AckEntry, State2 #vqstate {
                                   pending_ack   = dict:erase(SeqId, PA),
                                   ram_ack_index =
                                       gb_trees:delete_any(SeqId, RAI)})}
          end, {accumulate_ack_init(), State}, AckTags),
    IndexState1 = rabbit_queue_index:ack(PersistentSeqIds, IndexState),
    [ok = MsgStoreFun(MSCState, IsPersistent, MsgIds)
     || {IsPersistent, MsgIds} <- orddict:to_list(MsgIdsByStore)],
    PCount1 = PCount - find_persistent_count(sum_msg_ids_by_store_to_len(
                                               orddict:new(), MsgIdsByStore)),
    {lists:reverse(AllMsgIds),
     State1 #vqstate { index_state      = IndexState1,
                       persistent_count = PCount1,
                       ack_out_counter  = AckOutCount + length(AckTags) }}.

accumulate_ack_init() -> {[], orddict:new(), []}.

accumulate_ack(_SeqId, #msg_status { is_persistent = false, %% ASSERTIONS
                                     msg_on_disk   = false,
                                     index_on_disk = false,
                                     msg_id        = MsgId },
              {PersistentSeqIdsAcc, MsgIdsByStore, AllMsgIds}) ->
    {PersistentSeqIdsAcc, MsgIdsByStore, [MsgId | AllMsgIds]};
accumulate_ack(SeqId, {IsPersistent, MsgId, _MsgProps},
               {PersistentSeqIdsAcc, MsgIdsByStore, AllMsgIds}) ->
    {cons_if(IsPersistent, SeqId, PersistentSeqIdsAcc),
     rabbit_misc:orddict_cons(IsPersistent, MsgId, MsgIdsByStore),
     [MsgId | AllMsgIds]}.

find_persistent_count(LensByStore) ->
    case orddict:find(true, LensByStore) of
        error     -> 0;
        {ok, Len} -> Len
    end.

%%----------------------------------------------------------------------------
%% Internal plumbing for confirms (aka publisher acks)
%%----------------------------------------------------------------------------

confirm_commit_index(State = #vqstate { index_state = IndexState }) ->
    case needs_index_sync(State) of
        true  -> State #vqstate {
                   index_state = rabbit_queue_index:sync(IndexState) };
        false -> State
    end.

record_confirms(MsgIdSet, State = #vqstate { msgs_on_disk        = MOD,
                                             msg_indices_on_disk = MIOD,
                                             unconfirmed         = UC,
                                             confirmed           = C }) ->
    State #vqstate { msgs_on_disk        = gb_sets:difference(MOD,  MsgIdSet),
                     msg_indices_on_disk = gb_sets:difference(MIOD, MsgIdSet),
                     unconfirmed         = gb_sets:difference(UC,   MsgIdSet),
                     confirmed           = gb_sets:union     (C,    MsgIdSet) }.

needs_index_sync(#vqstate { msg_indices_on_disk = MIOD,
                            unconfirmed = UC }) ->
    %% If UC is empty then by definition, MIOD and MOD are also empty
    %% and there's nothing that can be pending a sync.

    %% If UC is not empty, then we want to find is_empty(UC - MIOD),
    %% but the subtraction can be expensive. Thus instead, we test to
    %% see if UC is a subset of MIOD. This can only be the case if
    %% MIOD == UC, which would indicate that every message in UC is
    %% also in MIOD and is thus _all_ pending on a msg_store sync, not
    %% on a qi sync. Thus the negation of this is sufficient. Because
    %% is_subset is short circuiting, this is more efficient than the
    %% subtraction.
    not (gb_sets:is_empty(UC) orelse gb_sets:is_subset(UC, MIOD)).

blind_confirm(Callback, MsgIdSet) ->
    Callback(?MODULE,
             fun (?MODULE, State) -> record_confirms(MsgIdSet, State) end).

msgs_written_to_disk(Callback, MsgIdSet, removed) ->
    blind_confirm(Callback, MsgIdSet);
msgs_written_to_disk(Callback, MsgIdSet, written) ->
    Callback(?MODULE,
             fun (?MODULE, State = #vqstate { msgs_on_disk        = MOD,
                                              msg_indices_on_disk = MIOD,
                                              unconfirmed         = UC }) ->
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

%%----------------------------------------------------------------------------
%% Phase changes
%%----------------------------------------------------------------------------

%% Determine whether a reduction in memory use is necessary, and call
%% functions to perform the required phase changes. The function can
%% also be used to just do the former, by passing in dummy phase
%% change functions.
%%
%% The function does not report on any needed beta->delta conversions,
%% though the conversion function for that is called as necessary. The
%% reason is twofold. Firstly, this is safe because the conversion is
%% only ever necessary just after a transition to a
%% target_ram_count of zero or after an incremental alpha->beta
%% conversion. In the former case the conversion is performed straight
%% away (i.e. any betas present at the time are converted to deltas),
%% and in the latter case the need for a conversion is flagged up
%% anyway. Secondly, this is necessary because we do not have a
%% precise and cheap predicate for determining whether a beta->delta
%% conversion is necessary - due to the complexities of retaining up
%% one segment's worth of messages in q3 - and thus would risk
%% perpetually reporting the need for a conversion when no such
%% conversion is needed. That in turn could cause an infinite loop.
reduce_memory_use(_AlphaBetaFun, _BetaGammaFun, _BetaDeltaFun, _AckFun,
                  State = #vqstate {target_ram_count = infinity}) ->
    {false, State};
reduce_memory_use(AlphaBetaFun, BetaGammaFun, BetaDeltaFun, AckFun,
                  State = #vqstate {
                    ram_ack_index    = RamAckIndex,
                    ram_msg_count    = RamMsgCount,
                    target_ram_count = TargetRamCount,
                    rates            = #rates { avg_ingress = AvgIngress,
                                                avg_egress  = AvgEgress },
                    ack_rates        = #rates { avg_ingress = AvgAckIngress,
                                                avg_egress  = AvgAckEgress }
                   }) ->

    {Reduce, State1} =
        case chunk_size(RamMsgCount + gb_trees:size(RamAckIndex),
                        TargetRamCount) of
            0  -> {false, State};
            %% Reduce memory of pending acks and alphas. The order is
            %% determined based on which is growing faster. Whichever
            %% comes second may very well get a quota of 0 if the
            %% first manages to push out the max number of messages.
            S1 -> {_, State2} =
                      lists:foldl(fun (ReduceFun, {QuotaN, StateN}) ->
                                          ReduceFun(QuotaN, StateN)
                                  end,
                                  {S1, State},
                                  case (AvgAckIngress - AvgAckEgress) >
                                      (AvgIngress - AvgEgress) of
                                      true  -> [AckFun, AlphaBetaFun];
                                      false -> [AlphaBetaFun, AckFun]
                                  end),
                  {true, State2}
        end,

    case State1 #vqstate.target_ram_count of
        0 -> {Reduce, BetaDeltaFun(State1)};
        _ -> case chunk_size(State1 #vqstate.ram_index_count,
                             permitted_ram_index_count(State1)) of
                 ?IO_BATCH_SIZE = S2 -> {true, BetaGammaFun(S2, State1)};
                 _                   -> {Reduce, State1}
             end
    end.

limit_ram_acks(0, State) ->
    {0, State};
limit_ram_acks(Quota, State = #vqstate { pending_ack   = PA,
                                         ram_ack_index = RAI }) ->
    case gb_trees:is_empty(RAI) of
        true ->
            {Quota, State};
        false ->
            {SeqId, MsgId, RAI1} = gb_trees:take_largest(RAI),
            MsgStatus = #msg_status {
              msg_id        = MsgId, %% ASSERTION
              is_persistent = false, %% ASSERTION
              msg_props     = MsgProps } = dict:fetch(SeqId, PA),
            {_, State1} = maybe_write_to_disk(true, false, MsgStatus, State),
            PA1 = dict:store(SeqId, {false, MsgId, MsgProps}, PA),
            limit_ram_acks(Quota - 1,
                           State1 #vqstate { pending_ack   = PA1,
                                             ram_ack_index = RAI1 })
    end.


reduce_memory_use(State) ->
    {_, State1} = reduce_memory_use(fun push_alphas_to_betas/2,
                                    fun limit_ram_index/2,
                                    fun push_betas_to_deltas/1,
                                    fun limit_ram_acks/2,
                                    State),
    State1.

limit_ram_index(Quota, State = #vqstate { q2 = Q2, q3 = Q3,
                                          index_state = IndexState,
                                          ram_index_count = RamIndexCount }) ->
    {Q2a, {Quota1, IndexState1}} = limit_ram_index(
                                     fun bpqueue:map_fold_filter_r/4,
                                     Q2, {Quota, IndexState}),
    %% TODO: we shouldn't be writing index entries for messages that
    %% can never end up in delta due them residing in the only segment
    %% held by q3.
    {Q3a, {Quota2, IndexState2}} = limit_ram_index(
                                     fun q3tree:map_fold_filter_r/4,
                                     Q3, {Quota1, IndexState1}),
    State #vqstate { q2 = Q2a, q3 = Q3a,
                     index_state = IndexState2,
                     ram_index_count = RamIndexCount - (Quota - Quota2) }.

limit_ram_index(_MapFoldFilterFun, Q, {0, IndexState}) ->
    {Q, {0, IndexState}};
limit_ram_index(MapFoldFilterFun, Q, {Quota, IndexState}) ->
    MapFoldFilterFun(
      fun erlang:'not'/1,
      fun (MsgStatus, {0, _IndexStateN}) ->
              false = MsgStatus #msg_status.index_on_disk, %% ASSERTION
              stop;
          (MsgStatus, {N, IndexStateN}) when N > 0 ->
              false = MsgStatus #msg_status.index_on_disk, %% ASSERTION
              {MsgStatus1, IndexStateN1} =
                  maybe_write_index_to_disk(true, MsgStatus, IndexStateN),
              {true, m(MsgStatus1), {N-1, IndexStateN1}}
      end, {Quota, IndexState}, Q).

permitted_ram_index_count(#vqstate { len = 0 }) ->
    infinity;
permitted_ram_index_count(#vqstate { len   = Len,
                                     q2    = Q2,
                                     q3    = Q3,
                                     delta = #delta { count = DeltaCount } }) ->
    BetaLen = bpqueue:len(Q2) + q3tree:len(Q3),
    BetaLen - trunc(BetaLen * BetaLen / (Len - DeltaCount)).

chunk_size(Current, Permitted)
  when Permitted =:= infinity orelse Permitted >= Current ->
    0;
chunk_size(Current, Permitted) ->
    lists:min([Current - Permitted, ?IO_BATCH_SIZE]).

fetch_from_q3(State = #vqstate {
                q1                = Q1,
                q2                = Q2,
                delta             = #delta { count = DeltaCount },
                q3                = Q3,
                q4                = Q4,
                ram_index_count   = RamIndexCount}) ->
    case q3tree:out(Q3) of
        {empty, _Q3} ->
            {empty, State};
        {{value, IndexOnDisk, MsgStatus}, Q3a} ->
            RamIndexCount1 = RamIndexCount - one_if(not IndexOnDisk),
            true = RamIndexCount1 >= 0, %% ASSERTION
            State1 = State #vqstate { q3              = Q3a,
                                      ram_index_count = RamIndexCount1 },
            State2 =
                case {q3tree:is_empty(Q3a), 0 == DeltaCount} of
                    {true, true} ->
                        %% q3 is now empty, it wasn't before; delta is
                        %% still empty. So q2 must be empty, and we
                        %% know q4 is empty otherwise we wouldn't be
                        %% loading from q3. As such, we can just transform
                        %% q1 to q4.
                        true = bpqueue:is_empty(Q2),  %% ASSERTION
                        true = gb_trees:is_empty(Q4), %% ASSERTION
                        State1 #vqstate { q1 = queue:new(),
                                          q4 = q4_from_q1(Q1) };
                    {true, false} ->
                        maybe_deltas_to_betas(State1);
                    {false, _} ->
                        %% q3 still isn't empty, we've not touched
                        %% delta, so the invariants between q1, q2,
                        %% delta and q3 are maintained
                        State1
                end,
            {loaded, {MsgStatus, State2}}
    end.

maybe_deltas_to_betas(State = #vqstate { delta = ?BLANK_DELTA_PATTERN(X) }) ->
    State;
maybe_deltas_to_betas(State = #vqstate {
                        q2                   = Q2,
                        delta                = Delta,
                        q3                   = Q3,
                        index_state          = IndexState,
                        transient_threshold  = TransientThreshold }) ->
    #delta { start_seq_id = DeltaSeqId,
             count        = DeltaCount,
             end_seq_id   = DeltaSeqIdEnd } = Delta,
    DeltaSeqId1 =
        lists:min([rabbit_queue_index:next_segment_boundary(DeltaSeqId),
                   DeltaSeqIdEnd]),
    {List, IndexState1} =
        rabbit_queue_index:read(DeltaSeqId, DeltaSeqId1, IndexState),
    {Q3a, IndexState2} =
        betas_from_index_entries(List, TransientThreshold, IndexState1),
    State1 = State #vqstate { index_state = IndexState2 },
    case q3tree:len(Q3a) of
        0 ->
            %% we ignored every message in the segment due to it being
            %% transient and below the threshold
            maybe_deltas_to_betas(
              State1 #vqstate {
                delta = Delta #delta { start_seq_id = DeltaSeqId1 }});
        Q3aLen ->
            Q3b = q3tree:join(Q3, Q3a),
            case DeltaCount - Q3aLen of
                0 ->
                    %% delta is now empty, but it wasn't before, so
                    %% can now join q2 onto q3
                    State1 #vqstate { q2    = bpqueue:new(),
                                      delta = ?BLANK_DELTA,
                                      q3    = q3tree:join_bpqueue(Q3b, Q2) };
                N when N > 0 ->
                    Delta1 = #delta { start_seq_id = DeltaSeqId1,
                                      count        = N,
                                      end_seq_id   = DeltaSeqIdEnd },
                    State1 #vqstate { delta = Delta1,
                                      q3    = Q3b }
            end
    end.

push_alphas_to_betas(Quota, State) ->
    {Quota1, State1} = maybe_push_q1_to_betas(Quota,  State),
    {Quota2, State2} = maybe_push_q4_to_betas(Quota1, State1),
    {Quota2, State2}.

maybe_push_q1_to_betas(Quota, State = #vqstate { q1 = Q1 }) ->
    maybe_push_alphas_to_betas(
      fun queue:out/1,
      fun (MsgStatus = #msg_status { index_on_disk = IndexOnDisk },
           Q1a, State1 = #vqstate { q3 = Q3, delta = #delta { count = 0 } }) ->
              State1 #vqstate { q1 = Q1a,
                                q3 = q3tree:in(IndexOnDisk, MsgStatus, Q3) };
          (MsgStatus = #msg_status { index_on_disk = IndexOnDisk },
           Q1a, State1 = #vqstate { q2 = Q2 }) ->
              State1 #vqstate { q1 = Q1a,
                                q2 = bpqueue:in(IndexOnDisk, MsgStatus, Q2) }
      end, Quota, Q1, State).

maybe_push_q4_to_betas(Quota, State = #vqstate { q4 = Q4 }) ->
    maybe_push_alphas_to_betas(
      fun q4_out_r/1,
      fun (MsgStatus = #msg_status { index_on_disk = IndexOnDisk },
           Q4a, State1 = #vqstate { q3 = Q3 }) ->
              State1 #vqstate { q3 = q3tree:in_r(IndexOnDisk, MsgStatus, Q3),
                                q4 = Q4a }
      end, Quota, Q4, State).

maybe_push_alphas_to_betas(_Generator, _Consumer, Quota, _Q,
                           State = #vqstate {
                             ram_msg_count    = RamMsgCount,
                             target_ram_count = TargetRamCount })
  when Quota =:= 0 orelse
       TargetRamCount =:= infinity orelse
       TargetRamCount >= RamMsgCount ->
    {Quota, State};
maybe_push_alphas_to_betas(Generator, Consumer, Quota, Q, State) ->
    case Generator(Q) of
        {empty, _Q} ->
            {Quota, State};
        {{value, MsgStatus}, Qa} ->
            {MsgStatus1 = #msg_status { msg_on_disk = true,
                                        index_on_disk = IndexOnDisk },
             State1 = #vqstate { ram_msg_count   = RamMsgCount,
                                 ram_index_count = RamIndexCount }} =
                maybe_write_to_disk(true, false, MsgStatus, State),
            MsgStatus2 = m(MsgStatus1 #msg_status { msg = undefined }),
            RamIndexCount1 = RamIndexCount + one_if(not IndexOnDisk),
            State2 = State1 #vqstate { ram_msg_count = RamMsgCount - 1,
                                       ram_index_count = RamIndexCount1 },
            maybe_push_alphas_to_betas(Generator, Consumer, Quota - 1, Qa,
                                       Consumer(MsgStatus2, Qa, State2))
    end.

push_betas_to_deltas(State = #vqstate { q2              = Q2,
                                        delta           = Delta,
                                        q3              = Q3,
                                        index_state     = IndexState,
                                        ram_index_count = RamIndexCount }) ->
    {Delta2, Q2a, RamIndexCount2, IndexState2} =
        push_betas_to_deltas(fun (Q2MinSeqId) -> Q2MinSeqId end,
                             fun bpqueue:out/1, bpqueue, Q2,
                             RamIndexCount, IndexState),
    {Delta3, Q3a, RamIndexCount3, IndexState3} =
        push_betas_to_deltas(fun rabbit_queue_index:next_segment_boundary/1,
                             fun q3tree:out_r/1, q3tree, Q3,
                             RamIndexCount2, IndexState2),
    Delta4 = combine_deltas(Delta3, combine_deltas(Delta, Delta2)),
    State #vqstate { q2              = Q2a,
                     delta           = Delta4,
                     q3              = Q3a,
                     index_state     = IndexState3,
                     ram_index_count = RamIndexCount3 }.

push_betas_to_deltas(LimitFun, Generator, QMod, Q, RamIndexCount, IndexState) ->
    case QMod:out(Q) of
        {empty, _Q} ->
            {?BLANK_DELTA, Q, RamIndexCount, IndexState};
        {{value, _IndexOnDisk1, #msg_status { seq_id = MinSeqId }}, _Qa} ->
            {{value, _IndexOnDisk2, #msg_status { seq_id = MaxSeqId }}, _Qb} =
                QMod:out_r(Q),
            Limit = LimitFun(MinSeqId),
            case MaxSeqId < Limit of
                true  -> {?BLANK_DELTA, Q, RamIndexCount, IndexState};
                false -> {Len, Qc, RamIndexCount1, IndexState1} =
                             push_betas_to_deltas1(Generator, Limit, Q, 0,
                                                   RamIndexCount, IndexState),
                         {#delta { start_seq_id = Limit,
                                   count        = Len,
                                   end_seq_id   = MaxSeqId + 1 },
                          Qc, RamIndexCount1, IndexState1}
            end
    end.

push_betas_to_deltas1(Generator, Limit, Q, Count, RamIndexCount, IndexState) ->
    case Generator(Q) of
        {empty, _Q} ->
            {Count, Q, RamIndexCount, IndexState};
        {{value, _IndexOnDisk, #msg_status { seq_id = SeqId }}, _Qa}
          when SeqId < Limit ->
            {Count, Q, RamIndexCount, IndexState};
        {{value, IndexOnDisk, MsgStatus}, Qa} ->
            {RamIndexCount1, IndexState1} =
                case IndexOnDisk of
                    true  -> {RamIndexCount, IndexState};
                    false -> {#msg_status { index_on_disk = true },
                              IndexState2} =
                                 maybe_write_index_to_disk(true, MsgStatus,
                                                           IndexState),
                             {RamIndexCount - 1, IndexState2}
                end,
            push_betas_to_deltas1(
              Generator, Limit, Qa, Count + 1, RamIndexCount1, IndexState1)
    end.

%%----------------------------------------------------------------------------
%% Upgrading
%%----------------------------------------------------------------------------

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

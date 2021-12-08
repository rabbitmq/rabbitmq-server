%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2010-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mirror_queue_master).

-export([init/3, terminate/2, delete_and_terminate/2,
         purge/1, purge_acks/1, publish/6, publish_delivered/5,
         batch_publish/4, batch_publish_delivered/4,
         discard/4, fetch/2, drop/2, ack/2, requeue/2, ackfold/4, fold/3,
         len/1, is_empty/1, depth/1, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, set_ram_duration_target/2, ram_duration/1,
         needs_timeout/1, timeout/1, handle_pre_hibernate/1, resume/1,
         msg_rates/1, info/2, invoke/3, is_duplicate/2, set_queue_mode/2,
         zip_msgs_and_acks/4, handle_info/2]).

-export([start/2, stop/1, delete_crashed/1]).

-export([promote_backing_queue_state/8, sender_death_fun/0, depth_fun/0]).

-export([init_with_existing_bq/3, stop_mirroring/1, sync_mirrors/3]).

-behaviour(rabbit_backing_queue).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-record(state, { name,
                 gm,
                 coordinator,
                 backing_queue,
                 backing_queue_state,
                 seen_status,
                 confirmed,
                 known_senders,
                 wait_timeout
               }).

-export_type([death_fun/0, depth_fun/0, stats_fun/0]).

-type death_fun() :: fun ((pid()) -> 'ok').
-type depth_fun() :: fun (() -> 'ok').
-type stats_fun() :: fun ((any()) -> 'ok').
-type master_state() :: #state { name                :: rabbit_amqqueue:name(),
                                 gm                  :: pid(),
                                 coordinator         :: pid(),
                                 backing_queue       :: atom(),
                                 backing_queue_state :: any(),
                                 seen_status         :: map(),
                                 confirmed           :: [rabbit_guid:guid()],
                                 known_senders       :: sets:set()
                               }.

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------

-spec start(_, _) -> no_return().
start(_Vhost, _DurableQueues) ->
    %% This will never get called as this module will never be
    %% installed as the default BQ implementation.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

-spec stop(_) -> no_return().
stop(_Vhost) ->
    %% Same as start/1.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

-spec delete_crashed(_) -> no_return().
delete_crashed(_QName) ->
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

init(Q, Recover, AsyncCallback) ->
    {ok, BQ} = application:get_env(backing_queue_module),
    BQS = BQ:init(Q, Recover, AsyncCallback),
    State = #state{gm = GM} = init_with_existing_bq(Q, BQ, BQS),
    ok = gm:broadcast(GM, {depth, BQ:depth(BQS)}),
    State.

-spec init_with_existing_bq(amqqueue:amqqueue(), atom(), any()) ->
          master_state().

init_with_existing_bq(Q0, BQ, BQS) when ?is_amqqueue(Q0) ->
    QName = amqqueue:get_name(Q0),
    case rabbit_mirror_queue_coordinator:start_link(
       Q0, undefined, sender_death_fun(), depth_fun()) of
    {ok, CPid} ->
        GM = rabbit_mirror_queue_coordinator:get_gm(CPid),
        Self = self(),
        Fun = fun () ->
                  [Q1] = mnesia:read({rabbit_queue, QName}),
                  true = amqqueue:is_amqqueue(Q1),
                  GMPids0 = amqqueue:get_gm_pids(Q1),
                  GMPids1 = [{GM, Self} | GMPids0],
                  Q2 = amqqueue:set_gm_pids(Q1, GMPids1),
                  Q3 = amqqueue:set_state(Q2, live),
                  %% amqqueue migration:
                  %% The amqqueue was read from this transaction, no
                  %% need to handle migration.
                  ok = rabbit_amqqueue:store_queue(Q3)
              end,
        ok = rabbit_misc:execute_mnesia_transaction(Fun),
        {_MNode, SNodes} = rabbit_mirror_queue_misc:suggested_queue_nodes(Q0),
        %% We need synchronous add here (i.e. do not return until the
        %% mirror is running) so that when queue declaration is finished
        %% all mirrors are up; we don't want to end up with unsynced mirrors
        %% just by declaring a new queue. But add can't be synchronous all
        %% the time as it can be called by mirrors and that's
        %% deadlock-prone.
        rabbit_mirror_queue_misc:add_mirrors(QName, SNodes, sync),
        #state{name                = QName,
               gm                  = GM,
               coordinator         = CPid,
               backing_queue       = BQ,
               backing_queue_state = BQS,
               seen_status         = #{},
               confirmed           = [],
               known_senders       = sets:new(),
               wait_timeout        = rabbit_misc:get_env(rabbit, slave_wait_timeout, 15000)};
    {error, Reason} ->
        %% The GM can shutdown before the coordinator has started up
        %% (lost membership or missing group), thus the start_link of
        %% the coordinator returns {error, shutdown} as rabbit_amqqueue_process
        % is trapping exists
        throw({coordinator_not_started, Reason})
    end.

-spec stop_mirroring(master_state()) -> {atom(), any()}.

stop_mirroring(State = #state { coordinator         = CPid,
                                backing_queue       = BQ,
                                backing_queue_state = BQS }) ->
    unlink(CPid),
    stop_all_slaves(shutdown, State),
    {BQ, BQS}.

-spec sync_mirrors(stats_fun(), stats_fun(), master_state()) ->
          {'ok', master_state()} | {stop, any(), master_state()}.

sync_mirrors(HandleInfo, EmitStats,
             State = #state { name                = QName,
                              gm                  = GM,
                              backing_queue       = BQ,
                              backing_queue_state = BQS }) ->
    Log = fun (Fmt, Params) ->
                  rabbit_mirror_queue_misc:log_info(
                    QName, "Synchronising: " ++ Fmt ++ "", Params)
          end,
    Log("~p messages to synchronise", [BQ:len(BQS)]),
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    SPids = amqqueue:get_slave_pids(Q),
    SyncBatchSize = rabbit_mirror_queue_misc:sync_batch_size(Q),
    SyncThroughput = rabbit_mirror_queue_misc:default_max_sync_throughput(),
    log_mirror_sync_config(Log, SyncBatchSize, SyncThroughput),
    Ref = make_ref(),
    Syncer = rabbit_mirror_queue_sync:master_prepare(Ref, QName, Log, SPids),
    gm:broadcast(GM, {sync_start, Ref, Syncer, SPids}),
    S = fun(BQSN) -> State#state{backing_queue_state = BQSN} end,
    case rabbit_mirror_queue_sync:master_go(
           Syncer, Ref, Log, HandleInfo, EmitStats, SyncBatchSize, SyncThroughput, BQ, BQS) of
        {cancelled, BQS1}      -> Log(" synchronisation cancelled ", []),
                                  {ok, S(BQS1)};
        {shutdown,  R, BQS1}   -> {stop, R, S(BQS1)};
        {sync_died, R, BQS1}   -> Log("~p", [R]),
                                  {ok, S(BQS1)};
        {already_synced, BQS1} -> {ok, S(BQS1)};
        {ok, BQS1}             -> Log("complete", []),
                                  {ok, S(BQS1)}
    end.

log_mirror_sync_config(Log, SyncBatchSize, 0) ->
  Log("batch size: ~p", [SyncBatchSize]);
log_mirror_sync_config(Log, SyncBatchSize, SyncThroughput) ->
  Log("max batch size: ~p; max sync throughput: ~p bytes/s", [SyncBatchSize, SyncThroughput]).

terminate({shutdown, dropped} = Reason,
          State = #state { backing_queue       = BQ,
                           backing_queue_state = BQS }) ->
    %% Backing queue termination - this node has been explicitly
    %% dropped. Normally, non-durable queues would be tidied up on
    %% startup, but there's a possibility that we will be added back
    %% in without this node being restarted. Thus we must do the full
    %% blown delete_and_terminate now, but only locally: we do not
    %% broadcast delete_and_terminate.
    State#state{backing_queue_state = BQ:delete_and_terminate(Reason, BQS)};

terminate(Reason,
          State = #state { name                = QName,
                           backing_queue       = BQ,
                           backing_queue_state = BQS }) ->
    %% Backing queue termination. The queue is going down but
    %% shouldn't be deleted. Most likely safe shutdown of this
    %% node.
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    SSPids = amqqueue:get_sync_slave_pids(Q),
    case SSPids =:= [] andalso
        rabbit_policy:get(<<"ha-promote-on-shutdown">>, Q) =/= <<"always">> of
        true  -> %% Remove the whole queue to avoid data loss
                 rabbit_mirror_queue_misc:log_warning(
                   QName, "Stopping all nodes on master shutdown since no "
                   "synchronised mirror (replica) is available", []),
                 stop_all_slaves(Reason, State);
        false -> %% Just let some other mirror take over.
                 ok
    end,
    State #state { backing_queue_state = BQ:terminate(Reason, BQS) }.

delete_and_terminate(Reason, State = #state { backing_queue       = BQ,
                                              backing_queue_state = BQS }) ->
    stop_all_slaves(Reason, State),
    State#state{backing_queue_state = BQ:delete_and_terminate(Reason, BQS)}.

stop_all_slaves(Reason, #state{name = QName, gm = GM, wait_timeout = WT}) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    SPids = amqqueue:get_slave_pids(Q),
    rabbit_mirror_queue_misc:stop_all_slaves(Reason, SPids, QName, GM, WT).

purge(State = #state { gm                  = GM,
                       backing_queue       = BQ,
                       backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {drop, 0, BQ:len(BQS), false}),
    {Count, BQS1} = BQ:purge(BQS),
    {Count, State #state { backing_queue_state = BQS1 }}.

-spec purge_acks(_) -> no_return().
purge_acks(_State) -> exit({not_implemented, {?MODULE, purge_acks}}).

publish(Msg = #basic_message { id = MsgId }, MsgProps, IsDelivered, ChPid, Flow,
        State = #state { gm                  = GM,
                         seen_status         = SS,
                         backing_queue       = BQ,
                         backing_queue_state = BQS }) ->
    false = maps:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {publish, ChPid, Flow, MsgProps, Msg},
                      rabbit_basic:msg_size(Msg)),
    BQS1 = BQ:publish(Msg, MsgProps, IsDelivered, ChPid, Flow, BQS),
    ensure_monitoring(ChPid, State #state { backing_queue_state = BQS1 }).

batch_publish(Publishes, ChPid, Flow,
              State = #state { gm                  = GM,
                               seen_status         = SS,
                               backing_queue       = BQ,
                               backing_queue_state = BQS }) ->
    {Publishes1, false, MsgSizes} =
        lists:foldl(fun ({Msg = #basic_message { id = MsgId },
                          MsgProps, _IsDelivered}, {Pubs, false, Sizes}) ->
                            {[{Msg, MsgProps, true} | Pubs], %% [0]
                             false = maps:is_key(MsgId, SS), %% ASSERTION
                             Sizes + rabbit_basic:msg_size(Msg)}
                    end, {[], false, 0}, Publishes),
    Publishes2 = lists:reverse(Publishes1),
    ok = gm:broadcast(GM, {batch_publish, ChPid, Flow, Publishes2},
                      MsgSizes),
    BQS1 = BQ:batch_publish(Publishes2, ChPid, Flow, BQS),
    ensure_monitoring(ChPid, State #state { backing_queue_state = BQS1 }).
%% [0] When the mirror process handles the publish command, it sets the
%% IsDelivered flag to true, so to avoid iterating over the messages
%% again at the mirror, we do it here.

publish_delivered(Msg = #basic_message { id = MsgId }, MsgProps,
                  ChPid, Flow, State = #state { gm                  = GM,
                                                seen_status         = SS,
                                                backing_queue       = BQ,
                                                backing_queue_state = BQS }) ->
    false = maps:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {publish_delivered, ChPid, Flow, MsgProps, Msg},
                      rabbit_basic:msg_size(Msg)),
    {AckTag, BQS1} = BQ:publish_delivered(Msg, MsgProps, ChPid, Flow, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {AckTag, ensure_monitoring(ChPid, State1)}.

batch_publish_delivered(Publishes, ChPid, Flow,
                        State = #state { gm                  = GM,
                                         seen_status         = SS,
                                         backing_queue       = BQ,
                                         backing_queue_state = BQS }) ->
    {false, MsgSizes} =
        lists:foldl(fun ({Msg = #basic_message { id = MsgId }, _MsgProps},
                         {false, Sizes}) ->
                            {false = maps:is_key(MsgId, SS), %% ASSERTION
                             Sizes + rabbit_basic:msg_size(Msg)}
                    end, {false, 0}, Publishes),
    ok = gm:broadcast(GM, {batch_publish_delivered, ChPid, Flow, Publishes},
                      MsgSizes),
    {AckTags, BQS1} = BQ:batch_publish_delivered(Publishes, ChPid, Flow, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {AckTags, ensure_monitoring(ChPid, State1)}.

discard(MsgId, ChPid, Flow, State = #state { gm                  = GM,
                                             backing_queue       = BQ,
                                             backing_queue_state = BQS,
                                             seen_status         = SS }) ->
    false = maps:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {discard, ChPid, Flow, MsgId}),
    ensure_monitoring(ChPid,
                      State #state { backing_queue_state =
                                         BQ:discard(MsgId, ChPid, Flow, BQS) }).

dropwhile(Pred, State = #state{backing_queue       = BQ,
                               backing_queue_state = BQS }) ->
    Len  = BQ:len(BQS),
    {Next, BQS1} = BQ:dropwhile(Pred, BQS),
    {Next, drop(Len, false, State #state { backing_queue_state = BQS1 })}.

fetchwhile(Pred, Fun, Acc, State = #state{backing_queue       = BQ,
                                          backing_queue_state = BQS }) ->
    Len  = BQ:len(BQS),
    {Next, Acc1, BQS1} = BQ:fetchwhile(Pred, Fun, Acc, BQS),
    {Next, Acc1, drop(Len, true, State #state { backing_queue_state = BQS1 })}.

drain_confirmed(State = #state { backing_queue       = BQ,
                                 backing_queue_state = BQS,
                                 seen_status         = SS,
                                 confirmed           = Confirmed }) ->
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    {MsgIds1, SS1} =
        lists:foldl(
          fun (MsgId, {MsgIdsN, SSN}) ->
                  %% We will never see 'discarded' here
                  case maps:find(MsgId, SSN) of
                      error ->
                          {[MsgId | MsgIdsN], SSN};
                      {ok, published} ->
                          %% It was published when we were a mirror,
                          %% and we were promoted before we saw the
                          %% publish from the channel. We still
                          %% haven't seen the channel publish, and
                          %% consequently we need to filter out the
                          %% confirm here. We will issue the confirm
                          %% when we see the publish from the channel.
                          {MsgIdsN, maps:put(MsgId, confirmed, SSN)};
                      {ok, confirmed} ->
                          %% Well, confirms are racy by definition.
                          {[MsgId | MsgIdsN], SSN}
                  end
          end, {[], SS}, MsgIds),
    {Confirmed ++ MsgIds1, State #state { backing_queue_state = BQS1,
                                          seen_status         = SS1,
                                          confirmed           = [] }}.

fetch(AckRequired, State = #state { backing_queue       = BQ,
                                    backing_queue_state = BQS }) ->
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {Result, case Result of
                 empty                          -> State1;
                 {_MsgId, _IsDelivered, _AckTag} -> drop_one(AckRequired, State1)
             end}.

drop(AckRequired, State = #state { backing_queue       = BQ,
                                   backing_queue_state = BQS }) ->
    {Result, BQS1} = BQ:drop(AckRequired, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {Result, case Result of
                 empty            -> State1;
                 {_MsgId, _AckTag} -> drop_one(AckRequired, State1)
             end}.

ack(AckTags, State = #state { gm                  = GM,
                              backing_queue       = BQ,
                              backing_queue_state = BQS }) ->
    {MsgIds, BQS1} = BQ:ack(AckTags, BQS),
    case MsgIds of
        [] -> ok;
        _  -> ok = gm:broadcast(GM, {ack, MsgIds})
    end,
    {MsgIds, State #state { backing_queue_state = BQS1 }}.

requeue(AckTags, State = #state { gm                  = GM,
                                  backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    {MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    ok = gm:broadcast(GM, {requeue, MsgIds}),
    {MsgIds, State #state { backing_queue_state = BQS1 }}.

ackfold(MsgFun, Acc, State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }, AckTags) ->
    {Acc1, BQS1} = BQ:ackfold(MsgFun, Acc, BQS, AckTags),
    {Acc1, State #state { backing_queue_state =  BQS1 }}.

fold(Fun, Acc, State = #state { backing_queue = BQ,
                                backing_queue_state = BQS }) ->
    {Result, BQS1} = BQ:fold(Fun, Acc, BQS),
    {Result, State #state { backing_queue_state = BQS1 }}.

len(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:len(BQS).

is_empty(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:is_empty(BQS).

depth(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:depth(BQS).

set_ram_duration_target(Target, State = #state { backing_queue       = BQ,
                                                 backing_queue_state = BQS }) ->
    State #state { backing_queue_state =
                       BQ:set_ram_duration_target(Target, BQS) }.

ram_duration(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    {Result, BQS1} = BQ:ram_duration(BQS),
    {Result, State #state { backing_queue_state = BQS1 }}.

needs_timeout(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:needs_timeout(BQS).

timeout(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:timeout(BQS) }.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:handle_pre_hibernate(BQS) }.

handle_info(Msg, State = #state { backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:handle_info(Msg, BQS) }.

resume(State = #state { backing_queue       = BQ,
                        backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:resume(BQS) }.

msg_rates(#state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:msg_rates(BQS).

info(backing_queue_status,
     State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:info(backing_queue_status, BQS) ++
        [ {mirror_seen,    maps:size(State #state.seen_status)},
          {mirror_senders, sets:size(State #state.known_senders)} ];
info(Item, #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:info(Item, BQS).

invoke(?MODULE, Fun, State) ->
    Fun(?MODULE, State);
invoke(Mod, Fun, State = #state { backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:invoke(Mod, Fun, BQS) }.

is_duplicate(Message = #basic_message { id = MsgId },
             State = #state { seen_status         = SS,
                              backing_queue       = BQ,
                              backing_queue_state = BQS,
                              confirmed           = Confirmed }) ->
    %% Here, we need to deal with the possibility that we're about to
    %% receive a message that we've already seen when we were a mirror
    %% (we received it via gm). Thus if we do receive such message now
    %% via the channel, there may be a confirm waiting to issue for
    %% it.

    %% We will never see {published, ChPid, MsgSeqNo} here.
    case maps:find(MsgId, SS) of
        error ->
            %% We permit the underlying BQ to have a peek at it, but
            %% only if we ourselves are not filtering out the msg.
            {Result, BQS1} = BQ:is_duplicate(Message, BQS),
            {Result, State #state { backing_queue_state = BQS1 }};
        {ok, published} ->
            %% It already got published when we were a mirror and no
            %% confirmation is waiting. amqqueue_process will have, in
            %% its msg_id_to_channel mapping, the entry for dealing
            %% with the confirm when that comes back in (it's added
            %% immediately after calling is_duplicate). The msg is
            %% invalid. We will not see this again, nor will we be
            %% further involved in confirming this message, so erase.
            {{true, drop}, State #state { seen_status = maps:remove(MsgId, SS) }};
        {ok, Disposition}
          when Disposition =:= confirmed
            %% It got published when we were a mirror via gm, and
            %% confirmed some time after that (maybe even after
            %% promotion), but before we received the publish from the
            %% channel, so couldn't previously know what the
            %% msg_seq_no was (and thus confirm as a mirror). So we
            %% need to confirm now. As above, amqqueue_process will
            %% have the entry for the msg_id_to_channel mapping added
            %% immediately after calling is_duplicate/2.
          orelse Disposition =:= discarded ->
            %% Message was discarded while we were a mirror. Confirm now.
            %% As above, amqqueue_process will have the entry for the
            %% msg_id_to_channel mapping.
            {{true, drop}, State #state { seen_status = maps:remove(MsgId, SS),
                                          confirmed = [MsgId | Confirmed] }}
    end.

set_queue_mode(Mode, State = #state { gm                  = GM,
                                      backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {set_queue_mode, Mode}),
    BQS1 = BQ:set_queue_mode(Mode, BQS),
    State #state { backing_queue_state = BQS1 }.

zip_msgs_and_acks(Msgs, AckTags, Accumulator,
                  #state { backing_queue = BQ,
                           backing_queue_state = BQS }) ->
    BQ:zip_msgs_and_acks(Msgs, AckTags, Accumulator, BQS).

%% ---------------------------------------------------------------------------
%% Other exported functions
%% ---------------------------------------------------------------------------

-spec promote_backing_queue_state
        (rabbit_amqqueue:name(), pid(), atom(), any(), pid(), [any()],
         map(), [pid()]) ->
            master_state().

promote_backing_queue_state(QName, CPid, BQ, BQS, GM, AckTags, Seen, KS) ->
    {_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    Len   = BQ:len(BQS1),
    Depth = BQ:depth(BQS1),
    true = Len == Depth, %% ASSERTION: everything must have been requeued
    ok = gm:broadcast(GM, {depth, Depth}),
    WaitTimeout = rabbit_misc:get_env(rabbit, slave_wait_timeout, 15000),
    #state { name                = QName,
             gm                  = GM,
             coordinator         = CPid,
             backing_queue       = BQ,
             backing_queue_state = BQS1,
             seen_status         = Seen,
             confirmed           = [],
             known_senders       = sets:from_list(KS),
             wait_timeout        = WaitTimeout }.

-spec sender_death_fun() -> death_fun().

sender_death_fun() ->
    Self = self(),
    fun (DeadPid) ->
            rabbit_amqqueue:run_backing_queue(
              Self, ?MODULE,
              fun (?MODULE, State = #state { gm = GM, known_senders = KS }) ->
                      ok = gm:broadcast(GM, {sender_death, DeadPid}),
                      KS1 = sets:del_element(DeadPid, KS),
                      State #state { known_senders = KS1 }
              end)
    end.

-spec depth_fun() -> depth_fun().

depth_fun() ->
    Self = self(),
    fun () ->
            rabbit_amqqueue:run_backing_queue(
              Self, ?MODULE,
              fun (?MODULE, State = #state { gm                  = GM,
                                             backing_queue       = BQ,
                                             backing_queue_state = BQS }) ->
                      ok = gm:broadcast(GM, {depth, BQ:depth(BQS)}),
                      State
              end)
    end.

%% ---------------------------------------------------------------------------
%% Helpers
%% ---------------------------------------------------------------------------

drop_one(AckRequired, State = #state { gm                  = GM,
                                       backing_queue       = BQ,
                                       backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {drop, BQ:len(BQS), 1, AckRequired}),
    State.

drop(PrevLen, AckRequired, State = #state { gm                  = GM,
                                            backing_queue       = BQ,
                                            backing_queue_state = BQS }) ->
    Len = BQ:len(BQS),
    case PrevLen - Len of
        0       -> State;
        Dropped -> ok = gm:broadcast(GM, {drop, Len, Dropped, AckRequired}),
                   State
    end.

ensure_monitoring(ChPid, State = #state { coordinator = CPid,
                                          known_senders = KS }) ->
    case sets:is_element(ChPid, KS) of
        true  -> State;
        false -> ok = rabbit_mirror_queue_coordinator:ensure_monitoring(
                        CPid, [ChPid]),
                 State #state { known_senders = sets:add_element(ChPid, KS) }
    end.

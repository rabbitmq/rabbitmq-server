%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2010-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_master).

-export([init/3, terminate/2, delete_and_terminate/2,
         purge/1, purge_acks/1, publish/5, publish_delivered/4,
         discard/3, fetch/2, drop/2, ack/2, requeue/2, ackfold/4, fold/3,
         len/1, is_empty/1, depth/1, drain_confirmed/1,
         dropwhile/2, fetchwhile/4, set_ram_duration_target/2, ram_duration/1,
         needs_timeout/1, timeout/1, handle_pre_hibernate/1,
         status/1, invoke/3, is_duplicate/2]).

-export([start/1, stop/0]).

-export([promote_backing_queue_state/8, sender_death_fun/0, depth_fun/0]).

-export([init_with_existing_bq/3, stop_mirroring/1, sync_mirrors/3]).

-behaviour(rabbit_backing_queue).

-include("rabbit.hrl").

-record(state, { name,
                 gm,
                 coordinator,
                 backing_queue,
                 backing_queue_state,
                 seen_status,
                 confirmed,
                 known_senders
               }).

-ifdef(use_specs).

-export_type([death_fun/0, depth_fun/0, stats_fun/0]).

-type(death_fun() :: fun ((pid()) -> 'ok')).
-type(depth_fun() :: fun (() -> 'ok')).
-type(stats_fun() :: fun ((any()) -> 'ok')).
-type(master_state() :: #state { name                :: rabbit_amqqueue:name(),
                                 gm                  :: pid(),
                                 coordinator         :: pid(),
                                 backing_queue       :: atom(),
                                 backing_queue_state :: any(),
                                 seen_status         :: dict(),
                                 confirmed           :: [rabbit_guid:guid()],
                                 known_senders       :: set()
                               }).

-spec(promote_backing_queue_state/8 ::
        (rabbit_amqqueue:name(), pid(), atom(), any(), pid(), [any()], dict(),
         [pid()]) -> master_state()).
-spec(sender_death_fun/0 :: () -> death_fun()).
-spec(depth_fun/0 :: () -> depth_fun()).
-spec(init_with_existing_bq/3 :: (rabbit_types:amqqueue(), atom(), any()) ->
                                      master_state()).
-spec(stop_mirroring/1 :: (master_state()) -> {atom(), any()}).
-spec(sync_mirrors/3 :: (stats_fun(), stats_fun(), master_state()) ->
    {'ok', master_state()} | {stop, any(), master_state()}).

-endif.

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator

%% ---------------------------------------------------------------------------
%% Backing queue
%% ---------------------------------------------------------------------------

start(_DurableQueues) ->
    %% This will never get called as this module will never be
    %% installed as the default BQ implementation.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

stop() ->
    %% Same as start/1.
    exit({not_valid_for_generic_backing_queue, ?MODULE}).

init(Q, Recover, AsyncCallback) ->
    {ok, BQ} = application:get_env(backing_queue_module),
    BQS = BQ:init(Q, Recover, AsyncCallback),
    State = #state{gm = GM} = init_with_existing_bq(Q, BQ, BQS),
    ok = gm:broadcast(GM, {depth, BQ:depth(BQS)}),
    State.

init_with_existing_bq(Q = #amqqueue{name = QName}, BQ, BQS) ->
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(
                   Q, undefined, sender_death_fun(), depth_fun()),
    GM = rabbit_mirror_queue_coordinator:get_gm(CPid),
    Self = self(),
    ok = rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   [Q1 = #amqqueue{gm_pids = GMPids}]
                       = mnesia:read({rabbit_queue, QName}),
                   ok = rabbit_amqqueue:store_queue(
                          Q1#amqqueue{gm_pids = [{GM, Self} | GMPids]})
           end),
    {_MNode, SNodes} = rabbit_mirror_queue_misc:suggested_queue_nodes(Q),
    rabbit_mirror_queue_misc:add_mirrors(QName, SNodes),
    #state { name                = QName,
             gm                  = GM,
             coordinator         = CPid,
             backing_queue       = BQ,
             backing_queue_state = BQS,
             seen_status         = dict:new(),
             confirmed           = [],
             known_senders       = sets:new() }.

stop_mirroring(State = #state { coordinator         = CPid,
                                backing_queue       = BQ,
                                backing_queue_state = BQS }) ->
    unlink(CPid),
    stop_all_slaves(shutdown, State),
    {BQ, BQS}.

sync_mirrors(HandleInfo, EmitStats,
             State = #state { name                = QName,
                              gm                  = GM,
                              backing_queue       = BQ,
                              backing_queue_state = BQS }) ->
    Log = fun (Fmt, Params) ->
                  rabbit_log:info("Synchronising ~s: " ++ Fmt ++ "~n",
                                  [rabbit_misc:rs(QName) | Params])
          end,
    Log("~p messages to synchronise", [BQ:len(BQS)]),
    {ok, #amqqueue{slave_pids = SPids}} = rabbit_amqqueue:lookup(QName),
    Ref = make_ref(),
    Syncer = rabbit_mirror_queue_sync:master_prepare(Ref, Log, SPids),
    gm:broadcast(GM, {sync_start, Ref, Syncer, SPids}),
    S = fun(BQSN) -> State#state{backing_queue_state = BQSN} end,
    case rabbit_mirror_queue_sync:master_go(
           Syncer, Ref, Log, HandleInfo, EmitStats, BQ, BQS) of
        {shutdown,  R, BQS1}   -> {stop, R, S(BQS1)};
        {sync_died, R, BQS1}   -> Log("~p", [R]),
                                  {ok, S(BQS1)};
        {already_synced, BQS1} -> {ok, S(BQS1)};
        {ok, BQS1}             -> Log("complete", []),
                                  {ok, S(BQS1)}
    end.

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
          State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    %% Backing queue termination. The queue is going down but
    %% shouldn't be deleted. Most likely safe shutdown of this
    %% node. Thus just let some other slave take over.
    State #state { backing_queue_state = BQ:terminate(Reason, BQS) }.

delete_and_terminate(Reason, State = #state { backing_queue       = BQ,
                                              backing_queue_state = BQS }) ->
    stop_all_slaves(Reason, State),
    State#state{backing_queue_state = BQ:delete_and_terminate(Reason, BQS)}.

stop_all_slaves(Reason, #state{name = QName, gm   = GM}) ->
    {ok, #amqqueue{slave_pids = SPids}} = rabbit_amqqueue:lookup(QName),
    MRefs = [erlang:monitor(process, SPid) || SPid <- SPids],
    ok = gm:broadcast(GM, {delete_and_terminate, Reason}),
    [receive {'DOWN', MRef, process, _Pid, _Info} -> ok end || MRef <- MRefs],
    %% Normally when we remove a slave another slave or master will
    %% notice and update Mnesia. But we just removed them all, and
    %% have stopped listening ourselves. So manually clean up.
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              [Q] = mnesia:read({rabbit_queue, QName}),
              rabbit_mirror_queue_misc:store_updated_slaves(
                Q #amqqueue { gm_pids = [], slave_pids = [] })
      end),
    ok = gm:forget_group(QName).

purge(State = #state { gm                  = GM,
                       backing_queue       = BQ,
                       backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {drop, 0, BQ:len(BQS), false}),
    {Count, BQS1} = BQ:purge(BQS),
    {Count, State #state { backing_queue_state = BQS1 }}.

purge_acks(_State) -> exit({not_implemented, {?MODULE, purge_acks}}).

publish(Msg = #basic_message { id = MsgId }, MsgProps, IsDelivered, ChPid,
        State = #state { gm                  = GM,
                         seen_status         = SS,
                         backing_queue       = BQ,
                         backing_queue_state = BQS }) ->
    false = dict:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {publish, ChPid, MsgProps, Msg}),
    BQS1 = BQ:publish(Msg, MsgProps, IsDelivered, ChPid, BQS),
    ensure_monitoring(ChPid, State #state { backing_queue_state = BQS1 }).

publish_delivered(Msg = #basic_message { id = MsgId }, MsgProps,
                  ChPid, State = #state { gm                  = GM,
                                          seen_status         = SS,
                                          backing_queue       = BQ,
                                          backing_queue_state = BQS }) ->
    false = dict:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {publish_delivered, ChPid, MsgProps, Msg}),
    {AckTag, BQS1} = BQ:publish_delivered(Msg, MsgProps, ChPid, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {AckTag, ensure_monitoring(ChPid, State1)}.

discard(MsgId, ChPid, State = #state { gm                  = GM,
                                       backing_queue       = BQ,
                                       backing_queue_state = BQS,
                                       seen_status         = SS }) ->
    false = dict:is_key(MsgId, SS), %% ASSERTION
    ok = gm:broadcast(GM, {discard, ChPid, MsgId}),
    ensure_monitoring(ChPid, State #state { backing_queue_state =
                                                BQ:discard(MsgId, ChPid, BQS) }).

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
                  case dict:find(MsgId, SSN) of
                      error ->
                          {[MsgId | MsgIdsN], SSN};
                      {ok, published} ->
                          %% It was published when we were a slave,
                          %% and we were promoted before we saw the
                          %% publish from the channel. We still
                          %% haven't seen the channel publish, and
                          %% consequently we need to filter out the
                          %% confirm here. We will issue the confirm
                          %% when we see the publish from the channel.
                          {MsgIdsN, dict:store(MsgId, confirmed, SSN)};
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
                 {_MsgId, _IsDelivered, AckTag} -> drop_one(AckTag, State1)
             end}.

drop(AckRequired, State = #state { backing_queue       = BQ,
                                   backing_queue_state = BQS }) ->
    {Result, BQS1} = BQ:drop(AckRequired, BQS),
    State1 = State #state { backing_queue_state = BQS1 },
    {Result, case Result of
                 empty            -> State1;
                 {_MsgId, AckTag} -> drop_one(AckTag, State1)
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

status(State = #state { backing_queue = BQ, backing_queue_state = BQS }) ->
    BQ:status(BQS) ++
        [ {mirror_seen,    dict:size(State #state.seen_status)},
          {mirror_senders, sets:size(State #state.known_senders)} ].

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
    %% receive a message that we've already seen when we were a slave
    %% (we received it via gm). Thus if we do receive such message now
    %% via the channel, there may be a confirm waiting to issue for
    %% it.

    %% We will never see {published, ChPid, MsgSeqNo} here.
    case dict:find(MsgId, SS) of
        error ->
            %% We permit the underlying BQ to have a peek at it, but
            %% only if we ourselves are not filtering out the msg.
            {Result, BQS1} = BQ:is_duplicate(Message, BQS),
            {Result, State #state { backing_queue_state = BQS1 }};
        {ok, published} ->
            %% It already got published when we were a slave and no
            %% confirmation is waiting. amqqueue_process will have, in
            %% its msg_id_to_channel mapping, the entry for dealing
            %% with the confirm when that comes back in (it's added
            %% immediately after calling is_duplicate). The msg is
            %% invalid. We will not see this again, nor will we be
            %% further involved in confirming this message, so erase.
            {true, State #state { seen_status = dict:erase(MsgId, SS) }};
        {ok, Disposition}
          when Disposition =:= confirmed
            %% It got published when we were a slave via gm, and
            %% confirmed some time after that (maybe even after
            %% promotion), but before we received the publish from the
            %% channel, so couldn't previously know what the
            %% msg_seq_no was (and thus confirm as a slave). So we
            %% need to confirm now. As above, amqqueue_process will
            %% have the entry for the msg_id_to_channel mapping added
            %% immediately after calling is_duplicate/2.
          orelse Disposition =:= discarded ->
            %% Message was discarded while we were a slave. Confirm now.
            %% As above, amqqueue_process will have the entry for the
            %% msg_id_to_channel mapping.
            {true, State #state { seen_status = dict:erase(MsgId, SS),
                                  confirmed = [MsgId | Confirmed] }}
    end.

%% ---------------------------------------------------------------------------
%% Other exported functions
%% ---------------------------------------------------------------------------

promote_backing_queue_state(QName, CPid, BQ, BQS, GM, AckTags, Seen, KS) ->
    {_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    Len   = BQ:len(BQS1),
    Depth = BQ:depth(BQS1),
    true = Len == Depth, %% ASSERTION: everything must have been requeued
    ok = gm:broadcast(GM, {depth, Depth}),
    #state { name                = QName,
             gm                  = GM,
             coordinator         = CPid,
             backing_queue       = BQ,
             backing_queue_state = BQS1,
             seen_status         = Seen,
             confirmed           = [],
             known_senders       = sets:from_list(KS) }.

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

drop_one(AckTag, State = #state { gm                  = GM,
                                  backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    ok = gm:broadcast(GM, {drop, BQ:len(BQS), 1, AckTag =/= undefined}),
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

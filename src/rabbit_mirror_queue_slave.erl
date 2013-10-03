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

-module(rabbit_mirror_queue_slave).

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator
%%
%% We receive messages from GM and from publishers, and the gm
%% messages can arrive either before or after the 'actual' message.
%% All instructions from the GM group must be processed in the order
%% in which they're received.

-export([start_link/1, set_maximum_since_use/2, info/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).

-export([joined/2, members_changed/4, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").

-include("gm_specs.hrl").

%%----------------------------------------------------------------------------

-define(INFO_KEYS,
        [pid,
         name,
         master_pid,
         is_synchronised
        ]).

-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).
-define(DEATH_TIMEOUT,                 20000). %% 20 seconds

-record(state, { q,
                 gm,
                 backing_queue,
                 backing_queue_state,
                 sync_timer_ref,
                 rate_timer_ref,

                 sender_queues, %% :: Pid -> {Q Msg, Set MsgId, ChState}
                 msg_id_ack,    %% :: MsgId -> AckTag

                 msg_id_status,
                 known_senders,

                 %% Master depth - local depth
                 depth_delta
               }).

%%----------------------------------------------------------------------------

start_link(Q) -> gen_server2:start_link(?MODULE, Q, []).

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

info(QPid) -> gen_server2:call(QPid, info, infinity).

init(Q = #amqqueue { name = QName }) ->
    %% We join the GM group before we add ourselves to the amqqueue
    %% record. As a result:
    %% 1. We can receive msgs from GM that correspond to messages we will
    %% never receive from publishers.
    %% 2. When we receive a message from publishers, we must receive a
    %% message from the GM group for it.
    %% 3. However, that instruction from the GM group can arrive either
    %% before or after the actual message. We need to be able to
    %% distinguish between GM instructions arriving early, and case (1)
    %% above.
    %%
    process_flag(trap_exit, true), %% amqqueue_process traps exits too.
    {ok, GM} = gm:start_link(QName, ?MODULE, [self()],
                             fun rabbit_misc:execute_mnesia_transaction/1),
    receive {joined, GM} -> ok end,
    Self = self(),
    Node = node(),
    case rabbit_misc:execute_mnesia_transaction(
           fun() -> init_it(Self, GM, Node, QName) end) of
        {new, QPid, GMPids} ->
            erlang:monitor(process, QPid),
            ok = file_handle_cache:register_callback(
                   rabbit_amqqueue, set_maximum_since_use, [Self]),
            ok = rabbit_memory_monitor:register(
                   Self, {rabbit_amqqueue, set_ram_duration_target, [Self]}),
            {ok, BQ} = application:get_env(backing_queue_module),
            Q1 = Q #amqqueue { pid = QPid },
            BQS = bq_init(BQ, Q1, false),
            State = #state { q                   = Q1,
                             gm                  = GM,
                             backing_queue       = BQ,
                             backing_queue_state = BQS,
                             rate_timer_ref      = undefined,
                             sync_timer_ref      = undefined,

                             sender_queues       = dict:new(),
                             msg_id_ack          = dict:new(),

                             msg_id_status       = dict:new(),
                             known_senders       = pmon:new(delegate),

                             depth_delta         = undefined
                   },
            ok = gm:broadcast(GM, request_depth),
            ok = gm:validate_members(GM, [GM | [G || {G, _} <- GMPids]]),
            {ok, State, hibernate,
             {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
              ?DESIRED_HIBERNATE}};
        {stale, StalePid} ->
            {stop, {stale_master_pid, StalePid}};
        duplicate_live_master ->
            {stop, {duplicate_live_master, Node}};
        existing ->
            gm:leave(GM),
            ignore;
        master_in_recovery ->
            %% The queue record vanished - we must have a master starting
            %% concurrently with us. In that case we can safely decide to do
            %% nothing here, and the master will start us in
            %% master:init_with_existing_bq/3
            ignore
    end.

init_it(Self, GM, Node, QName) ->
    case mnesia:read({rabbit_queue, QName}) of
        [Q = #amqqueue { pid = QPid, slave_pids = SPids, gm_pids = GMPids }] ->
            case [Pid || Pid <- [QPid | SPids], node(Pid) =:= Node] of
                []     -> add_slave(Q, Self, GM),
                          {new, QPid, GMPids};
                [QPid] -> case rabbit_misc:is_process_alive(QPid) of
                              true  -> duplicate_live_master;
                              false -> {stale, QPid}
                          end;
                [SPid] -> case rabbit_misc:is_process_alive(SPid) of
                              true  -> existing;
                              false -> GMPids = [T || T = {_, S} <- GMPids,
                                                      S =/= SPid],
                                       Q1 = Q#amqqueue{
                                              slave_pids = SPids -- [SPid],
                                              gm_pids    = GMPids},
                                       add_slave(Q1, Self, GM),
                                       {new, QPid, GMPids}
                          end
            end;
        [] ->
            master_in_recovery
    end.

%% Add to the end, so they are in descending order of age, see
%% rabbit_mirror_queue_misc:promote_slave/1
add_slave(Q = #amqqueue { slave_pids = SPids, gm_pids = GMPids }, New, GM) ->
    rabbit_mirror_queue_misc:store_updated_slaves(
      Q#amqqueue{slave_pids = SPids ++ [New], gm_pids = [{GM, New} | GMPids]}).

handle_call({deliver, Delivery, true}, From, State) ->
    %% Synchronous, "mandatory" deliver mode.
    gen_server2:reply(From, ok),
    noreply(maybe_enqueue_message(Delivery, State));

handle_call({gm_deaths, LiveGMPids}, From,
            State = #state { q = Q = #amqqueue { name = QName, pid = MPid }}) ->
    Self = self(),
    case rabbit_mirror_queue_misc:remove_from_queue(QName, Self, LiveGMPids) of
        {error, not_found} ->
            gen_server2:reply(From, ok),
            {stop, normal, State};
        {ok, Pid, DeadPids} ->
            rabbit_mirror_queue_misc:report_deaths(Self, false, QName,
                                                   DeadPids),
            case Pid of
                MPid ->
                    %% master hasn't changed
                    gen_server2:reply(From, ok),
                    noreply(State);
                Self ->
                    %% we've become master
                    QueueState = promote_me(From, State),
                    {become, rabbit_amqqueue_process, QueueState, hibernate};
                _ ->
                    %% master has changed to not us
                    gen_server2:reply(From, ok),
                    erlang:monitor(process, Pid),
                    noreply(State #state { q = Q #amqqueue { pid = Pid } })
            end
    end;

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State).

handle_cast({run_backing_queue, Mod, Fun}, State) ->
    noreply(run_backing_queue(Mod, Fun, State));

handle_cast({gm, Instruction}, State) ->
    handle_process_result(process_instruction(Instruction, State));

handle_cast({deliver, Delivery = #delivery{sender = Sender}, true, Flow},
            State) ->
    %% Asynchronous, non-"mandatory", deliver mode.
    case Flow of
        flow   -> credit_flow:ack(Sender);
        noflow -> ok
    end,
    noreply(maybe_enqueue_message(Delivery, State));

handle_cast({sync_start, Ref, Syncer},
            State = #state { depth_delta         = DD,
                             backing_queue       = BQ,
                             backing_queue_state = BQS }) ->
    State1 = #state{rate_timer_ref = TRef} = ensure_rate_timer(State),
    S = fun({MA, TRefN, BQSN}) ->
                State1#state{depth_delta         = undefined,
                             msg_id_ack          = dict:from_list(MA),
                             rate_timer_ref      = TRefN,
                             backing_queue_state = BQSN}
        end,
    case rabbit_mirror_queue_sync:slave(
           DD, Ref, TRef, Syncer, BQ, BQS,
           fun (BQN, BQSN) ->
                   BQSN1 = update_ram_duration(BQN, BQSN),
                   TRefN = erlang:send_after(?RAM_DURATION_UPDATE_INTERVAL,
                                             self(), update_ram_duration),
                   {TRefN, BQSN1}
           end) of
        denied              -> noreply(State1);
        {ok,           Res} -> noreply(set_delta(0, S(Res)));
        {failed,       Res} -> noreply(S(Res));
        {stop, Reason, Res} -> {stop, Reason, S(Res)}
    end;

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast({set_ram_duration_target, Duration},
            State = #state { backing_queue       = BQ,
                             backing_queue_state = BQS }) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State #state { backing_queue_state = BQS1 }).

handle_info(update_ram_duration, State = #state{backing_queue       = BQ,
                                                backing_queue_state = BQS}) ->
    BQS1 = update_ram_duration(BQ, BQS),
    %% Don't call noreply/1, we don't want to set timers
    {State1, Timeout} = next_state(State #state {
                                     rate_timer_ref      = undefined,
                                     backing_queue_state = BQS1 }),
    {noreply, State1, Timeout};

handle_info(sync_timeout, State) ->
    noreply(backing_queue_timeout(
              State #state { sync_timer_ref = undefined }));

handle_info(timeout, State) ->
    noreply(backing_queue_timeout(State));

handle_info({'DOWN', _MonitorRef, process, MPid, _Reason},
            State = #state { gm = GM, q = #amqqueue { pid = MPid } }) ->
    ok = gm:broadcast(GM, process_death),
    noreply(State);

handle_info({'DOWN', _MonitorRef, process, ChPid, _Reason}, State) ->
    local_sender_death(ChPid, State),
    noreply(maybe_forget_sender(ChPid, down_from_ch, State));

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    noreply(State);

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

%% If the Reason is shutdown, or {shutdown, _}, it is not the queue
%% being deleted: it's just the node going down. Even though we're a
%% slave, we have no idea whether or not we'll be the only copy coming
%% back up. Thus we must assume we will be, and preserve anything we
%% have on disk.
terminate(_Reason, #state { backing_queue_state = undefined }) ->
    %% We've received a delete_and_terminate from gm, thus nothing to
    %% do here.
    ok;
terminate({shutdown, dropped} = R, #state { backing_queue       = BQ,
                                            backing_queue_state = BQS }) ->
    %% See rabbit_mirror_queue_master:terminate/2
    BQ:delete_and_terminate(R, BQS);
terminate(Reason, #state { q                   = Q,
                           gm                  = GM,
                           backing_queue       = BQ,
                           backing_queue_state = BQS,
                           rate_timer_ref      = RateTRef }) ->
    ok = gm:leave(GM),
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, BQ, BQS, RateTRef, [], pmon:new(), dict:new()),
    rabbit_amqqueue_process:terminate(Reason, QueueState);
terminate([_SPid], _Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    BQS3 = BQ:handle_pre_hibernate(BQS2),
    {hibernate, stop_rate_timer(State #state { backing_queue_state = BQS3 })}.

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        info                                 -> 9;
        {gm_deaths, _Live}                   -> 5;
        _                                    -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        {gm, _Msg}                           -> 5;
        _                                    -> 0
    end.

prioritise_info(Msg, _Len, _State) ->
    case Msg of
        update_ram_duration                  -> 8;
        sync_timeout                         -> 6;
        _                                    -> 0
    end.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([SPid], _Members) -> SPid ! {joined, self()}, ok.

members_changed([_SPid], _Births, [], _Live) ->
    ok;
members_changed([ SPid], _Births, _Deaths, Live) ->
    inform_deaths(SPid, Live).

handle_msg([_SPid], _From, request_depth) ->
    %% This is only of value to the master
    ok;
handle_msg([_SPid], _From, {ensure_monitoring, _Pid}) ->
    %% This is only of value to the master
    ok;
handle_msg([_SPid], _From, process_death) ->
    %% Since GM is by nature lazy we need to make sure there is some
    %% traffic when a master dies, to make sure we get informed of the
    %% death. That's all process_death does, create some traffic. We
    %% must not take any notice of the master death here since it
    %% comes without ordering guarantees - there could still be
    %% messages from the master we have yet to receive. When we get
    %% members_changed, then there will be no more messages.
    ok;
handle_msg([CPid], _From, {delete_and_terminate, _Reason} = Msg) ->
    ok = gen_server2:cast(CPid, {gm, Msg}),
    {stop, {shutdown, ring_shutdown}};
handle_msg([SPid], _From, {sync_start, Ref, Syncer, SPids}) ->
    case lists:member(SPid, SPids) of
        true  -> gen_server2:cast(SPid, {sync_start, Ref, Syncer});
        false -> ok
    end;
handle_msg([SPid], _From, Msg) ->
    ok = gen_server2:cast(SPid, {gm, Msg}).

inform_deaths(SPid, Live) ->
    case gen_server2:call(SPid, {gm_deaths, Live}, infinity) of
        ok              -> ok;
        {promote, CPid} -> {become, rabbit_mirror_queue_coordinator, [CPid]}
    end.

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid,             _State)                                   -> self();
i(name,            #state { q = #amqqueue { name = Name } }) -> Name;
i(master_pid,      #state { q = #amqqueue { pid  = MPid } }) -> MPid;
i(is_synchronised, #state { depth_delta = DD })              -> DD =:= 0;
i(Item,            _State) -> throw({bad_argument, Item}).

bq_init(BQ, Q, Recover) ->
    Self = self(),
    BQ:init(Q, Recover,
            fun (Mod, Fun) ->
                    rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
            end).

run_backing_queue(rabbit_mirror_queue_master, Fun, State) ->
    %% Yes, this might look a little crazy, but see comments in
    %% confirm_sender_death/1
    Fun(?MODULE, State);
run_backing_queue(Mod, Fun, State = #state { backing_queue       = BQ,
                                             backing_queue_state = BQS }) ->
    State #state { backing_queue_state = BQ:invoke(Mod, Fun, BQS) }.

send_or_record_confirm(_, #delivery{ msg_seq_no = undefined }, MS, _State) ->
    MS;
send_or_record_confirm(published, #delivery { sender     = ChPid,
                                              msg_seq_no = MsgSeqNo,
                                              message    = #basic_message {
                                                id            = MsgId,
                                                is_persistent = true } },
                       MS, #state { q = #amqqueue { durable = true } }) ->
    dict:store(MsgId, {published, ChPid, MsgSeqNo} , MS);
send_or_record_confirm(_Status, #delivery { sender     = ChPid,
                                            msg_seq_no = MsgSeqNo },
                       MS, _State) ->
    ok = rabbit_misc:confirm_to_sender(ChPid, [MsgSeqNo]),
    MS.

confirm_messages(MsgIds, State = #state { msg_id_status = MS }) ->
    {CMs, MS1} =
        lists:foldl(
          fun (MsgId, {CMsN, MSN} = Acc) ->
                  %% We will never see 'discarded' here
                  case dict:find(MsgId, MSN) of
                      error ->
                          %% If it needed confirming, it'll have
                          %% already been done.
                          Acc;
                      {ok, published} ->
                          %% Still not seen it from the channel, just
                          %% record that it's been confirmed.
                          {CMsN, dict:store(MsgId, confirmed, MSN)};
                      {ok, {published, ChPid, MsgSeqNo}} ->
                          %% Seen from both GM and Channel. Can now
                          %% confirm.
                          {rabbit_misc:gb_trees_cons(ChPid, MsgSeqNo, CMsN),
                           dict:erase(MsgId, MSN)};
                      {ok, confirmed} ->
                          %% It's already been confirmed. This is
                          %% probably it's been both sync'd to disk
                          %% and then delivered and ack'd before we've
                          %% seen the publish from the
                          %% channel. Nothing to do here.
                          Acc
                  end
          end, {gb_trees:empty(), MS}, MsgIds),
    rabbit_misc:gb_trees_foreach(fun rabbit_misc:confirm_to_sender/2, CMs),
    State #state { msg_id_status = MS1 }.

handle_process_result({ok,   State}) -> noreply(State);
handle_process_result({stop, State}) -> {stop, normal, State}.

-ifdef(use_specs).
-spec(promote_me/2 :: ({pid(), term()}, #state{}) -> no_return()).
-endif.
promote_me(From, #state { q                   = Q = #amqqueue { name = QName },
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = RateTRef,
                          sender_queues       = SQ,
                          msg_id_ack          = MA,
                          msg_id_status       = MS,
                          known_senders       = KS }) ->
    rabbit_log:info("Mirrored-queue (~s): Promoting slave ~s to master~n",
                    [rabbit_misc:rs(QName), rabbit_misc:pid_to_string(self())]),
    Q1 = Q #amqqueue { pid = self() },
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(
                   Q1, GM, rabbit_mirror_queue_master:sender_death_fun(),
                   rabbit_mirror_queue_master:depth_fun()),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),

    %% Everything that we're monitoring, we need to ensure our new
    %% coordinator is monitoring.
    MPids = pmon:monitored(KS),
    ok = rabbit_mirror_queue_coordinator:ensure_monitoring(CPid, MPids),

    %% We find all the messages that we've received from channels but
    %% not from gm, and pass them to the
    %% queue_process:init_with_backing_queue_state to be enqueued.
    %%
    %% We also have to requeue messages which are pending acks: the
    %% consumers from the master queue have been lost and so these
    %% messages need requeuing. They might also be pending
    %% confirmation, and indeed they might also be pending arrival of
    %% the publication from the channel itself, if we received both
    %% the publication and the fetch via gm first! Requeuing doesn't
    %% affect confirmations: if the message was previously pending a
    %% confirmation then it still will be, under the same msg_id. So
    %% as a master, we need to be prepared to filter out the
    %% publication of said messages from the channel (is_duplicate
    %% (thus such requeued messages must remain in the msg_id_status
    %% (MS) which becomes seen_status (SS) in the master)).
    %%
    %% Then there are messages we already have in the queue, which are
    %% not currently pending acknowledgement:
    %% 1. Messages we've only received via gm:
    %%    Filter out subsequent publication from channel through
    %%    validate_message. Might have to issue confirms then or
    %%    later, thus queue_process state will have to know that
    %%    there's a pending confirm.
    %% 2. Messages received via both gm and channel:
    %%    Queue will have to deal with issuing confirms if necessary.
    %%
    %% MS contains the following three entry types:
    %%
    %% a) published:
    %%   published via gm only; pending arrival of publication from
    %%   channel, maybe pending confirm.
    %%
    %% b) {published, ChPid, MsgSeqNo}:
    %%   published via gm and channel; pending confirm.
    %%
    %% c) confirmed:
    %%   published via gm only, and confirmed; pending publication
    %%   from channel.
    %%
    %% d) discarded:
    %%   seen via gm only as discarded. Pending publication from
    %%   channel
    %%
    %% The forms a, c and d only, need to go to the master state
    %% seen_status (SS).
    %%
    %% The form b only, needs to go through to the queue_process
    %% state to form the msg_id_to_channel mapping (MTC).
    %%
    %% No messages that are enqueued from SQ at this point will have
    %% entries in MS.
    %%
    %% Messages that are extracted from MA may have entries in MS, and
    %% those messages are then requeued. However, as discussed above,
    %% this does not affect MS, nor which bits go through to SS in
    %% Master, or MTC in queue_process.

    St = [published, confirmed, discarded],
    SS = dict:filter(fun (_MsgId, Status) -> lists:member(Status, St) end, MS),
    AckTags = [AckTag || {_MsgId, AckTag} <- dict:to_list(MA)],

    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    QName, CPid, BQ, BQS, GM, AckTags, SS, MPids),

    MTC = dict:fold(fun (MsgId, {published, ChPid, MsgSeqNo}, MTC0) ->
                            gb_trees:insert(MsgId, {ChPid, MsgSeqNo}, MTC0);
                        (_Msgid, _Status, MTC0) ->
                            MTC0
                    end, gb_trees:empty(), MS),
    Deliveries = [Delivery ||
                   {_ChPid, {PubQ, _PendCh, _ChState}} <- dict:to_list(SQ),
                   Delivery <- queue:to_list(PubQ)],
    AwaitGmDown = [ChPid || {ChPid, {_, _, down_from_ch}} <- dict:to_list(SQ)],
    KS1 = lists:foldl(fun (ChPid0, KS0) ->
                              pmon:demonitor(ChPid0, KS0)
                      end, KS, AwaitGmDown),
    rabbit_amqqueue_process:init_with_backing_queue_state(
      Q1, rabbit_mirror_queue_master, MasterState, RateTRef, Deliveries, KS1,
      MTC).

noreply(State) ->
    {NewState, Timeout} = next_state(State),
    {noreply, ensure_rate_timer(NewState), Timeout}.

reply(Reply, State) ->
    {NewState, Timeout} = next_state(State),
    {reply, Reply, ensure_rate_timer(NewState), Timeout}.

next_state(State = #state{backing_queue = BQ, backing_queue_state = BQS}) ->
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    State1 = confirm_messages(MsgIds,
                              State #state { backing_queue_state = BQS1 }),
    case BQ:needs_timeout(BQS1) of
        false -> {stop_sync_timer(State1),   hibernate     };
        idle  -> {stop_sync_timer(State1),   ?SYNC_INTERVAL};
        timed -> {ensure_sync_timer(State1), 0             }
    end.

backing_queue_timeout(State = #state { backing_queue = BQ }) ->
    run_backing_queue(BQ, fun (M, BQS) -> M:timeout(BQS) end, State).

ensure_sync_timer(State) ->
    rabbit_misc:ensure_timer(State, #state.sync_timer_ref,
                             ?SYNC_INTERVAL, sync_timeout).

stop_sync_timer(State) -> rabbit_misc:stop_timer(State, #state.sync_timer_ref).

ensure_rate_timer(State) ->
    rabbit_misc:ensure_timer(State, #state.rate_timer_ref,
                             ?RAM_DURATION_UPDATE_INTERVAL,
                             update_ram_duration).

stop_rate_timer(State) -> rabbit_misc:stop_timer(State, #state.rate_timer_ref).

ensure_monitoring(ChPid, State = #state { known_senders = KS }) ->
    State #state { known_senders = pmon:monitor(ChPid, KS) }.

local_sender_death(ChPid, #state { known_senders = KS }) ->
    %% The channel will be monitored iff we have received a delivery
    %% from it but not heard about its death from the master. So if it
    %% is monitored we need to point the death out to the master (see
    %% essay).
    ok = case pmon:is_monitored(ChPid, KS) of
             false -> ok;
             true  -> confirm_sender_death(ChPid)
         end.

confirm_sender_death(Pid) ->
    %% We have to deal with the possibility that we'll be promoted to
    %% master before this thing gets run. Consequently we set the
    %% module to rabbit_mirror_queue_master so that if we do become a
    %% rabbit_amqqueue_process before then, sane things will happen.
    Fun =
        fun (?MODULE, State = #state { known_senders = KS,
                                       gm            = GM }) ->
                %% We're running still as a slave
                %%
                %% See comment in local_sender_death/2; we might have
                %% received a sender_death in the meanwhile so check
                %% again.
                ok = case pmon:is_monitored(Pid, KS) of
                         false -> ok;
                         true  -> gm:broadcast(GM, {ensure_monitoring, [Pid]}),
                                  confirm_sender_death(Pid)
                     end,
                State;
            (rabbit_mirror_queue_master, State) ->
                %% We've become a master. State is now opaque to
                %% us. When we became master, if Pid was still known
                %% to us then we'd have set up monitoring of it then,
                %% so this is now a noop.
                State
        end,
    %% Note that we do not remove our knowledge of this ChPid until we
    %% get the sender_death from GM as well as a DOWN notification.
    {ok, _TRef} = timer:apply_after(
                    ?DEATH_TIMEOUT, rabbit_amqqueue, run_backing_queue,
                    [self(), rabbit_mirror_queue_master, Fun]),
    ok.

forget_sender(_, running)                        -> false;
forget_sender(Down1, Down2) when Down1 =/= Down2 -> true.

%% Record and process lifetime events from channels. Forget all about a channel
%% only when down notifications are received from both the channel and from gm.
maybe_forget_sender(ChPid, ChState, State = #state { sender_queues = SQ,
                                                     msg_id_status = MS,
                                                     known_senders = KS }) ->
    case dict:find(ChPid, SQ) of
        error ->
            State;
        {ok, {MQ, PendCh, ChStateRecord}} ->
            case forget_sender(ChState, ChStateRecord) of
                true ->
                    credit_flow:peer_down(ChPid),
                    State #state { sender_queues = dict:erase(ChPid, SQ),
                                   msg_id_status = lists:foldl(
                                                     fun dict:erase/2,
                                                     MS, sets:to_list(PendCh)),
                                   known_senders = pmon:demonitor(ChPid, KS) };
                false ->
                    SQ1 = dict:store(ChPid, {MQ, PendCh, ChState}, SQ),
                    State #state { sender_queues = SQ1 }
            end
    end.

maybe_enqueue_message(
  Delivery = #delivery { message = #basic_message { id = MsgId },
                         sender  = ChPid },
  State = #state { sender_queues = SQ, msg_id_status = MS }) ->
    State1 = ensure_monitoring(ChPid, State),
    %% We will never see {published, ChPid, MsgSeqNo} here.
    case dict:find(MsgId, MS) of
        error ->
            {MQ, PendingCh, ChState} = get_sender_queue(ChPid, SQ),
            MQ1 = queue:in(Delivery, MQ),
            SQ1 = dict:store(ChPid, {MQ1, PendingCh, ChState}, SQ),
            State1 #state { sender_queues = SQ1 };
        {ok, Status} ->
            MS1 = send_or_record_confirm(
                    Status, Delivery, dict:erase(MsgId, MS), State1),
            SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
            State1 #state { msg_id_status = MS1,
                            sender_queues = SQ1 }
    end.

get_sender_queue(ChPid, SQ) ->
    case dict:find(ChPid, SQ) of
        error     -> {queue:new(), sets:new(), running};
        {ok, Val} -> Val
    end.

remove_from_pending_ch(MsgId, ChPid, SQ) ->
    case dict:find(ChPid, SQ) of
        error ->
            SQ;
        {ok, {MQ, PendingCh, ChState}} ->
            dict:store(ChPid, {MQ, sets:del_element(MsgId, PendingCh), ChState},
                       SQ)
    end.

publish_or_discard(Status, ChPid, MsgId,
                   State = #state { sender_queues = SQ, msg_id_status = MS }) ->
    %% We really are going to do the publish/discard right now, even
    %% though we may not have seen it directly from the channel. But
    %% we cannot issue confirms until the latter has happened. So we
    %% need to keep track of the MsgId and its confirmation status in
    %% the meantime.
    State1 = ensure_monitoring(ChPid, State),
    {MQ, PendingCh, ChState} = get_sender_queue(ChPid, SQ),
    {MQ1, PendingCh1, MS1} =
        case queue:out(MQ) of
            {empty, _MQ2} ->
                {MQ, sets:add_element(MsgId, PendingCh),
                 dict:store(MsgId, Status, MS)};
            {{value, Delivery = #delivery {
                       message = #basic_message { id = MsgId } }}, MQ2} ->
                {MQ2, PendingCh,
                 %% We received the msg from the channel first. Thus
                 %% we need to deal with confirms here.
                 send_or_record_confirm(Status, Delivery, MS, State1)};
            {{value, #delivery {}}, _MQ2} ->
                %% The instruction was sent to us before we were
                %% within the slave_pids within the #amqqueue{}
                %% record. We'll never receive the message directly
                %% from the channel. And the channel will not be
                %% expecting any confirms from us.
                {MQ, PendingCh, MS}
        end,
    SQ1 = dict:store(ChPid, {MQ1, PendingCh1, ChState}, SQ),
    State1 #state { sender_queues = SQ1, msg_id_status = MS1 }.


process_instruction({publish, ChPid, MsgProps,
                     Msg = #basic_message { id = MsgId }}, State) ->
    State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
        publish_or_discard(published, ChPid, MsgId, State),
    BQS1 = BQ:publish(Msg, MsgProps, true, ChPid, BQS),
    {ok, State1 #state { backing_queue_state = BQS1 }};
process_instruction({publish_delivered, ChPid, MsgProps,
                     Msg = #basic_message { id = MsgId }}, State) ->
    State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
        publish_or_discard(published, ChPid, MsgId, State),
    true = BQ:is_empty(BQS),
    {AckTag, BQS1} = BQ:publish_delivered(Msg, MsgProps, ChPid, BQS),
    {ok, maybe_store_ack(true, MsgId, AckTag,
                         State1 #state { backing_queue_state = BQS1 })};
process_instruction({discard, ChPid, MsgId}, State) ->
    State1 = #state { backing_queue = BQ, backing_queue_state = BQS } =
        publish_or_discard(discarded, ChPid, MsgId, State),
    BQS1 = BQ:discard(MsgId, ChPid, BQS),
    {ok, State1 #state { backing_queue_state = BQS1 }};
process_instruction({drop, Length, Dropped, AckRequired},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    QLen = BQ:len(BQS),
    ToDrop = case QLen - Length of
                 N when N > 0 -> N;
                 _            -> 0
             end,
    State1 = lists:foldl(
               fun (const, StateN = #state{backing_queue_state = BQSN}) ->
                       {{MsgId, AckTag}, BQSN1} = BQ:drop(AckRequired, BQSN),
                       maybe_store_ack(
                         AckRequired, MsgId, AckTag,
                         StateN #state { backing_queue_state = BQSN1 })
               end, State, lists:duplicate(ToDrop, const)),
    {ok, case AckRequired of
             true  -> State1;
             false -> update_delta(ToDrop - Dropped, State1)
         end};
process_instruction({ack, MsgIds},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    {AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
    {MsgIds1, BQS1} = BQ:ack(AckTags, BQS),
    [] = MsgIds1 -- MsgIds, %% ASSERTION
    {ok, update_delta(length(MsgIds1) - length(MsgIds),
                      State #state { msg_id_ack          = MA1,
                                     backing_queue_state = BQS1 })};
process_instruction({requeue, MsgIds},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    {AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
    {_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    {ok, State #state { msg_id_ack          = MA1,
                        backing_queue_state = BQS1 }};
process_instruction({sender_death, ChPid},
                    State = #state { known_senders = KS }) ->
    %% The channel will be monitored iff we have received a message
    %% from it. In this case we just want to avoid doing work if we
    %% never got any messages.
    {ok, case pmon:is_monitored(ChPid, KS) of
             false -> State;
             true  -> maybe_forget_sender(ChPid, down_from_gm, State)
         end};
process_instruction({depth, Depth},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    {ok, set_delta(Depth - BQ:depth(BQS), State)};

process_instruction({delete_and_terminate, Reason},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    BQ:delete_and_terminate(Reason, BQS),
    {stop, State #state { backing_queue_state = undefined }}.

msg_ids_to_acktags(MsgIds, MA) ->
    {AckTags, MA1} =
        lists:foldl(
          fun (MsgId, {Acc, MAN}) ->
                  case dict:find(MsgId, MA) of
                      error        -> {Acc, MAN};
                      {ok, AckTag} -> {[AckTag | Acc], dict:erase(MsgId, MAN)}
                  end
          end, {[], MA}, MsgIds),
    {lists:reverse(AckTags), MA1}.

maybe_store_ack(false, _MsgId, _AckTag, State) ->
    State;
maybe_store_ack(true, MsgId, AckTag, State = #state { msg_id_ack = MA }) ->
    State #state { msg_id_ack = dict:store(MsgId, AckTag, MA) }.

set_delta(0,        State = #state { depth_delta = undefined }) ->
    ok = record_synchronised(State#state.q),
    State #state { depth_delta = 0 };
set_delta(NewDelta, State = #state { depth_delta = undefined }) ->
    true = NewDelta > 0, %% assertion
    State #state { depth_delta = NewDelta };
set_delta(NewDelta, State = #state { depth_delta = Delta     }) ->
    update_delta(NewDelta - Delta, State).

update_delta(_DeltaChange, State = #state { depth_delta = undefined }) ->
    State;
update_delta( DeltaChange, State = #state { depth_delta = 0         }) ->
    0 = DeltaChange, %% assertion: we cannot become unsync'ed
    State;
update_delta( DeltaChange, State = #state { depth_delta = Delta     }) ->
    true = DeltaChange =< 0, %% assertion: we cannot become 'less' sync'ed
    set_delta(Delta + DeltaChange, State #state { depth_delta = undefined }).

update_ram_duration(BQ, BQS) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQ:set_ram_duration_target(DesiredDuration, BQS1).

%% [1] - the arrival of this newly synced slave may cause the master to die if
%% the admin has requested a migration-type change to policy.
record_synchronised(#amqqueue { name = QName }) ->
    Self = self(),
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   case mnesia:read({rabbit_queue, QName}) of
                       [] ->
                           ok;
                       [Q1 = #amqqueue { sync_slave_pids = SSPids }] ->
                           Q2 = Q1#amqqueue{sync_slave_pids = [Self | SSPids]},
                           rabbit_mirror_queue_misc:store_updated_slaves(Q2),
                           {ok, Q1, Q2}
                   end
           end) of
        ok           -> ok;
        {ok, Q1, Q2} -> rabbit_mirror_queue_misc:update_mirrors(Q1, Q2) %% [1]
    end.

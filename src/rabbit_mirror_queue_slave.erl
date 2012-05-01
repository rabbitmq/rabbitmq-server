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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2010-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_slave).

%% For general documentation of HA design, see
%% rabbit_mirror_queue_coordinator
%%
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
%% All instructions from the GM group must be processed in the order
%% in which they're received.

-export([start_link/1, set_maximum_since_use/2, info/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, prioritise_call/3,
         prioritise_cast/2, prioritise_info/2]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-include("gm_specs.hrl").

-ifdef(use_specs).
%% Shut dialyzer up
-spec(promote_me/2 :: (_, _) -> no_return()).
-endif.

%%----------------------------------------------------------------------------


-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         master_pid,
         is_synchronised
        ]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS).

-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).
-define(DEATH_TIMEOUT,                 20000). %% 20 seconds

-record(state, { q,
                 gm,
                 master_pid,
                 backing_queue,
                 backing_queue_state,
                 sync_timer_ref,
                 rate_timer_ref,

                 sender_queues, %% :: Pid -> {Q {Msg, Bool}, Set MsgId}
                 msg_id_ack,    %% :: MsgId -> AckTag
                 ack_num,

                 msg_id_status,
                 known_senders,

                 synchronised
               }).

start_link(Q) ->
    gen_server2:start_link(?MODULE, Q, []).

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

info(QPid) ->
    gen_server2:call(QPid, info, infinity).

init(#amqqueue { name = QueueName } = Q) ->
    Self = self(),
    Node = node(),
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   [Q1 = #amqqueue { pid = QPid, slave_pids = MPids }] =
                       mnesia:read({rabbit_queue, QueueName}),
                   case [Pid || Pid <- [QPid | MPids], node(Pid) =:= Node] of
                       []     -> MPids1 = MPids ++ [Self],
                                 ok = rabbit_amqqueue:store_queue(
                                        Q1 #amqqueue { slave_pids = MPids1 }),
                                 {new, QPid};
                       [SPid] -> true = rabbit_misc:is_process_alive(SPid),
                                 existing
                   end
           end) of
        {new, MPid} ->
            process_flag(trap_exit, true), %% amqqueue_process traps exits too.
            {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
            receive {joined, GM} ->
                    ok
            end,
            erlang:monitor(process, MPid),
            ok = file_handle_cache:register_callback(
                   rabbit_amqqueue, set_maximum_since_use, [Self]),
            ok = rabbit_memory_monitor:register(
                   Self, {rabbit_amqqueue, set_ram_duration_target, [Self]}),
            {ok, BQ} = application:get_env(backing_queue_module),
            BQS = bq_init(BQ, Q, false),
            State = #state { q                   = Q,
                             gm                  = GM,
                             master_pid          = MPid,
                             backing_queue       = BQ,
                             backing_queue_state = BQS,
                             rate_timer_ref      = undefined,
                             sync_timer_ref      = undefined,

                             sender_queues       = dict:new(),
                             msg_id_ack          = dict:new(),
                             ack_num             = 0,

                             msg_id_status       = dict:new(),
                             known_senders       = pmon:new(),

                             synchronised        = false
                   },
            rabbit_event:notify(queue_slave_created,
                                infos(?CREATION_EVENT_KEYS, State)),
            ok = gm:broadcast(GM, request_length),
            {ok, State, hibernate,
             {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
              ?DESIRED_HIBERNATE}};
        existing ->
            ignore
    end.

handle_call({deliver, Delivery = #delivery { immediate = true }},
            From, State) ->
    %% It is safe to reply 'false' here even if a) we've not seen the
    %% msg via gm, or b) the master dies before we receive the msg via
    %% gm. In the case of (a), we will eventually receive the msg via
    %% gm, and it's only the master's result to the channel that is
    %% important. In the case of (b), if the master does die and we do
    %% get promoted then at that point we have no consumers, thus
    %% 'false' is precisely the correct answer. However, we must be
    %% careful to _not_ enqueue the message in this case.

    %% Note this is distinct from the case where we receive the msg
    %% via gm first, then we're promoted to master, and only then do
    %% we receive the msg from the channel.
    gen_server2:reply(From, false), %% master may deliver it, not us
    noreply(maybe_enqueue_message(Delivery, false, State));

handle_call({deliver, Delivery = #delivery { mandatory = true }},
            From, State) ->
    gen_server2:reply(From, true), %% amqqueue throws away the result anyway
    noreply(maybe_enqueue_message(Delivery, true, State));

handle_call({gm_deaths, Deaths}, From,
            State = #state { q          = #amqqueue { name = QueueName },
                             gm         = GM,
                             master_pid = MPid }) ->
    %% The GM has told us about deaths, which means we're not going to
    %% receive any more messages from GM
    case rabbit_mirror_queue_misc:remove_from_queue(QueueName, Deaths) of
        {error, not_found} ->
            gen_server2:reply(From, ok),
            {stop, normal, State};
        {ok, Pid, DeadPids} ->
            rabbit_mirror_queue_misc:report_deaths(self(), false, QueueName,
                                                   DeadPids),
            if node(Pid) =:= node(MPid) ->
                    %% master hasn't changed
                    reply(ok, State);
               node(Pid) =:= node() ->
                    %% we've become master
                    promote_me(From, State);
               true ->
                    %% master has changed to not us.
                    gen_server2:reply(From, ok),
                    erlang:monitor(process, Pid),
                    ok = gm:broadcast(GM, heartbeat),
                    noreply(State #state { master_pid = Pid })
            end
    end;

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State).

handle_cast({run_backing_queue, Mod, Fun}, State) ->
    noreply(run_backing_queue(Mod, Fun, State));

handle_cast({gm, Instruction}, State) ->
    handle_process_result(process_instruction(Instruction, State));

handle_cast({deliver, Delivery = #delivery{sender = Sender}, Flow}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    case Flow of
        flow   -> credit_flow:ack(Sender);
        noflow -> ok
    end,
    noreply(maybe_enqueue_message(Delivery, true, State));

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast({set_ram_duration_target, Duration},
            State = #state { backing_queue       = BQ,
                             backing_queue_state = BQS }) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State #state { backing_queue_state = BQS1 }).

handle_info(update_ram_duration,
            State = #state { backing_queue = BQ,
                             backing_queue_state = BQS }) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    noreply(State #state { rate_timer_ref = just_measured,
                           backing_queue_state = BQS2 });

handle_info(sync_timeout, State) ->
    noreply(backing_queue_timeout(
              State #state { sync_timer_ref = undefined }));

handle_info(timeout, State) ->
    noreply(backing_queue_timeout(State));

handle_info({'DOWN', _MonitorRef, process, MPid, _Reason},
           State = #state { gm = GM, master_pid = MPid }) ->
    ok = gm:broadcast(GM, {process_death, MPid}),
    noreply(State);

handle_info({'DOWN', _MonitorRef, process, ChPid, _Reason}, State) ->
    noreply(local_sender_death(ChPid, State));

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
                   Q, BQ, BQS, RateTRef, [], [], pmon:new(), dict:new()),
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

prioritise_call(Msg, _From, _State) ->
    case Msg of
        info                                 -> 9;
        {gm_deaths, _Deaths}                 -> 5;
        _                                    -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        {gm, _Msg}                           -> 5;
        {post_commit, _Txn, _AckTags}        -> 4;
        _                                    -> 0
    end.

prioritise_info(Msg, _State) ->
    case Msg of
        update_ram_duration                  -> 8;
        sync_timeout                         -> 6;
        _                                    -> 0
    end.

%% ---------------------------------------------------------------------------
%% GM
%% ---------------------------------------------------------------------------

joined([SPid], _Members) ->
    SPid ! {joined, self()},
    ok.

members_changed([_SPid], _Births, []) ->
    ok;
members_changed([SPid], _Births, Deaths) ->
    inform_deaths(SPid, Deaths).

handle_msg([_SPid], _From, heartbeat) ->
    ok;
handle_msg([_SPid], _From, request_length) ->
    %% This is only of value to the master
    ok;
handle_msg([_SPid], _From, {ensure_monitoring, _Pid}) ->
    %% This is only of value to the master
    ok;
handle_msg([SPid], _From, {process_death, Pid}) ->
    inform_deaths(SPid, [Pid]);
handle_msg([SPid], _From, Msg) ->
    ok = gen_server2:cast(SPid, {gm, Msg}).

inform_deaths(SPid, Deaths) ->
    rabbit_misc:with_exit_handler(
      fun () -> {stop, normal} end,
      fun () ->
              case gen_server2:call(SPid, {gm_deaths, Deaths}, infinity) of
                  ok ->
                      ok;
                  {promote, CPid} ->
                      {become, rabbit_mirror_queue_coordinator, [CPid]}
              end
      end).

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(pid,             _State)                                   -> self();
i(name,            #state { q = #amqqueue { name = Name } }) -> Name;
i(master_pid,      #state { master_pid = MPid })             -> MPid;
i(is_synchronised, #state { synchronised = Synchronised })   -> Synchronised;
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

needs_confirming(#delivery{ msg_seq_no = undefined }, _State) ->
    never;
needs_confirming(#delivery { message = #basic_message {
                               is_persistent = true } },
                 #state { q = #amqqueue { durable = true } }) ->
    eventually;
needs_confirming(_Delivery, _State) ->
    immediately.

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
                      {ok, {published, ChPid}} ->
                          %% Still not seen it from the channel, just
                          %% record that it's been confirmed.
                          {CMsN, dict:store(MsgId, {confirmed, ChPid}, MSN)};
                      {ok, {published, ChPid, MsgSeqNo}} ->
                          %% Seen from both GM and Channel. Can now
                          %% confirm.
                          {rabbit_misc:gb_trees_cons(ChPid, MsgSeqNo, CMsN),
                           dict:erase(MsgId, MSN)};
                      {ok, {confirmed, _ChPid}} ->
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

promote_me(From, #state { q                   = Q = #amqqueue { name = QName },
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = RateTRef,
                          sender_queues       = SQ,
                          msg_id_ack          = MA,
                          msg_id_status       = MS,
                          known_senders       = KS }) ->
    rabbit_event:notify(queue_slave_promoted, [{pid,  self()},
                                               {name, QName}]),
    rabbit_log:info("Mirrored-queue (~s): Promoting slave ~s to master~n",
                    [rabbit_misc:rs(QName), rabbit_misc:pid_to_string(self())]),
    Q1 = Q #amqqueue { pid = self() },
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(
                   Q1, GM, rabbit_mirror_queue_master:sender_death_fun(),
                   rabbit_mirror_queue_master:length_fun()),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),
    ok = gm:confirmed_broadcast(GM, heartbeat),

    %% Everything that we're monitoring, we need to ensure our new
    %% coordinator is monitoring.
    MPids = pmon:monitored(KS),
    ok = rabbit_mirror_queue_coordinator:ensure_monitoring(CPid, MPids),

    %% We find all the messages that we've received from channels but
    %% not from gm, and if they're due to be enqueued on promotion
    %% then we pass them to the
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
    %% a) {published, ChPid}:
    %%   published via gm only; pending arrival of publication from
    %%   channel, maybe pending confirm.
    %%
    %% b) {published, ChPid, MsgSeqNo}:
    %%   published via gm and channel; pending confirm.
    %%
    %% c) {confirmed, ChPid}:
    %%   published via gm only, and confirmed; pending publication
    %%   from channel.
    %%
    %% d) discarded
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
    %%
    %% Everything that's in MA gets requeued. Consequently the new
    %% master should start with a fresh AM as there are no messages
    %% pending acks.

    MSList = dict:to_list(MS),
    SS = dict:from_list(
           [E || E = {_MsgId, discarded} <- MSList] ++
               [{MsgId, Status}
                || {MsgId, {Status, _ChPid}} <- MSList,
                   Status =:= published orelse Status =:= confirmed]),

    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    CPid, BQ, BQS, GM, SS, MPids),

    MTC = lists:foldl(fun ({MsgId, {published, ChPid, MsgSeqNo}}, MTC0) ->
                              gb_trees:insert(MsgId, {ChPid, MsgSeqNo}, MTC0);
                          (_, MTC0) ->
                              MTC0
                      end, gb_trees:empty(), MSList),
    NumAckTags = [NumAckTag || {_MsgId, NumAckTag} <- dict:to_list(MA)],
    AckTags = [AckTag || {_Num, AckTag} <- lists:sort(NumAckTags)],
    Deliveries = [Delivery || {_ChPid, {PubQ, _PendCh}} <- dict:to_list(SQ),
                              {Delivery, true} <- queue:to_list(PubQ)],
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q1, rabbit_mirror_queue_master, MasterState, RateTRef,
                   AckTags, Deliveries, KS, MTC),
    {become, rabbit_amqqueue_process, QueueState, hibernate}.

noreply(State) ->
    {NewState, Timeout} = next_state(State),
    {noreply, NewState, Timeout}.

reply(Reply, State) ->
    {NewState, Timeout} = next_state(State),
    {reply, Reply, NewState, Timeout}.

next_state(State = #state{backing_queue = BQ, backing_queue_state = BQS}) ->
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    State1 = ensure_rate_timer(
               confirm_messages(MsgIds, State #state {
                                          backing_queue_state = BQS1 })),
    case BQ:needs_timeout(BQS1) of
        false -> {stop_sync_timer(State1),   hibernate};
        idle  -> {stop_sync_timer(State1),   0        };
        timed -> {ensure_sync_timer(State1), 0        }
    end.

backing_queue_timeout(State = #state { backing_queue = BQ }) ->
    run_backing_queue(BQ, fun (M, BQS) -> M:timeout(BQS) end, State).

ensure_sync_timer(State = #state { sync_timer_ref = undefined }) ->
    TRef = erlang:send_after(?SYNC_INTERVAL, self(), sync_timeout),
    State #state { sync_timer_ref = TRef };
ensure_sync_timer(State) ->
    State.

stop_sync_timer(State = #state { sync_timer_ref = undefined }) ->
    State;
stop_sync_timer(State = #state { sync_timer_ref = TRef }) ->
    erlang:cancel_timer(TRef),
    State #state { sync_timer_ref = undefined }.

ensure_rate_timer(State = #state { rate_timer_ref = undefined }) ->
    TRef = erlang:send_after(?RAM_DURATION_UPDATE_INTERVAL,
                             self(), update_ram_duration),
    State #state { rate_timer_ref = TRef };
ensure_rate_timer(State = #state { rate_timer_ref = just_measured }) ->
    State #state { rate_timer_ref = undefined };
ensure_rate_timer(State) ->
    State.

stop_rate_timer(State = #state { rate_timer_ref = undefined }) ->
    State;
stop_rate_timer(State = #state { rate_timer_ref = just_measured }) ->
    State #state { rate_timer_ref = undefined };
stop_rate_timer(State = #state { rate_timer_ref = TRef }) ->
    erlang:cancel_timer(TRef),
    State #state { rate_timer_ref = undefined }.

ensure_monitoring(ChPid, State = #state { known_senders = KS }) ->
    State #state { known_senders = pmon:monitor(ChPid, KS) }.

local_sender_death(ChPid, State = #state { known_senders = KS }) ->
    ok = case pmon:is_monitored(ChPid, KS) of
             false -> ok;
             true  -> credit_flow:peer_down(ChPid),
                      confirm_sender_death(ChPid)
         end,
    State.

confirm_sender_death(Pid) ->
    %% We have to deal with the possibility that we'll be promoted to
    %% master before this thing gets run. Consequently we set the
    %% module to rabbit_mirror_queue_master so that if we do become a
    %% rabbit_amqqueue_process before then, sane things will happen.
    Fun =
        fun (?MODULE, State = #state { known_senders = KS,
                                       gm            = GM }) ->
                %% We're running still as a slave
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
    %% get the sender_death from GM.
    {ok, _TRef} = timer:apply_after(
                    ?DEATH_TIMEOUT, rabbit_amqqueue, run_backing_queue,
                    [self(), rabbit_mirror_queue_master, Fun]),
    ok.

maybe_enqueue_message(
  Delivery = #delivery { message    = #basic_message { id = MsgId },
                         msg_seq_no = MsgSeqNo,
                         sender     = ChPid },
  EnqueueOnPromotion,
  State = #state { sender_queues = SQ, msg_id_status = MS }) ->
    State1 = ensure_monitoring(ChPid, State),
    %% We will never see {published, ChPid, MsgSeqNo} here.
    case dict:find(MsgId, MS) of
        error ->
            {MQ, PendingCh} = get_sender_queue(ChPid, SQ),
            MQ1 = queue:in({Delivery, EnqueueOnPromotion}, MQ),
            SQ1 = dict:store(ChPid, {MQ1, PendingCh}, SQ),
            State1 #state { sender_queues = SQ1 };
        {ok, {confirmed, ChPid}} ->
            %% BQ has confirmed it but we didn't know what the
            %% msg_seq_no was at the time. We do now!
            ok = rabbit_misc:confirm_to_sender(ChPid, [MsgSeqNo]),
            SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
            State1 #state { sender_queues = SQ1,
                            msg_id_status = dict:erase(MsgId, MS) };
        {ok, {published, ChPid}} ->
            %% It was published to the BQ and we didn't know the
            %% msg_seq_no so couldn't confirm it at the time.
            case needs_confirming(Delivery, State1) of
                never ->
                    SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
                    State1 #state { msg_id_status = dict:erase(MsgId, MS),
                                    sender_queues = SQ1 };
                eventually ->
                    State1 #state {
                      msg_id_status =
                          dict:store(MsgId, {published, ChPid, MsgSeqNo}, MS) };
                immediately ->
                    ok = rabbit_misc:confirm_to_sender(ChPid, [MsgSeqNo]),
                    SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
                    State1 #state { msg_id_status = dict:erase(MsgId, MS),
                                    sender_queues = SQ1 }
            end;
        {ok, discarded} ->
            %% We've already heard from GM that the msg is to be
            %% discarded. We won't see this again.
            SQ1 = remove_from_pending_ch(MsgId, ChPid, SQ),
            State1 #state { msg_id_status = dict:erase(MsgId, MS),
                            sender_queues = SQ1 }
    end.

get_sender_queue(ChPid, SQ) ->
    case dict:find(ChPid, SQ) of
        error     -> {queue:new(), sets:new()};
        {ok, Val} -> Val
    end.

remove_from_pending_ch(MsgId, ChPid, SQ) ->
    case dict:find(ChPid, SQ) of
        error ->
            SQ;
        {ok, {MQ, PendingCh}} ->
            dict:store(ChPid, {MQ, sets:del_element(MsgId, PendingCh)}, SQ)
    end.

process_instruction(
  {publish, Deliver, ChPid, MsgProps, Msg = #basic_message { id = MsgId }},
  State = #state { sender_queues       = SQ,
                   backing_queue       = BQ,
                   backing_queue_state = BQS,
                   msg_id_status       = MS }) ->

    %% We really are going to do the publish right now, even though we
    %% may not have seen it directly from the channel. As a result, we
    %% may know that it needs confirming without knowing its
    %% msg_seq_no, which means that we can see the confirmation come
    %% back from the backing queue without knowing the msg_seq_no,
    %% which means that we're going to have to hang on to the fact
    %% that we've seen the msg_id confirmed until we can associate it
    %% with a msg_seq_no.
    State1 = ensure_monitoring(ChPid, State),
    {MQ, PendingCh} = get_sender_queue(ChPid, SQ),
    {MQ1, PendingCh1, MS1} =
        case queue:out(MQ) of
            {empty, _MQ2} ->
                {MQ, sets:add_element(MsgId, PendingCh),
                 dict:store(MsgId, {published, ChPid}, MS)};
            {{value, {Delivery = #delivery {
                        msg_seq_no = MsgSeqNo,
                        message    = #basic_message { id = MsgId } },
                      _EnqueueOnPromotion}}, MQ2} ->
                %% We received the msg from the channel first. Thus we
                %% need to deal with confirms here.
                case needs_confirming(Delivery, State1) of
                    never ->
                        {MQ2, PendingCh, MS};
                    eventually ->
                        {MQ2, PendingCh,
                         dict:store(MsgId, {published, ChPid, MsgSeqNo}, MS)};
                    immediately ->
                        ok = rabbit_misc:confirm_to_sender(ChPid, [MsgSeqNo]),
                        {MQ2, PendingCh, MS}
                end;
            {{value, {#delivery {}, _EnqueueOnPromotion}}, _MQ2} ->
                %% The instruction was sent to us before we were
                %% within the slave_pids within the #amqqueue{}
                %% record. We'll never receive the message directly
                %% from the channel. And the channel will not be
                %% expecting any confirms from us.
                {MQ, PendingCh, MS}
        end,

    SQ1 = dict:store(ChPid, {MQ1, PendingCh1}, SQ),
    State2 = State1 #state { sender_queues = SQ1, msg_id_status = MS1 },

    {ok,
     case Deliver of
         false ->
             BQS1 = BQ:publish(Msg, MsgProps, ChPid, BQS),
             State2 #state { backing_queue_state = BQS1 };
         {true, AckRequired} ->
             {AckTag, BQS1} = BQ:publish_delivered(AckRequired, Msg, MsgProps,
                                                   ChPid, BQS),
             maybe_store_ack(AckRequired, MsgId, AckTag,
                             State2 #state { backing_queue_state = BQS1 })
     end};
process_instruction({discard, ChPid, Msg = #basic_message { id = MsgId }},
                    State = #state { sender_queues       = SQ,
                                     backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_status       = MS }) ->
    %% Many of the comments around the publish head above apply here
    %% too.
    State1 = ensure_monitoring(ChPid, State),
    {MQ, PendingCh} = get_sender_queue(ChPid, SQ),
    {MQ1, PendingCh1, MS1} =
        case queue:out(MQ) of
            {empty, _MQ} ->
                {MQ, sets:add_element(MsgId, PendingCh),
                 dict:store(MsgId, discarded, MS)};
            {{value, {#delivery { message = #basic_message { id = MsgId } },
                      _EnqueueOnPromotion}}, MQ2} ->
                %% We've already seen it from the channel, we're not
                %% going to see this again, so don't add it to MS
                {MQ2, PendingCh, MS};
            {{value, {#delivery {}, _EnqueueOnPromotion}}, _MQ2} ->
                %% The instruction was sent to us before we were
                %% within the slave_pids within the #amqqueue{}
                %% record. We'll never receive the message directly
                %% from the channel.
                {MQ, PendingCh, MS}
        end,
    SQ1 = dict:store(ChPid, {MQ1, PendingCh1}, SQ),
    BQS1 = BQ:discard(Msg, ChPid, BQS),
    {ok, State1 #state { sender_queues       = SQ1,
                         msg_id_status       = MS1,
                         backing_queue_state = BQS1 }};
process_instruction({set_length, Length, AckRequired},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    QLen = BQ:len(BQS),
    ToDrop = QLen - Length,
    {ok,
     case ToDrop >= 0 of
         true ->
             State1 =
                 lists:foldl(
                   fun (const, StateN = #state {backing_queue_state = BQSN}) ->
                           {{#basic_message{id = MsgId}, _IsDelivered, AckTag,
                             _Remaining}, BQSN1} = BQ:fetch(AckRequired, BQSN),
                           maybe_store_ack(
                             AckRequired, MsgId, AckTag,
                             StateN #state { backing_queue_state = BQSN1 })
                   end, State, lists:duplicate(ToDrop, const)),
             set_synchronised(true, State1);
         false ->
             State
     end};
process_instruction({fetch, AckRequired, MsgId, Remaining},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    QLen = BQ:len(BQS),
    {ok, case QLen - 1 of
             Remaining ->
                 {{#basic_message{id = MsgId}, _IsDelivered,
                   AckTag, Remaining}, BQS1} = BQ:fetch(AckRequired, BQS),
                 maybe_store_ack(AckRequired, MsgId, AckTag,
                                 State #state { backing_queue_state = BQS1 });
             Other when Other + 1 =:= Remaining ->
                 set_synchronised(true, State);
             Other when Other < Remaining ->
                 %% we must be shorter than the master
                 State
         end};
process_instruction({ack, MsgIds},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    {AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
    {MsgIds1, BQS1} = BQ:ack(AckTags, BQS),
    [] = MsgIds1 -- MsgIds, %% ASSERTION
    {ok, State #state { msg_id_ack          = MA1,
                        backing_queue_state = BQS1 }};
process_instruction({requeue, MsgIds},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    {AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
    {ok, case length(AckTags) =:= length(MsgIds) of
             true ->
                 {MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
                 State #state { msg_id_ack          = MA1,
                                backing_queue_state = BQS1 };
             false ->
                 %% The only thing we can safely do is nuke out our BQ
                 %% and MA. The interaction between this and confirms
                 %% doesn't really bear thinking about...
                 {_Count, BQS1} = BQ:purge(BQS),
                 {_MsgIds, BQS2} = ack_all(BQ, MA, BQS1),
                 State #state { msg_id_ack          = dict:new(),
                                backing_queue_state = BQS2 }
         end};
process_instruction({sender_death, ChPid},
                    State = #state { sender_queues = SQ,
                                     msg_id_status = MS,
                                     known_senders = KS }) ->
    {ok, case pmon:is_monitored(ChPid, KS) of
             false -> State;
             true  -> MS1 = case dict:find(ChPid, SQ) of
                                error ->
                                    MS;
                                {ok, {_MQ, PendingCh}} ->
                                    lists:foldl(fun dict:erase/2, MS,
                                                sets:to_list(PendingCh))
                            end,
                      State #state { sender_queues = dict:erase(ChPid, SQ),
                                     msg_id_status = MS1,
                                     known_senders = pmon:demonitor(ChPid, KS) }
         end};
process_instruction({length, Length},
                    State = #state { backing_queue = BQ,
                                     backing_queue_state = BQS }) ->
    {ok, set_synchronised(Length =:= BQ:len(BQS), State)};
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
                      error                -> {Acc, MAN};
                      {ok, {_Num, AckTag}} -> {[AckTag | Acc],
                                               dict:erase(MsgId, MAN)}
                  end
          end, {[], MA}, MsgIds),
    {lists:reverse(AckTags), MA1}.

ack_all(BQ, MA, BQS) ->
    BQ:ack([AckTag || {_MsgId, {_Num, AckTag}} <- dict:to_list(MA)], BQS).

maybe_store_ack(false, _MsgId, _AckTag, State) ->
    State;
maybe_store_ack(true, MsgId, AckTag, State = #state { msg_id_ack = MA,
                                                      ack_num    = Num }) ->
    State #state { msg_id_ack = dict:store(MsgId, {Num, AckTag}, MA),
                   ack_num    = Num + 1 }.

%% We intentionally leave out the head where a slave becomes
%% unsynchronised: we assert that can never happen.
set_synchronised(true, State = #state { q = #amqqueue { name = QName },
                                        synchronised = false }) ->
    rabbit_event:notify(queue_slave_synchronised, [{pid,  self()},
                                                   {name, QName}]),
    State #state { synchronised = true };
set_synchronised(true, State) ->
    State;
set_synchronised(false, State = #state { synchronised = false }) ->
    State.

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
%% Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mirror_queue_slave).

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

-export([start_link/1, set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1, prioritise_call/3,
         prioritise_cast/2]).

-export([joined/2, members_changed/3, handle_msg/3]).

-behaviour(gen_server2).
-behaviour(gm).

-include("rabbit.hrl").
-include("gm_specs.hrl").

-record(state, { q,
                 gm,
                 master_node,
                 backing_queue,
                 backing_queue_state,
                 sync_timer_ref,
                 rate_timer_ref,

                 sender_queues, %% :: Pid -> MsgQ
                 msg_id_ack,    %% :: MsgId -> AckTag

                 msg_id_status
               }).

-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

start_link(Q) ->
    gen_server2:start_link(?MODULE, [Q], []).

set_maximum_since_use(QPid, Age) ->
    gen_server2:cast(QPid, {set_maximum_since_use, Age}).

init([#amqqueue { name = QueueName } = Q]) ->
    process_flag(trap_exit, true), %% amqqueue_process traps exits too.
    ok = gm:create_tables(),
    {ok, GM} = gm:start_link(QueueName, ?MODULE, [self()]),
    receive {joined, GM} ->
            ok
    end,
    Self = self(),
    Node = node(),
    {ok, MPid} =
        rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  [Q1 = #amqqueue { pid = QPid, mirror_pids = MPids }] =
                      mnesia:read({rabbit_queue, QueueName}),
                  %% ASSERTION
                  [] = [Pid || Pid <- [QPid | MPids], node(Pid) =:= Node],
                  MPids1 = MPids ++ [Self],
                  mnesia:write(rabbit_queue,
                               Q1 #amqqueue { mirror_pids = MPids1 },
                               write),
                  {ok, QPid}
          end),
    ok = file_handle_cache:register_callback(
           rabbit_amqqueue, set_maximum_since_use, [self()]),
    ok = rabbit_memory_monitor:register(
           self(), {rabbit_amqqueue, set_ram_duration_target, [self()]}),
    {ok, BQ} = application:get_env(backing_queue_module),
    BQS = bq_init(BQ, Q, false),
    {ok, #state { q                   = Q,
                  gm                  = GM,
                  master_node         = node(MPid),
                  backing_queue       = BQ,
                  backing_queue_state = BQS,
                  rate_timer_ref      = undefined,
                  sync_timer_ref      = undefined,

                  sender_queues       = dict:new(),
                  msg_id_ack          = dict:new(),
                  msg_id_status       = dict:new()
                }, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

handle_call({deliver_immediately, Delivery = #delivery {}}, From, State) ->
    %% Synchronous, "immediate" delivery mode

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

handle_call({deliver, Delivery = #delivery {}}, From, State) ->
    %% Synchronous, "mandatory" delivery mode
    gen_server2:reply(From, true), %% amqqueue throws away the result anyway
    noreply(maybe_enqueue_message(Delivery, true, State));

handle_call({gm_deaths, Deaths}, From,
            State = #state { q           = #amqqueue { name = QueueName },
                             gm          = GM,
                             master_node = MNode }) ->
    rabbit_log:info("Slave ~p saw deaths ~p for ~s~n",
                    [self(), Deaths, rabbit_misc:rs(QueueName)]),
    %% The GM has told us about deaths, which means we're not going to
    %% receive any more messages from GM
    case rabbit_mirror_queue_misc:remove_from_queue(QueueName, Deaths) of
        {ok, Pid} when node(Pid) =:= MNode ->
            reply(ok, State);
        {ok, Pid} when node(Pid) =:= node() ->
            promote_me(From, State);
        {ok, Pid} ->
            gen_server2:reply(From, ok),
            ok = gm:broadcast(GM, heartbeat),
            noreply(State #state { master_node = node(Pid) });
        {error, not_found} ->
            gen_server2:reply(From, ok),
            {stop, normal, State}
    end;

handle_call({run_backing_queue, Mod, Fun}, _From, State) ->
    reply(ok, run_backing_queue(Mod, Fun, State));

handle_call({commit, _Txn, _ChPid}, _From, State) ->
    %% We don't support transactions in mirror queues
    reply(ok, State).

handle_cast({run_backing_queue, Mod, Fun}, State) ->
    noreply(run_backing_queue(Mod, Fun, State));

handle_cast({gm, Instruction}, State) ->
    handle_process_result(process_instruction(Instruction, State));

handle_cast({deliver, Delivery = #delivery {}}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    noreply(maybe_enqueue_message(Delivery, true, State));

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast({set_ram_duration_target, Duration},
            State = #state { backing_queue       = BQ,
                             backing_queue_state = BQS }) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State #state { backing_queue_state = BQS1 });

handle_cast(update_ram_duration,
            State = #state { backing_queue = BQ,
                             backing_queue_state = BQS }) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    noreply(State #state { rate_timer_ref = just_measured,
                           backing_queue_state = BQS2 });

handle_cast(sync_timeout, State) ->
    noreply(backing_queue_idle_timeout(
              State #state { sync_timer_ref = undefined }));

handle_cast({rollback, _Txn, _ChPid}, State) ->
    %% We don't support transactions in mirror queues
    noreply(State).

handle_info(timeout, State) ->
    noreply(backing_queue_idle_timeout(State));

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
terminate(Reason, #state { q                   = Q,
                           gm                  = GM,
                           backing_queue       = BQ,
                           backing_queue_state = BQS,
                           rate_timer_ref      = RateTRef }) ->
    ok = gm:leave(GM),
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, BQ, BQS, RateTRef, [], [], dict:new()),
    rabbit_amqqueue_process:terminate(Reason, QueueState);
terminate([_SPid], _Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    %% mainly copied from amqqueue_process
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    BQS3 = BQ:handle_pre_hibernate(BQS2),
    {hibernate, stop_rate_timer(State #state { backing_queue_state = BQS3 })}.

prioritise_call(Msg, _From, _State) ->
    case Msg of
        {run_backing_queue, _Mod, _Fun} -> 6;
        {gm_deaths, _Deaths}            -> 5;
        _                               -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        update_ram_duration                  -> 8;
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        sync_timeout                         -> 6;
        {gm, _Msg}                           -> 5;
        {post_commit, _Txn, _AckTags}        -> 4;
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

handle_msg([_SPid], _From, heartbeat) ->
    ok;
handle_msg([SPid], _From, Msg) ->
    ok = gen_server2:cast(SPid, {gm, Msg}).

%% ---------------------------------------------------------------------------
%% Others
%% ---------------------------------------------------------------------------

bq_init(BQ, Q, Recover) ->
    Self = self(),
    BQ:init(Q, Recover,
            fun (Mod, Fun) ->
                    rabbit_amqqueue:run_backing_queue_async(Self, Mod, Fun)
            end,
            fun (Mod, Fun) ->
                    rabbit_misc:with_exit_handler(
                      fun () -> error end,
                      fun () ->
                              rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
                      end)
            end).

run_backing_queue(rabbit_mirror_queue_master, Fun, State) ->
    %% Yes, this might look a little crazy, but see comments around
    %% process_instruction({tx_commit,...}, State).
    Fun(rabbit_mirror_queue_master, State);
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
    {MS1, CMs} =
        lists:foldl(
          fun (MsgId, {MSN, CMsN} = Acc) ->
                  %% We will never see 'discarded' here
                  case dict:find(MsgId, MSN) of
                      error ->
                          %% If it needed confirming, it'll have
                          %% already been done.
                          Acc;
                      {ok, {published, ChPid}} ->
                          %% Still not seen it from the channel, just
                          %% record that it's been confirmed.
                          {dict:store(MsgId, {confirmed, ChPid}, MSN), CMsN};
                      {ok, {published, ChPid, MsgSeqNo}} ->
                          %% Seen from both GM and Channel. Can now
                          %% confirm.
                          {dict:erase(MsgId, MSN),
                           gb_trees_cons(ChPid, MsgSeqNo, CMsN)};
                      {ok, {confirmed, _ChPid}} ->
                          %% It's already been confirmed. This is
                          %% probably it's been both sync'd to disk
                          %% and then delivered and ack'd before we've
                          %% seen the publish from the
                          %% channel. Nothing to do here.
                          Acc
                  end
          end, {MS, gb_trees:empty()}, MsgIds),
    gb_trees:map(fun (ChPid, MsgSeqNos) ->
                         ok = rabbit_channel:confirm(ChPid, MsgSeqNos)
                 end, CMs),
    State #state { msg_id_status = MS1 }.

gb_trees_cons(Key, Value, Tree) ->
    case gb_trees:lookup(Key, Tree) of
        {value, Values} -> gb_trees:update(Key, [Value | Values], Tree);
        none            -> gb_trees:insert(Key, [Value], Tree)
    end.

handle_process_result({ok,   State}) -> noreply(State);
handle_process_result({stop, State}) -> {stop, normal, State}.

promote_me(From, #state { q                   = Q,
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = RateTRef,
                          sender_queues       = SQ,
                          msg_id_ack          = MA,
                          msg_id_status       = MS }) ->
    rabbit_log:info("Promoting slave ~p for ~s~n",
                    [self(), rabbit_misc:rs(Q #amqqueue.name)]),
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(Q, GM),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),
    ok = gm:confirmed_broadcast(GM, heartbeat),

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
    %% pending acks (txns will have been rolled back).

    MSList = dict:to_list(MS),
    SS = dict:from_list(
           [E || E = {_MsgId, discarded} <- MSList] ++
               [{MsgId, Status}
                || {MsgId, {Status, _ChPid}} <- MSList,
                   Status =:= published orelse Status =:= confirmed]),

    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    CPid, BQ, BQS, GM, SS),

    MTC = dict:from_list(
            [{MsgId, {ChPid, MsgSeqNo}} ||
                {MsgId, {published, ChPid, MsgSeqNo}} <- dict:to_list(MS)]),
    AckTags = [AckTag || {_MsgId, AckTag} <- dict:to_list(MA)],
    Deliveries = [Delivery || {_ChPid, PubQ} <- dict:to_list(SQ),
                              {Delivery, true} <- queue:to_list(PubQ)],
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, rabbit_mirror_queue_master, MasterState, RateTRef,
                   AckTags, Deliveries, MTC),
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
    case BQ:needs_idle_timeout(BQS1) of
        true  -> {ensure_sync_timer(State1), 0};
        false -> {stop_sync_timer(State1), hibernate}
    end.

%% copied+pasted from amqqueue_process
backing_queue_idle_timeout(State = #state { backing_queue = BQ }) ->
    run_backing_queue(BQ, fun (M, BQS) -> M:idle_timeout(BQS) end, State).

ensure_sync_timer(State = #state { sync_timer_ref = undefined }) ->
    {ok, TRef} = timer:apply_after(
                   ?SYNC_INTERVAL, rabbit_amqqueue, sync_timeout, [self()]),
    State #state { sync_timer_ref = TRef };
ensure_sync_timer(State) ->
    State.

stop_sync_timer(State = #state { sync_timer_ref = undefined }) ->
    State;
stop_sync_timer(State = #state { sync_timer_ref = TRef }) ->
    {ok, cancel} = timer:cancel(TRef),
    State #state { sync_timer_ref = undefined }.

ensure_rate_timer(State = #state { rate_timer_ref = undefined }) ->
    {ok, TRef} = timer:apply_after(
                   ?RAM_DURATION_UPDATE_INTERVAL,
                   rabbit_amqqueue, update_ram_duration,
                   [self()]),
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
    {ok, cancel} = timer:cancel(TRef),
    State #state { rate_timer_ref = undefined }.

maybe_enqueue_message(
  Delivery = #delivery { message    = #basic_message { id = MsgId },
                         msg_seq_no = MsgSeqNo,
                         sender     = ChPid,
                         txn        = none },
  EnqueueOnPromotion,
  State = #state { sender_queues = SQ,
                   msg_id_status = MS }) ->
    %% We will never see {published, ChPid, MsgSeqNo} here.
    case dict:find(MsgId, MS) of
        error ->
            MQ = case dict:find(ChPid, SQ) of
                     {ok, MQ1} -> MQ1;
                     error    -> queue:new()
                 end,
            SQ1 = dict:store(ChPid,
                             queue:in({Delivery, EnqueueOnPromotion}, MQ), SQ),
            State #state { sender_queues = SQ1 };
        {ok, {confirmed, ChPid}} ->
            %% BQ has confirmed it but we didn't know what the
            %% msg_seq_no was at the time. We do now!
            ok = rabbit_channel:confirm(ChPid, [MsgSeqNo]),
            State #state { msg_id_status = dict:erase(MsgId, MS) };
        {ok, {published, ChPid}} ->
            %% It was published to the BQ and we didn't know the
            %% msg_seq_no so couldn't confirm it at the time.
            case needs_confirming(Delivery, State) of
                never ->
                    State #state { msg_id_status = dict:erase(MsgId, MS) };
                eventually ->
                    State #state {
                      msg_id_status =
                          dict:store(MsgId, {published, ChPid, MsgSeqNo}, MS) };
                immediately ->
                    ok = rabbit_channel:confirm(ChPid, [MsgSeqNo]),
                    State #state { msg_id_status = dict:erase(MsgId, MS) }
            end;
        {ok, discarded} ->
            %% We've already heard from GM that the msg is to be
            %% discarded. We won't see this again.
            State #state { msg_id_status = dict:erase(MsgId, MS) }
    end;
maybe_enqueue_message(_Delivery, _EnqueueOnPromotion, State) ->
    %% We don't support txns in mirror queues.
    State.

process_instruction(
  {publish, Deliver, ChPid, MsgProps, Msg = #basic_message { id = MsgId }},
  State = #state { sender_queues       = SQ,
                   backing_queue       = BQ,
                   backing_queue_state = BQS,
                   msg_id_ack          = MA,
                   msg_id_status       = MS }) ->

    %% We really are going to do the publish right now, even though we
    %% may not have seen it directly from the channel. As a result, we
    %% may know that it needs confirming without knowing its
    %% msg_seq_no, which means that we can see the confirmation come
    %% back from the backing queue without knowing the msg_seq_no,
    %% which means that we're going to have to hang on to the fact
    %% that we've seen the msg_id confirmed until we can associate it
    %% with a msg_seq_no.
    MS1 = dict:store(MsgId, {published, ChPid}, MS),
    {SQ1, MS2} =
        case dict:find(ChPid, SQ) of
            error ->
                {SQ, MS1};
            {ok, MQ} ->
                case queue:out(MQ) of
                    {empty, _MQ} ->
                        {SQ, MS1};
                    {{value, {Delivery = #delivery {
                                msg_seq_no = MsgSeqNo,
                                message    = #basic_message { id = MsgId } },
                              _EnqueueOnPromotion}}, MQ1} ->
                        %% We received the msg from the channel
                        %% first. Thus we need to deal with confirms
                        %% here.
                        {dict:store(ChPid, MQ1, SQ),
                         case needs_confirming(Delivery, State) of
                             never ->
                                 MS;
                             eventually ->
                                 dict:store(
                                   MsgId, {published, ChPid, MsgSeqNo}, MS);
                             immediately ->
                                 ok = rabbit_channel:confirm(ChPid, [MsgSeqNo]),
                                 MS
                         end};
                    {{value, {#delivery {}, _EnqueueOnPromotion}}, _MQ1} ->
                        %% The instruction was sent to us before we
                        %% were within the mirror_pids within the
                        %% #amqqueue{} record. We'll never receive the
                        %% message directly from the channel. And the
                        %% channel will not be expecting any confirms
                        %% from us.
                        {SQ, MS}
                end
        end,

    State1 = State #state { sender_queues = SQ1,
                            msg_id_status = MS2 },

    {ok,
     case Deliver of
         false ->
             BQS1 = BQ:publish(Msg, MsgProps, ChPid, BQS),
             State1 #state { backing_queue_state = BQS1 };
         {true, AckRequired} ->
             {AckTag, BQS1} = BQ:publish_delivered(AckRequired, Msg, MsgProps,
                                                   ChPid, BQS),
             MA1 = case AckRequired of
                       true  -> dict:store(MsgId, AckTag, MA);
                       false -> MA
                   end,
             State1 #state { backing_queue_state = BQS1,
                             msg_id_ack          = MA1 }
     end};
process_instruction({discard, ChPid, Msg = #basic_message { id = MsgId }},
                    State = #state { sender_queues       = SQ,
                                     backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_status       = MS }) ->
    %% Many of the comments around the publish head above apply here
    %% too.
    MS1 = dict:store(MsgId, discarded, MS),
    {SQ1, MS2} =
        case dict:find(ChPid, SQ) of
            error ->
                {SQ, MS1};
            {ok, MQ} ->
                case queue:out(MQ) of
                    {empty, _MQ} ->
                        {SQ, MS1};
                    {{value, {#delivery {
                                  message = #basic_message { id = MsgId } },
                              _EnqueueOnPromotion}}, MQ1} ->
                        %% We've already seen it from the channel,
                        %% we're not going to see this again, so don't
                        %% add it to MS
                        {dict:store(ChPid, MQ1, SQ), MS};
                    {{value, {#delivery {}, _EnqueueOnPromotion}}, _MQ1} ->
                        %% The instruction was sent to us before we
                        %% were within the mirror_pids within the
                        %% #amqqueue{} record. We'll never receive the
                        %% message directly from the channel.
                        {SQ, MS}
                end
        end,
    BQS1 = BQ:discard(Msg, ChPid, BQS),
    {ok, State #state { sender_queues       = SQ1,
                        msg_id_status       = MS2,
                        backing_queue_state = BQS1 }};
process_instruction({set_length, Length},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    QLen = BQ:len(BQS),
    ToDrop = QLen - Length,
    {ok, case ToDrop > 0 of
             true  -> BQS1 =
                          lists:foldl(
                            fun (const, BQSN) ->
                                    {{_Msg, _IsDelivered, _AckTag, _Remaining},
                                     BQSN1} = BQ:fetch(false, BQSN),
                                    BQSN1
                            end, BQS, lists:duplicate(ToDrop, const)),
                      State #state { backing_queue_state = BQS1 };
             false -> State
         end};
process_instruction({fetch, AckRequired, MsgId, Remaining},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    QLen = BQ:len(BQS),
    {ok, case QLen - 1 of
             Remaining ->
                 {{_Msg, _IsDelivered, AckTag, Remaining}, BQS1} =
                     BQ:fetch(AckRequired, BQS),
                 MA1 = case AckRequired of
                           true  -> dict:store(MsgId, AckTag, MA);
                           false -> MA
                       end,
                 State #state { backing_queue_state = BQS1,
                                msg_id_ack          = MA1 };
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
process_instruction({requeue, MsgPropsFun, MsgIds},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     msg_id_ack          = MA }) ->
    {AckTags, MA1} = msg_ids_to_acktags(MsgIds, MA),
    {ok, case length(AckTags) =:= length(MsgIds) of
             true ->
                 {MsgIds, BQS1} = BQ:requeue(AckTags, MsgPropsFun, BQS),
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
process_instruction(delete_and_terminate,
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    BQ:delete_and_terminate(BQS),
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

ack_all(BQ, MA, BQS) ->
    BQ:ack([AckTag || {_MsgId, AckTag} <- dict:to_list(MA)], BQS).

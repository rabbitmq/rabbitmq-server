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
%%
%% Thus, we need a queue per sender, and a queue for GM instructions.
%%
%% On receipt of a GM group instruction, three things are possible:
%% 1. The queue of publisher messages is empty. Thus store the GM
%%    instruction to the instrQ.
%% 2. The head of the queue of publisher messages has a message that
%%    matches the GUID of the GM instruction. Remove the message, and
%%    route appropriately.
%% 3. The head of the queue of publisher messages has a message that
%%    does not match the GUID of the GM instruction. Throw away the GM
%%    instruction: the GM instruction must correspond to a message
%%    that we'll never receive. If it did not, then before the current
%%    instruction, we would have received an instruction for the
%%    message at the head of this queue, thus the head of the queue
%%    would have been removed and processed.
%%
%% On receipt of a publisher message, three things are possible:
%% 1. The queue of GM group instructions is empty. Add the message to
%%    the relevant queue and await instructions from the GM.
%% 2. The head of the queue of GM group instructions has an
%%    instruction matching the GUID of the message. Remove that
%%    instruction and act on it. Attempt to process the rest of the
%%    instrQ.
%% 3. The head of the queue of GM group instructions has an
%%    instruction that does not match the GUID of the message. If the
%%    message is from the same publisher as is referred to by the
%%    instruction then throw away the GM group instruction and repeat
%%    - attempt to match against the next instruction if there is one:
%%    The instruction thrown away was for a message we'll never
%%    receive.
%%
%% In all cases, we are relying heavily on order preserving messaging
%% both from the GM group and from the publishers.

-export([start_link/1, set_maximum_since_use/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, handle_pre_hibernate/1]).

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
                 rate_timer_ref,

                 sender_queues, %% :: Pid -> MsgQ
                 guid_ack,      %% :: Guid -> AckTag
                 seen,          %% Set Guid

                 guid_to_channel %% for confirms
               }).

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
    case rabbit_misc:execute_mnesia_transaction(
           fun () ->
                   [Q1 = #amqqueue { pid = QPid, mirror_pids = MPids }] =
                       mnesia:read({rabbit_queue, QueueName}),
                   case [Pid || Pid <- [QPid | MPids], node(Pid) =:= Node] of
                       [] ->
                           MPids1 = MPids ++ [Self],
                           mnesia:write(rabbit_queue,
                                        Q1 #amqqueue { mirror_pids = MPids1 },
                                        write),
                           {ok, QPid};
                       _ ->
                           {error, node_already_present}
                   end
           end) of
        {ok, MPid} ->
            ok = file_handle_cache:register_callback(
                   rabbit_amqqueue, set_maximum_since_use, [self()]),
            ok = rabbit_memory_monitor:register(
                   self(), {rabbit_amqqueue, set_ram_duration_target,
                            [self()]}),
            {ok, BQ} = application:get_env(backing_queue_module),
            BQS = BQ:init(Q, false),
            {ok, #state { q                   = Q,
                          gm                  = GM,
                          master_node         = node(MPid),
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = undefined,

                          sender_queues       = dict:new(),
                          guid_ack            = dict:new(),
                          seen                = sets:new(),

                          guid_to_channel     = dict:new()
                        }, hibernate,
             {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN,
              ?DESIRED_HIBERNATE}};
        {error, Error} ->
            {stop, Error}
    end.

handle_call({deliver_immediately, Delivery = #delivery {}}, From, State) ->
    %% Synchronous, "immediate" delivery mode
    gen_server2:reply(From, false), %% master may deliver it, not us
    noreply(maybe_enqueue_message(Delivery, State));

handle_call({deliver, Delivery = #delivery {}}, From, State) ->
    %% Synchronous, "mandatory" delivery mode
    gen_server2:reply(From, true), %% amqqueue throws away the result anyway
    noreply(maybe_enqueue_message(Delivery, State));

handle_call({gm_deaths, Deaths}, From,
            State = #state { q           = #amqqueue { name = QueueName },
                             gm          = GM,
                             master_node = MNode }) ->
    rabbit_log:info("Slave ~p saw deaths ~p for queue ~p~n",
                    [self(), Deaths, QueueName]),
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

handle_call({maybe_run_queue_via_backing_queue, Mod, Fun}, _From, State) ->
    reply(ok, maybe_run_queue_via_backing_queue(Mod, Fun, State)).


handle_cast({maybe_run_queue_via_backing_queue, Mod, Fun}, State) ->
    noreply(maybe_run_queue_via_backing_queue(Mod, Fun, State));

handle_cast({gm, Instruction}, State) ->
    handle_process_result(process_instruction(Instruction, State));

handle_cast({deliver, Delivery = #delivery {}}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    noreply(maybe_enqueue_message(Delivery, State));

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
                           backing_queue_state = BQS2 }).

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
                   Q, BQ, BQS, RateTRef, [], []),
    rabbit_amqqueue_process:terminate(Reason, QueueState);
terminate([_SPid], _Reason) ->
    %% gm case
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_pre_hibernate(State = #state { backing_queue       = BQ,
                                      backing_queue_state = BQS }) ->
    %% mainly copied from amqqueue_process
    BQS1 = BQ:handle_pre_hibernate(BQS),
    %% no activity for a while == 0 egress and ingress rates
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), infinity),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    {hibernate, stop_rate_timer(State #state { backing_queue_state = BQS2 })}.

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

maybe_run_queue_via_backing_queue(
  Mod, Fun, State = #state { backing_queue       = BQ,
                             backing_queue_state = BQS,
                             guid_to_channel     = GTC }) ->
    {Guids, BQS1} = BQ:invoke(Mod, Fun, BQS),
    GTC1 = lists:foldl(fun maybe_confirm_message/2, GTC, Guids),
    State #state { backing_queue_state = BQS1,
                   guid_to_channel     = GTC1 }.

record_confirm_or_confirm(#delivery { msg_seq_no = undefined }, _Q, GTC) ->
    GTC;
record_confirm_or_confirm(
  #delivery { sender     = ChPid,
              message    = #basic_message { is_persistent = true,
                                            guid          = Guid },
              msg_seq_no = MsgSeqNo }, #amqqueue { durable = true }, GTC) ->
    dict:store(Guid, {ChPid, MsgSeqNo}, GTC);
record_confirm_or_confirm(#delivery { sender = ChPid, msg_seq_no = MsgSeqNo },
                          _Q, GTC) ->
    ok = rabbit_channel:confirm(ChPid, MsgSeqNo),
    GTC.

maybe_confirm_message(Guid, GTC) ->
    case dict:find(Guid, GTC) of
        {ok, {ChPid, MsgSeqNo}} when MsgSeqNo =/= undefined ->
            ok = rabbit_channel:confirm(ChPid, MsgSeqNo),
            dict:erase(Guid, GTC);
        error ->
            GTC
    end.

handle_process_result({ok,   State}) -> noreply(State);
handle_process_result({stop, State}) -> {stop, normal, State}.

promote_me(From, #state { q                   = Q,
                          gm                  = GM,
                          backing_queue       = BQ,
                          backing_queue_state = BQS,
                          rate_timer_ref      = RateTRef,
                          sender_queues       = SQ,
                          seen                = Seen,
                          guid_ack            = GA }) ->
    rabbit_log:info("Promoting slave ~p for queue ~p~n",
                    [self(), Q #amqqueue.name]),
    {ok, CPid} = rabbit_mirror_queue_coordinator:start_link(Q, GM),
    true = unlink(GM),
    gen_server2:reply(From, {promote, CPid}),
    ok = gm:confirmed_broadcast(GM, heartbeat),
    MasterState = rabbit_mirror_queue_master:promote_backing_queue_state(
                    CPid, BQ, BQS, GM, Seen),
    %% We have to do the requeue via this init because otherwise we
    %% don't have access to the relevent MsgPropsFun. Also, we are
    %% already in mnesia as the master queue pid. Thus we cannot just
    %% publish stuff by sending it to ourself - we must pass it
    %% through to this init, otherwise we can violate ordering
    %% constraints.
    AckTags = [AckTag || {_Guid, AckTag} <- dict:to_list(GA)],
    Deliveries = lists:append([queue:to_list(PubQ)
                               || {_ChPid, PubQ} <- dict:to_list(SQ)]),
    QueueState = rabbit_amqqueue_process:init_with_backing_queue_state(
                   Q, rabbit_mirror_queue_master, MasterState, RateTRef,
                   AckTags, Deliveries),
    {become, rabbit_amqqueue_process, QueueState, hibernate}.

noreply(State) ->
    {noreply, next_state(State), hibernate}.

reply(Reply, State) ->
    {reply, Reply, next_state(State), hibernate}.

next_state(State) ->
    ensure_rate_timer(State).

%% copied+pasted from amqqueue_process
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
  Delivery = #delivery { message = #basic_message { guid = Guid },
                         sender  = ChPid },
  State = #state { q               = Q,
                   sender_queues   = SQ,
                   seen            = Seen,
                   guid_to_channel = GTC }) ->
    case sets:is_element(Guid, Seen) of
        true ->
            GTC1 = record_confirm_or_confirm(Delivery, Q, GTC),
            State #state { guid_to_channel = GTC1,
                           seen            = sets:del_element(Guid, Seen) };
        false ->
            MQ = case dict:find(ChPid, SQ) of
                     {ok, MQ1} -> MQ1;
                     error    -> queue:new()
                 end,
            SQ1 = dict:store(ChPid, queue:in(Delivery, MQ), SQ),
            State #state { sender_queues = SQ1 }
    end.

process_instruction(
  {publish, Deliver, ChPid, MsgProps, Msg = #basic_message { guid = Guid }},
  State = #state { q                   = Q,
                   sender_queues       = SQ,
                   backing_queue       = BQ,
                   backing_queue_state = BQS,
                   guid_ack            = GA,
                   seen                = Seen,
                   guid_to_channel     = GTC }) ->
    {SQ1, Seen1, GTC1} =
        case dict:find(ChPid, SQ) of
            error ->
                {SQ, sets:add_element(Guid, Seen), GTC};
            {ok, MQ} ->
                case queue:out(MQ) of
                    {empty, _MQ} ->
                        {SQ, sets:add_element(Guid, Seen), GTC};
                    {{value, Delivery = #delivery {
                               message = #basic_message { guid = Guid } }},
                     MQ1} ->
                        GTC2 = record_confirm_or_confirm(Delivery, Q, GTC),
                        {dict:store(ChPid, MQ1, SQ), Seen, GTC2};
                    {{value, #delivery {}}, _MQ1} ->
                        %% The instruction was sent to us before we
                        %% were within the mirror_pids within the
                        %% amqqueue record. We'll never receive the
                        %% message directly.
                        {SQ, Seen, GTC}
                end
        end,
    State1 = State #state { sender_queues   = SQ1,
                            seen            = Seen1,
                            guid_to_channel = GTC1 },
    {ok,
     case Deliver of
         false ->
             BQS1 = BQ:publish(Msg, MsgProps, ChPid, BQS),
             State1 #state { backing_queue_state = BQS1 };
         {true, AckRequired} ->
             {AckTag, BQS1} = BQ:publish_delivered(AckRequired, Msg, MsgProps,
                                                   ChPid, BQS),
             {GA1, GTC3} = case AckRequired of
                               true  -> {dict:store(Guid, AckTag, GA), GTC1};
                               false -> {GA, maybe_confirm_message(Guid, GTC1)}
                           end,
             State1 #state { backing_queue_state = BQS1,
                             guid_ack            = GA1,
                             guid_to_channel     = GTC3 }
     end};
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
process_instruction({fetch, AckRequired, Guid, Remaining},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     guid_ack            = GA }) ->
    QLen = BQ:len(BQS),
    {ok, case QLen - 1 of
             Remaining ->
                 {{_Msg, _IsDelivered, AckTag, Remaining}, BQS1} =
                     BQ:fetch(AckRequired, BQS),
                 GA1 = case AckRequired of
                           true  -> dict:store(Guid, AckTag, GA);
                           false -> GA
                       end,
                 State #state { backing_queue_state = BQS1,
                                guid_ack            = GA1 };
             Other when Other < Remaining ->
                 %% we must be shorter than the master
                 State
         end};
process_instruction({ack, Guids},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     guid_ack            = GA }) ->
    {AckTags, GA1} = guids_to_acktags(Guids, GA),
    {Guids1, BQS1} = BQ:ack(AckTags, BQS),
    [] = Guids1 -- Guids, %% ASSERTION
    {ok, State #state { guid_ack            = GA1,
                        backing_queue_state = BQS1 }};
process_instruction({requeue, MsgPropsFun, Guids},
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS,
                                     guid_ack            = GA }) ->
    {AckTags, GA1} = guids_to_acktags(Guids, GA),
    {ok, case length(AckTags) =:= length(Guids) of
             true ->
                 {Guids, BQS1} = BQ:requeue(AckTags, MsgPropsFun, BQS),
                 State #state { guid_ack            = GA1,
                                backing_queue_state = BQS1 };
             false ->
                 %% the only thing we can safely do is nuke out our BQ
                 %% and GA
                 {_Count, BQS1} = BQ:purge(BQS),
                 {Guids, BQS2} = ack_all(BQ, GA, BQS1),
                 State #state { guid_ack            = dict:new(),
                                backing_queue_state = BQS2 }
         end};
process_instruction(delete_and_terminate,
                    State = #state { backing_queue       = BQ,
                                     backing_queue_state = BQS }) ->
    BQ:delete_and_terminate(BQS),
    {stop, State #state { backing_queue_state = undefined }}.

guids_to_acktags(Guids, GA) ->
    {AckTags, GA1} =
        lists:foldl(fun (Guid, {AckTagsN, GAN}) ->
                            case dict:find(Guid, GA) of
                                error        -> {AckTagsN, GAN};
                                {ok, AckTag} -> {[AckTag | AckTagsN],
                                                 dict:erase(Guid, GAN)}
                            end
                    end, {[], GA}, Guids),
    {lists:reverse(AckTags), GA1}.

ack_all(BQ, GA, BQS) ->
    BQ:ack([AckTag || {_Guid, AckTag} <- dict:to_list(GA)], BQS).

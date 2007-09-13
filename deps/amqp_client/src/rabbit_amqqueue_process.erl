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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server).

-define(UNSENT_MESSAGE_LIMIT, 100).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

-import(queue).
-import(erlang).
-import(lists).

% Queue's state
-record(q, {q,
            owner,
            exclusive_consumer,
            has_had_consumers,
            message_buffer,
            round_robin}).

-record(consumer, {tag, ack_required}).

% These are held in our process dictionary
-record(writer, {consumers,
                 writer_pid,
                 monitor_ref,
                 is_flow_control_active,
                 is_overload_protection_active,
                 unsent_message_count}).

init(Q) ->
    ?LOGDEBUG("Queue starting - ~p~n", [Q]),
    {ok, #q{q = Q,
            owner = none,
            exclusive_consumer = none,
            has_had_consumers = false,
            message_buffer = queue:new(),
            round_robin = queue:new()}}.

terminate(_Reason, _State) ->
    %% FIXME: How do we cancel active subscriptions?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%---------------------------------------------------------------------------

lookup_writer(WriterPid, Reason) ->
    case get({writer, WriterPid}) of
        undefined ->
            rabbit_log:warning("Writer ~p not found: ~p~n", [WriterPid, Reason]),
            not_found;
        W -> W
    end.

writer_record(WriterPid) ->
    Key = {writer, WriterPid},
    case get(Key) of
        undefined ->
            MonitorRef = erlang:monitor(process, WriterPid),
            W = #writer{consumers = [],
                        writer_pid = WriterPid,
                        monitor_ref = MonitorRef,
                        is_flow_control_active = false,
                        is_overload_protection_active = false,
                        unsent_message_count = 0},
            put(Key, W),
            W;
        W = #writer{} -> W
    end.

store_writer_record(W = #writer{writer_pid = WriterPid}) ->
    put({writer, WriterPid}, W).

update_store_and_maybe_block_writer(W = #writer{is_overload_protection_active = WasActive},
                                    NewCount) ->
    {Result, W1} = if
                       not(WasActive) and (NewCount > ?UNSENT_MESSAGE_LIMIT) ->
                           {block_writer, W#writer{is_overload_protection_active = true,
                                                   unsent_message_count = NewCount}};
                       WasActive and (NewCount == 0) ->
                           {unblock_writer, W#writer{is_overload_protection_active = false,
                                                     unsent_message_count = NewCount}};
                       true ->
                           {ok, W#writer{unsent_message_count = NewCount}}
                   end,
    store_writer_record(W1),
    Result.

deliver_immediately(Message, State = #q{q = #amqqueue{name = QName}, round_robin = RoundRobin}) ->
    ?LOGDEBUG("AMQQUEUE ~p DELIVERY:~n~p~n", [QName, Message]),
    case queue:out(RoundRobin) of
        {{value, QEntry = {WriterPid, #consumer{tag = ConsumerTag,
                                                ack_required = AckRequired}}},
         RoundRobinTail} ->
            rabbit_writer:deliver(WriterPid, ConsumerTag, AckRequired,
                                  QName, self(), Message),
            W = writer_record(WriterPid),
            #writer{unsent_message_count = OldCount} = W,
            NewCount = OldCount + 1,
            %%?LOGDEBUG("inc - NewCount ~p~n", [NewCount]),
            case update_store_and_maybe_block_writer(W, NewCount) of
                ok ->
                    {offered, State#q{round_robin = queue:in(QEntry, RoundRobinTail)}};
                block_writer ->
                    {offered, State#q{round_robin = block_consumers(WriterPid, RoundRobinTail)}}
            end;
        {empty, _} ->
            not_offered
    end.

deliver_or_enqueue(Message, State = #q{message_buffer = MessageBuffer}) ->
    case deliver_immediately(Message, State) of
        {offered, State1} ->
            {true, State1};
        not_offered ->
            {false, State#q{message_buffer = queue:in(Message, MessageBuffer)}}
    end.

block_consumers(WriterPid, RoundRobin) ->
    %%?LOGDEBUG("~p Blocking ~p from ~p~n", [self(), WriterPid, queue:to_list(RoundRobin)]),
    queue:from_list(lists:filter(fun ({WP, _}) -> WP /= WriterPid end,
                                 queue:to_list(RoundRobin))).

unblock_consumers(WriterPid, Consumers, RoundRobin) ->
    %%?LOGDEBUG("Unblocking ~p ~p ~p~n", [WriterPid, Consumers, queue:to_list(RoundRobin)]),
    queue:join(RoundRobin, queue:from_list(lists:map(fun (C) -> {WriterPid, C} end, Consumers))).

block_consumer(WriterPid, ConsumerTag, RoundRobin) ->
    %%?LOGDEBUG("~p Blocking ~p from ~p~n", [self(), ConsumerTag, queue:to_list(RoundRobin)]),
    queue:from_list(lists:filter(fun ({WP, #consumer{tag = CT}}) ->
                                         (WP /= WriterPid) or (CT /= ConsumerTag)
                                 end, queue:to_list(RoundRobin))).

possibly_unblock(W = #writer{consumers = Consumers, writer_pid = WriterPid},
                 NewCount,
                 State = #q{round_robin = RoundRobin}) ->
    %%?LOGDEBUG("Checking ~p against ~p~n", [Count, ?UNSENT_MESSAGE_LIMIT]),
    case update_store_and_maybe_block_writer(W, NewCount) of
        ok ->
            State;
        unblock_writer ->
            run_poke_burst(State#q{round_robin =
                                   unblock_consumers(WriterPid, Consumers, RoundRobin)})
    end.

requeue_messages(Messages, State = #q{message_buffer = MessageBuffer}) ->
    %% FIXME: Rolled-back messages should have redeliver flag set somehow??
    rabbit_log:info("Requeueing ~p messages, ~p already on queue~n",
                    [queue:len(Messages), queue:len(MessageBuffer)]),
    run_poke_burst(State#q{message_buffer = queue:join(MessageBuffer, Messages)}).

check_auto_delete(State = #q{q = #amqqueue{auto_delete = false}}) ->
    {continue, State};
check_auto_delete(State = #q{has_had_consumers = false}) ->
    {continue, State};
check_auto_delete(State = #q{round_robin = RoundRobin}) ->
    % The clauses above rule out cases where no-one has consumed from
    % this queue yet, and cases where we are not an auto_delete queue
    % in any case. Thus it remains to check whether we have any active
    % listeners at this point.
    case queue:is_empty(RoundRobin) of
        true ->
            % There are no waiting listeners. It's possible that we're
            % completely unused. Check.
            case is_unused() of
                true ->
                    % There are no writers with active consumers at
                    % this point. This is the signal to autodelete.
                    {stop, State};
                false ->
                    % There is at least one writer with an active
                    % consumer, so we shouldn't delete ourselves.
                    {continue, State}
            end;
        false ->
            % There are some waiting listeners, thus we are not
            % unused, so can continue life as normal without needing
            % to check the process dictionary.
            {continue, State}
    end.

handle_writer_down(#writer{monitor_ref = MonitorRef, writer_pid = WriterPid},
                   State = #q{exclusive_consumer = Holder, round_robin = ActiveConsumers}) ->
    NewActive = block_consumers(WriterPid, ActiveConsumers),
    erlang:demonitor(MonitorRef),
    erase({writer, WriterPid}),
    check_auto_delete(State#q{exclusive_consumer = case Holder of
                                                       {W, _} when W == WriterPid -> none;
                                                       Other -> Other
                                                   end,
                              round_robin = NewActive}).

cancel_holder(WriterPid, ConsumerTag, {WP, CT}) when WP == WriterPid andalso CT == ConsumerTag ->
    none;
cancel_holder(_WriterPid, _ConsumerTag, Holder) ->
    Holder.

check_queue_owner(none, _ReaderPid) ->
    ok;
check_queue_owner({OwnerPid, _MonitorRef}, ReaderPid) ->
    if
        OwnerPid == ReaderPid ->
            ok;
        true ->
            mismatch
    end.

check_exclusive_access({_WPid, _ConsumerTag}, _ExclusiveConsume) ->
    in_use;
check_exclusive_access(none, false) ->
    ok;
check_exclusive_access(none, true) ->
    case is_unused() of
        true ->
            ok;
        false ->
            in_use
    end.

run_poke_burst(State = #q{message_buffer = MessageBuffer}) ->
    run_poke_burst(MessageBuffer, State).

run_poke_burst(MessageBuffer, State) ->
    case queue:out(MessageBuffer) of
        {{value, Message}, BufferTail} ->
            case deliver_immediately(Message, State) of
                {offered, NewState} ->
                    run_poke_burst(BufferTail, NewState);
                not_offered ->
                    State#q{message_buffer = MessageBuffer}
            end;
        {empty, _} ->
            State#q{message_buffer = MessageBuffer}
    end.

is_unused() ->
    is_unused1(get()).

is_unused1([]) ->
    true;
is_unused1([{{writer, _}, #writer{consumers = Consumers}} | _Rest])
  when Consumers /= [] ->
    false;
is_unused1([_ | Rest]) ->
    is_unused1(Rest).

maybe_send_reply(_WriterPid, undefined) -> ok;
maybe_send_reply(WriterPid, Msg) ->
    ok = rabbit_writer:send_command(WriterPid, Msg).

%---------------------------------------------------------------------------

handle_call({deliver_immediately, Message}, _From, State) ->
    %% Synchronous, "immediate" delivery mode
    %%
    %% FIXME: Is this correct semantics?
    %%
    %% I'm worried in particular about the case where an exchange has
    %% two queues against a particular routing key, and a message is
    %% sent in immediate mode through the binding. In non-immediate
    %% mode, both queues get the message, saving it for later if
    %% there's noone ready to receive it just now. In immediate mode,
    %% should both queues still get the message, somehow, or should
    %% just all ready-to-consume queues get the message, with unready
    %% queues discarding the message?
    %%
    case deliver_immediately(Message, State) of
        {offered, State1} ->
            {reply, true, State1};
        not_offered ->
            {reply, false, State}
    end;

handle_call({deliver, Message}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    {Delivered, State1} = deliver_or_enqueue(Message, State),
    {reply, Delivered, State1};

handle_call({basic_get}, _From, State = #q{message_buffer = MessageBuffer}) ->
    {Val, BufferTail} = queue:out(MessageBuffer),
    Res = case Val of
              {value, Message} -> {ok, Message, queue:len(BufferTail)};
              empty -> empty
          end,
    {reply, Res, State#q{message_buffer = BufferTail}};

handle_call({basic_consume, NoAck, ReaderPid, WriterPid, ConsumerTag,
             ExclusiveConsume, OkMsg},
            _From, State = #q{owner = Owner,
                              exclusive_consumer = ExistingHolder,
                              round_robin = RoundRobin}) ->
    case check_queue_owner(Owner, ReaderPid) of
        mismatch ->
            {reply, {error, queue_owned_by_another_connection}, State};
        ok ->
            case check_exclusive_access(ExistingHolder, ExclusiveConsume) of
                in_use ->
                    {reply, {error, exclusive_consume_unavailable}, State};
                ok ->
                    W = writer_record(WriterPid),
                    Consumer = #consumer{tag = ConsumerTag, ack_required = not(NoAck)},
                    #writer{consumers = Consumers} = W,
                    W1 = W#writer{consumers = [Consumer | Consumers]},
                    store_writer_record(W1),
                    State1 = State#q{has_had_consumers = true,
                                     exclusive_consumer =
                                       if
                                           ExclusiveConsume -> {WriterPid, ConsumerTag};
                                           true -> ExistingHolder
                                       end,
                                     round_robin = queue:in({WriterPid, Consumer}, RoundRobin)},
                    ok = maybe_send_reply(WriterPid, OkMsg),
                    {reply, ok, run_poke_burst(State1)}
            end
    end;

handle_call(Request = {basic_cancel, WriterPid, ConsumerTag, OkMsg},
            _From, State = #q{exclusive_consumer = Holder,
                              round_robin = RoundRobin}) ->
    case lookup_writer(WriterPid, Request) of
        not_found ->
            ok = maybe_send_reply(WriterPid, OkMsg),
            {reply, ok, State};
        W = #writer{consumers = Consumers} ->
            NewConsumers = lists:filter
                             (fun (#consumer{tag = CT}) -> CT /= ConsumerTag end,
                              Consumers),
            W1 = W#writer{consumers = NewConsumers},
            store_writer_record(W1),
            ok = maybe_send_reply(WriterPid, OkMsg),
            case check_auto_delete(State#q{exclusive_consumer = cancel_holder(WriterPid,
                                                                              ConsumerTag,
                                                                              Holder),
                                           round_robin = block_consumer(WriterPid,
                                                                        ConsumerTag,
                                                                        RoundRobin)}) of
                {continue, State1} ->
                    {reply, ok, State1};
                {stop, State1} ->
                    ok = rabbit_amqqueue:internal_delete(State1#q.message_buffer,
                                                         (State1#q.q)#amqqueue.name),
                    {stop, normal, ok, State1}
            end
    end;

handle_call(stat, _From, State = #q{q = #amqqueue{name = Name},
                                    message_buffer = MessageBuffer,
                                    round_robin = RoundRobin}) ->
    {reply, {ok, Name, queue:len(MessageBuffer), queue:len(RoundRobin)}, State};

handle_call({delete, IfUnused, IfEmpty}, _From, State = #q{q = #amqqueue{name = Name},
                                                           message_buffer = MessageBuffer}) ->
    IsEmpty = queue:is_empty(MessageBuffer),
    IsUnused = is_unused(),
    if
        IfEmpty and not(IsEmpty) ->
            {reply, {error, not_empty}, State};
        IfUnused and not(IsUnused) ->
            {reply, {error, in_use}, State};
        true ->
            ok = rabbit_amqqueue:internal_delete(MessageBuffer, Name),
            {stop, normal, {ok, queue:len(MessageBuffer)}, State}
    end;

handle_call(purge, _From, State = #q{message_buffer = MessageBuffer}) ->
    ok = rabbit_amqqueue:purge_message_buffer(MessageBuffer),
    {reply, {ok, queue:len(MessageBuffer)}, State#q{message_buffer = queue:new()}};

handle_call({claim_queue, ReaderPid}, _From, State = #q{owner = Owner,
                                                        exclusive_consumer = Holder}) ->
    case Owner of
        none ->
            case check_exclusive_access(Holder, true) of
                in_use ->
                    %% FIXME: Is this really the right answer? What if
                    %% an active consumer's reader is actually the
                    %% claiming pid? Should that be allowed? In order
                    %% to check, we'd need to hold not just the writer
                    %% pid for each consumer, but also its reader
                    %% pid...
                    {reply, locked, State};
                ok ->
                    {reply, ok, State#q{owner = {ReaderPid, erlang:monitor(process, ReaderPid)}}}
            end;
        {ExistingPid, _MonitorRef} when ExistingPid == ReaderPid ->
            {reply, ok, State};
        _ ->
            {reply, locked, State}
    end.

handle_cast({deliver, Message}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    {_Delivered, State1} = deliver_or_enqueue(Message, State),
    {noreply, State1};

handle_cast({requeue, Messages}, State) ->
    {noreply, requeue_messages(Messages, State)};

handle_cast(Request = {notify_sent, WriterPid}, State) ->
    case lookup_writer(WriterPid, Request) of
        not_found -> {noreply, State};
        W = #writer{unsent_message_count = OldCount} ->
            NewCount = OldCount - 1,
            %%?LOGDEBUG("dec - NewCount ~p~n", [NewCount]),
            {noreply, possibly_unblock(W, NewCount, State)}
    end.

handle_info({'DOWN', MonitorRef, process, DownPid, _Reason}, State = #q{owner = {Pid, Ref}})
  when DownPid == Pid andalso MonitorRef == Ref ->
    %% We know here that there are no consumers on this queue that are
    %% owned by other pids than the one that just went down, so since
    %% exclusive in some sense implies autodelete, we delete the queue
    %% here. The other way of implementing the "exclusive implies
    %% autodelete" feature is to actually set autodelete when an
    %% exclusive declaration is seen, but this has the problem that
    %% the python tests rely on the queue not going away after a
    %% basic.cancel when the queue was declared exclusive and
    %% nonautodelete.
    NewState = State#q{owner = none},
    ok = rabbit_amqqueue:internal_delete(NewState#q.message_buffer,
                                         (NewState#q.q)#amqqueue.name),
    {stop, normal, NewState};
handle_info({'DOWN', MonitorRef, process, DownPid, Reason}, State) ->
    case lookup_writer(DownPid, {writer_down, MonitorRef, DownPid, Reason}) of
        not_found -> {noreply, State};
        W = #writer{monitor_ref = StoredMonitorRef} ->
            StoredMonitorRef = MonitorRef, % assertion
            case handle_writer_down(W, State) of
                {continue, NewState} ->
                    {noreply, NewState};
                {stop, NewState} ->
                    ok = rabbit_amqqueue:internal_delete(NewState#q.message_buffer,
                                                         (NewState#q.q)#amqqueue.name),
                    {stop, normal, NewState}
            end
    end;

handle_info(Info, State) ->
    ?LOGDEBUG("Info in queue: ~p~n", [Info]),
    {stop, {unhandled_info, Info}, State}.

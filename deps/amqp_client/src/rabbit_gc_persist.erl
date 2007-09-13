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

-module(rabbit_gc_persist).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([load_snapshot/1, current_snapshot/0, integrate_messages/1]).

-define(SERVER, ?MODULE).

-include("rabbit.hrl").

-record(gcstate, {message_tid, ack_tid, refcount_tid}).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

load_snapshot(LogHandle) ->
    gen_server:call(?SERVER, {load_snapshot, LogHandle}, infinity).

current_snapshot() ->
    gen_server:call(?SERVER, current_snapshot).

integrate_messages(Unit) ->
    gen_server:cast(?SERVER, {integrate_messages, Unit}).

%%--------------------------------------------------------------------

init([]) ->
    {ok, #gcstate{message_tid = ets:new(message_table, []),
                  ack_tid = ets:new(ack_table, []),
                  refcount_tid = ets:new(refcount_table, [])}}.

handle_call(current_snapshot, _From, State) ->
    {reply, {ok, internal_current_snapshot(State)}, State};
handle_call({load_snapshot, LogHandle}, _From, State) ->
    {reply, internal_load_snapshot(State, LogHandle), State};
handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({integrate_messages, Unit}, State) ->
    ok = internal_integrate_messages(State, Unit),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

internal_current_snapshot(#gcstate{message_tid = MTid, ack_tid = ATid, refcount_tid = RTid}) ->
    InnerSnapshot = {{txns, get()},
                     {messages, ets:tab2list(MTid)},
                     {acks, ets:tab2list(ATid)},
                     {refcounts, ets:tab2list(RTid)}},
    ?LOGDEBUG("Inner snapshot: ~p~n", [InnerSnapshot]),
    {gc_persist_snapshot, term_to_binary(InnerSnapshot)}.

internal_load_snapshot(State = #gcstate{message_tid = MTid, ack_tid = ATid, refcount_tid = RTid},
                       LogHandle) ->
    {K, [{gc_persist_snapshot, StateBin} | Items]} = disk_log:chunk(LogHandle, start),
    {{txns, ProcessDictionary},
     {messages, MessageTable},
     {acks, AckTable},
     {refcounts, RefcountTable}} = binary_to_term(StateBin),
    lists:foreach(fun ({Key = {txn, _TxnKey}, Val}) ->
                          ?LOGDEBUG("Restoring transaction ~p~n", [Key]),
                          put(Key, Val);
                      ({_Key, _Val}) ->
                          ?LOGDEBUG("Ignoring process state ~p~n", [{_Key, _Val}]),
                          ok
                  end, ProcessDictionary),
    ok = fill_ets_table(MTid, MessageTable),
    ok = fill_ets_table(ATid, AckTable),
    ok = fill_ets_table(RTid, RefcountTable),
    replay(State, Items, LogHandle, K),
    ok = rollback_all_remaining_transactions(),
    ok = redeliver_all_remaining_messages(State),
    ok.

fill_ets_table(_Tid, []) ->
    ok;
fill_ets_table(Tid, [Object | Rest]) ->
    ?LOGDEBUG("Recovery loading ~p~n", [Object]),
    true = ets:insert(Tid, Object),
    fill_ets_table(Tid, Rest).

rollback_all_remaining_transactions() ->
    lists:foreach(fun ({Key = {txn, _TxnKey}, _Val}) ->
                          ?LOGDEBUG("Rolling back incomplete txn ~p~n", [Key]),
                          erase(Key);
                      ({_Key, _Val}) ->
                          ok
                  end, get()),
    ok.

redeliver_all_remaining_messages(State = #gcstate{message_tid = MTid,
                                                  ack_tid = ATid,
                                                  refcount_tid = RTid}) ->
    Work = ets:foldl(fun ({PKey, {Expected, Seen}}, Acc) ->
                             if
                                 Expected == unknown ->
                                     Acc;
                                 Seen < Expected ->
                                     build_redelivery_instruction(State, PKey, Seen, Acc);
                                 true ->
                                     Acc
                             end
                     end, [], RTid),
    true = ets:delete_all_objects(MTid),
    true = ets:delete_all_objects(ATid),
    true = ets:delete_all_objects(RTid),
    WorkInOrder = lists:sort(Work),
    lists:foreach(fun ({PKey, Message, ToRedeliver}) ->
                          redeliver_single(State, PKey, Message, ToRedeliver)
                  end, WorkInOrder),
    ok.

build_redelivery_instruction(#gcstate{message_tid = MTid, ack_tid = ATid},
                             PKey, Seen, Acc) ->
    [{_Key1, DeliveredTo}] = ets:lookup(ATid, {PKey, delivery}),
    [{_Key2, Message}] = ets:lookup(MTid, PKey),
    AckedFrom = collect_acks(ATid, PKey, Seen, []),
    ToRedeliver = sets:to_list(sets:subtract(sets:from_list(DeliveredTo),
                                             sets:from_list(AckedFrom))),
    [{PKey, Message, ToRedeliver} | Acc].

collect_acks(_ATid, _PKey, 0, Acc) ->
    Acc;
collect_acks(ATid, PKey, Remaining, Acc) ->
    [{_Key, QName}] = ets:lookup(ATid, {PKey, ack, Remaining - 1}),
    collect_acks(ATid, PKey, Remaining - 1, [QName | Acc]).

redeliver_single(#gcstate{message_tid  = MTid,
                          ack_tid      = ATid,
                          refcount_tid = RTid},
                 PKey, Message, ToRedeliver) ->
    DeliveredTo =
        lists:filter(
          fun (QueueName) ->
                  case rabbit_amqqueue:lookup(QueueName) of
                      {ok, #amqqueue{pid = QPid}} ->
                          rabbit_amqqueue:deliver(false, false, Message, QPid);
                      {error, not_found} ->
                          false
                  end
          end, ToRedeliver),
    Expected = length(DeliveredTo),
    if
        Expected > 0 ->
            true = ets:insert(MTid, {PKey, Message}),
            true = ets:insert(ATid, {{PKey, delivery}, DeliveredTo}),
            true = ets:insert(RTid, {PKey, {Expected, 0}}),
            ok;
        true ->
            ok
    end.

replay(State, [], LogHandle, K) ->
    case disk_log:chunk(LogHandle, K) of
        {K1, Items} ->
            replay(State, Items, LogHandle, K1);
        {K1, Items, Badbytes} ->
            rabbit_log:warning("~p bad bytes recovering persister log~n", [Badbytes]),
            replay(State, Items, LogHandle, K1);
        eof ->
            ok
    end;
replay(State, [Item | Items], LogHandle, K) ->
    ok = internal_integrate_messages(State, Item),
    replay(State, Items, LogHandle, K).

internal_integrate_messages(State, {most_recent_first, ItemsRev}) ->
    ?LOGDEBUG("Starting integration ~p~n", [lists:reverse(ItemsRev)]),
    ok = internal_integrateN(State, ItemsRev),
    ?LOGDEBUG0("Finished integration~n"),
    ok;
internal_integrate_messages(_State, _Other) ->
    ?LOGDEBUG("Dropping snapshot state ~p~n", [_Other]),
    ok.

internal_integrateN(_State, []) ->
    ok;
internal_integrateN(State, [Item | ItemsRev]) ->
    %% Note: non-tail recursion! I'm doing this to avoid lists:reverse... sensible??
    internal_integrateN(State, ItemsRev),
    %% After doing the items that logically precede us, we're free now to process this item.
    internal_integrate1(State, Item).

internal_integrate1(_State, {begin_transaction, Key}) ->
    put({txn, Key}, []),
    ok;
internal_integrate1(_State, {extend_transaction, Key, MessageList}) ->
    case get({txn, Key}) of
        undefined ->
            ok;
        MessageLists ->
            put({txn, Key}, [MessageList | MessageLists]),
            ok
    end;
internal_integrate1(_State, {rollback_transaction, Key}) ->
    erase({txn, Key}),
    ok;
internal_integrate1(State, {commit_transaction, Key}) ->
    case erase({txn, Key}) of
        undefined ->
            ok;
        MessageLists ->
            ?LOGDEBUG("gc_persist committing txn ~p~n", [Key]),
            ok = lists:foldr(fun (MessageList, ok) ->
                                     perform_work(State, MessageList)
                             end, ok, MessageLists)
    end;
internal_integrate1(State, {dirty_work, MessageList}) ->
    ok = perform_work(State, MessageList).

perform_work(_State, []) ->
    ok;
perform_work(State, [Item | Rest]) ->
    ok = perform_work_item(State, Item),
    perform_work(State, Rest).
             
perform_work_item(State = #gcstate{ack_tid = ATid, refcount_tid = RTid},
                  {ack, QName, PKey}) ->
    ?LOGDEBUG("Recording Ack ~p~n", [{PKey, QName}]),
    {Expected, Seen} = case ets:lookup(RTid, PKey) of
                           [] ->
                               %% Ack arrived before the message was properly published!
                               %% This will happen in the transactional case quite often.
                               {unknown, 0};
                           [{_PKey, V = {_Expected, _Seen}}] ->
                               V
                       end,
    ?LOGDEBUG("State for ~p is Expected ~p, Seen ~p~n", [PKey, Expected, Seen]),
    if
        Expected == Seen + 1 ->
            remove_record(State, PKey, Seen);
        true ->
            true = ets:insert(ATid, {{PKey, ack, Seen}, QName}),
            true = ets:insert(RTid, {PKey, {Expected, Seen + 1}}),
            ok
    end;
perform_work_item(State = #gcstate{ack_tid = ATid, refcount_tid = RTid},
                  {delivery, PKey, QueueNameList}) ->
    ?LOGDEBUG("Recording Queue Deliveries ~p~n", [{PKey, QueueNameList}]),
    Expected = length(QueueNameList),
    case ets:lookup(RTid, PKey) of
        [] ->
            %% We're seeing the delivery before any acks have arrived.
            ?LOGDEBUG("State for ~p is Expected ~p, Seen ~p~n", [PKey, Expected, 0]),
            true = ets:insert(ATid, {{PKey, delivery}, QueueNameList}),
            true = ets:insert(RTid, {PKey, {Expected, 0}}),
            ok;
        [{_PKey, {unknown, Seen}}] when Expected > Seen ->
            ?LOGDEBUG("State for ~p is Expected ~p, Seen ~p~n", [PKey, Expected, Seen]),
            true = ets:insert(ATid, {{PKey, delivery}, QueueNameList}),
            true = ets:insert(RTid, {PKey, {Expected, Seen}}),
            ok;
        [{_PKey, {unknown, Seen}}] when Expected == Seen ->
            remove_record(State, PKey, Seen);
        Other ->
            rabbit_log:error("Internal error in gc_persist: Seen > Expected: ~p~n", [Other]),
            ok
    end;
perform_work_item(#gcstate{message_tid = MTid},
                  {publish, Message = #basic_message{persistent_key = PKey}}) ->
    ?LOGDEBUG("Recording Message ~p~n", [{PKey, Message}]),
    true = ets:insert(MTid, {PKey, Message}),
    ok.

remove_record(#gcstate{message_tid = MTid, ack_tid = ATid, refcount_tid = RTid},
              PKey, Seen) ->
    ?LOGDEBUG("No outstanding left, dropping persistent message ~p~n", [PKey]),
    ets:delete(MTid, PKey),
    ets:delete(ATid, {PKey, delivery}),
    delete_acks(ATid, PKey, Seen),
    ets:delete(RTid, PKey),
    ok.

delete_acks(_ATid, _PKey, 0) ->
    ok;
delete_acks(ATid, PKey, Remaining) ->
    ets:delete(ATid, {PKey, ack, Remaining - 1}),
    delete_acks(ATid, PKey, Remaining - 1).

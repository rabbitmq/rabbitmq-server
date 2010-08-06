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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_stats).

-include_lib("rabbit_common/include/rabbit.hrl").

-behaviour(gen_event).

-export([start/0]).

-export([get_queues/0, get_connections/0, get_connection/1,
         get_overview/0, get_msg_stats/4]).

-export([group_sum/2]).

-export([pget/2, add/2]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {tables}).
-define(TABLES, [queue_stats, connection_stats, channel_stats,
                 channel_exchange_stats, channel_queue_stats,
                 channel_queue_exchange_stats]).
-define(FINE_STATS, [ack, deliver, deliver_no_ack, get, get_no_ack, publish]).

%%----------------------------------------------------------------------------

start() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

get_queues() ->
    gen_event:call(rabbit_event, ?MODULE, get_queues, infinity).

get_connections() ->
    gen_event:call(rabbit_event, ?MODULE, get_connections, infinity).

get_connection(Id) ->
    gen_event:call(rabbit_event, ?MODULE, {get_connection, Id}, infinity).

get_overview() ->
    gen_event:call(rabbit_event, ?MODULE, get_overview, infinity).

get_msg_stats(Type, GroupBy, MatchKey, MatchValue) ->
    gen_event:call(rabbit_event, ?MODULE,
                   {get_msg_stats, Type, GroupBy, MatchKey, MatchValue},
                   infinity).

%%----------------------------------------------------------------------------

pget(Key, List) ->
    pget(Key, List, unknown).

pget(Key, List, Default) ->
    proplists:get_value(Key, List, Default).

id(Pid) when is_pid(Pid) -> list_to_binary(pid_to_list(Pid));
id(List) -> rabbit_management_format:pid(pget(pid, List)).

add(unknown, _) -> unknown;
add(_, unknown) -> unknown;
add(A, B)       -> A + B.

lookup_element(Table, Key) ->
    lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

result_or_error([]) -> error;
result_or_error(S)  -> S.

%% TODO To say these need unit tests is an understatement

%% TODO until we do rates properly (i.e. looking at a time series)
%% we have the problem that an object which stops emitting events will look
%% like its rate has stayed up. This might make time series more important.

rates(Table, Id, Stats, Timestamp, Keys) ->
    Stats ++ lists:filter(
               fun (unknown) -> false;
                   (_)       -> true
               end,
               [rate(Table, Id, Stats, Timestamp, Key) || Key <- Keys]).

rate(Table, Id, Stats, Timestamp, Key) ->
    Old = lookup_element(Table, Id),
    OldTS = lookup_element(Table, Id, 3),
    case OldTS == [] orelse not proplists:is_defined(Key, Old) of
        true ->
            unknown;
        _ ->
            Diff = pget(Key, Stats) - pget(Key, Old),
            {list_to_atom(atom_to_list(Key) ++ "_rate"),
             Diff / (timer:now_diff(Timestamp, OldTS) / 1000000)}
    end.

sum(Table, Keys) ->
    lists:foldl(fun (Stats, Acc) ->
                        [{Key, Val + pget(Key, Stats, 0)} || {Key, Val} <- Acc]
                end,
                [{Key, 0} || Key <- Keys],
                [Value || {_Key, Value, _TS} <- ets:tab2list(Table)]).

format_id({ChPid, #resource{name=XName, virtual_host=XVhost}}) ->
    [{channel, ChPid}, {exchange_name, XName}, {exchange_vhost, XVhost}];
format_id({ChPid, QPid}) ->
    [{channel, ChPid}, {queue, QPid}];
format_id({ChPid, QPid, #resource{name=XName, virtual_host=XVhost}}) ->
    [{channel, ChPid}, {queue, QPid},
     {exchange_name, XName}, {exchange_vhost, XVhost}].

group_sum(GroupBy, List) ->
    Res =
        lists:foldl(fun ({Ids, New}, Acc) ->
                            Id = {GroupBy, pget(GroupBy, Ids)},
                            dict:update(Id, fun(Cur) -> gs_update(Cur, New) end,
                                        New, Acc)
                    end,
                    dict:new(),
                    [I || I <- List]),
    [{[Ids], Stats} || {Ids, Stats} <- dict:to_list(Res)].

gs_update(Cur, New) ->
    [{Key, Val + pget(Key, Cur, 0)} || {Key, Val} <- New].

%%----------------------------------------------------------------------------

augment_items(Items, #state{tables = Tables}) ->
    [augment_item(I, Tables) || I <- Items].

augment_item({Ids, Stats}, Tables) ->
    {augment(Ids, [{channel, fun augment_channel_pid/2},
                   {queue,   fun augment_queue_pid/2}], Tables), Stats}.

augment(Ids, Funs, Tables) ->
    lists:flatten([augment(K, Ids, Fun, Tables) || {K, Fun} <- Funs] ++ Ids).

augment(K, Ids, Fun, Tables) ->
    Id = pget(K, Ids),
    case Id of
        unknown -> [];
        _       -> Fun(Id, Tables)
    end.

augment_channel_pid(Pid, Tables) ->
    Ch = lookup_element(
           orddict:fetch(channel_stats, Tables),
           {Pid, create}),
    Conn = lookup_element(
             orddict:fetch(connection_stats, Tables),
             {id(pget(connection, Ch)), create}),
    [{channel_number, pget(number, Ch)},
     {channel_connection, pget(pid, Conn)},
     {channel_peer_address, pget(peer_address, Conn)},
     {channel_peer_port, pget(peer_port, Conn)}].

augment_queue_pid(_Pid, _Tables) ->
    [].

%%----------------------------------------------------------------------------

%% TODO some sort of generalised query mechanism for the coarse stats?

init([]) ->
    {ok, #state{tables =
                    orddict:from_list(
                      [{Key, ets:new(anon, [private])} || Key <- ?TABLES])}}.

handle_call(get_queues, State = #state{tables = Tables}) ->
    Table = orddict:fetch(queue_stats, Tables),
    {ok, merge_created_stats(Table), State};

handle_call(get_connections, State = #state{tables = Tables}) ->
    Table = orddict:fetch(connection_stats, Tables),
    {ok, merge_created_stats(Table), State};

handle_call({get_connection, Id}, State = #state{tables = Tables}) ->
    Table = orddict:fetch(connection_stats, Tables),
    {ok, result_or_error(lookup_element(Table, {Id, create}) ++
                             lookup_element(Table, {Id, stats})), State};

handle_call(get_overview, State = #state{tables = Tables}) ->
    Table = orddict:fetch(connection_stats, Tables),
    {ok, sum(Table, [recv_oct, send_oct, recv_oct_rate, send_oct_rate]), State};

handle_call({get_msg_stats, Type, GroupBy, MatchKey, MatchValue},
            State = #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    All = [{format_id(Id), Stats} ||
              {Id, Stats, _Timestamp} <- ets:tab2list(Table)],
    Group = case {Type, GroupBy} of
                {_, undefined}                             -> false;
                {channel_queue_stats, "channel"}           -> true;
                {channel_queue_stats, "queue"}             -> true;
                {channel_exchange_stats, "channel"}        -> true;
                {channel_exchange_stats, "exchange"}       -> true;
                {channel_queue_exchange_stats, "channel"}  -> true;
                {channel_queue_exchange_stats, "exchange"} -> true;
                {channel_queue_exchange_stats, "queue"}    -> true;
                {_, _}                                     -> bad_request
            end,
    Res = case Group of
              false -> All;
              true  -> group_sum(list_to_atom(GroupBy), All);
              _     -> bad_request
          end,
    {ok, augment_items(Res, State), State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(Event = #event{type = queue_created}, State) ->
    handle_created(queue_stats, Event, State);

handle_event(Event = #event{type = queue_stats}, State) ->
    handle_stats(queue_stats, Event, [], State);

handle_event(Event = #event{type = queue_deleted}, State) ->
    handle_deleted(queue_stats, Event, State);

handle_event(Event = #event{type = connection_created}, State) ->
    handle_created(connection_stats, Event, State);

handle_event(Event = #event{type = connection_stats}, State) ->
    handle_stats(connection_stats, Event, [recv_oct, send_oct], State);

handle_event(Event = #event{type = connection_closed}, State) ->
    handle_deleted(connection_stats, Event, State);

handle_event(Event = #event{type = channel_created}, State) ->
    handle_created(channel_stats, Event, State);

handle_event(Event = #event{type = channel_stats, props = Stats,
                            timestamp = Timestamp}, State) ->
    handle_stats(channel_stats, Event, [], State),
    [[handle_fine_stats(Type, id(Stats), S, Timestamp, State) ||
         S <- pget(Type, Stats)] ||
        Type <- [channel_queue_stats, channel_exchange_stats,
                 channel_queue_exchange_stats]],
    {ok, State};

handle_event(Event = #event{type = channel_closed,
                            props = [{pid, Pid}]}, State) ->
    handle_deleted(channel_stats, Event, State),
    [delete_fine_stats(Type, id(Pid), State) ||
        Type <- [channel_queue_stats, channel_exchange_stats,
                 channel_queue_exchange_stats]],
    {ok, State};

handle_event(_Event, State) ->
%%    io:format("Got event ~p~n", [Event]),
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

handle_created(TName, #event{props = Stats}, State = #state{tables = Tables}) ->
    ets:insert(orddict:fetch(TName, Tables), {{id(Stats), create}, Stats}),
    {ok, State}.

handle_stats(TName, #event{props = Stats, timestamp = Timestamp},
             RatesKeys, State = #state{tables = Tables}) ->
    Table = orddict:fetch(TName, Tables),
    Id = {id(Stats), stats},
    ets:insert(Table,
               {Id,
                rates(Table, Id, Stats, Timestamp, RatesKeys),
                Timestamp}),
    {ok, State}.

handle_deleted(TName, #event{props = [{pid, Pid}]},
               State = #state{tables = Tables}) ->
    Table = orddict:fetch(TName, Tables),
    ets:delete(Table, {id(Pid), create}),
    ets:delete(Table, {id(Pid), stats}),
    {ok, State}.

handle_fine_stats(Type, ChPid, {Ids, Stats}, Timestamp,
                  State = #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    Id = fine_stats_key(ChPid, Ids),
    Res = rates(Table, Id, Stats, Timestamp, ?FINE_STATS),
    delete_fine_stats(Type, ChPid, State),
    ets:insert(Table, {Id, Res, Timestamp}).

delete_fine_stats(Type, ChPid, #state{tables = Tables}) ->
    Table = orddict:fetch(Type, Tables),
    ets:match_delete(Table, {{ChPid, '_'}, '_', '_'}),
    ets:match_delete(Table, {{ChPid, '_', '_'}, '_', '_'}).

fine_stats_key(ChPid, {QPid, X})              -> {ChPid, id(QPid), X};
fine_stats_key(ChPid, QPid) when is_pid(QPid) -> {ChPid, id(QPid)};
fine_stats_key(ChPid, X)                      -> {ChPid, X}.

merge_created_stats(Table) ->
    [Stats ++ lookup_element(Table, {Pid, stats}) ||
        {{Pid, create}, Stats} <- ets:tab2list(Table)].

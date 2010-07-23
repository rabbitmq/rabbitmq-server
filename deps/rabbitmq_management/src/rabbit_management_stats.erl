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

-export([get_queue_stats/1, get_connections/0, get_connection/1]).
-export([pget/2, add/2]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queue_stats, connection_stats}).

%%----------------------------------------------------------------------------

start() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

get_queue_stats(QPids) ->
    gen_event:call(rabbit_event, ?MODULE, {get_queue_stats, QPids}, infinity).

get_connections() ->
    gen_event:call(rabbit_event, ?MODULE, get_connections, infinity).

get_connection(Id) ->
    gen_event:call(rabbit_event, ?MODULE, {get_connection, Id}, infinity).

pget(Key, List) ->
    case proplists:get_value(Key, List) of
        undefined -> unknown;
        Val -> Val
    end.

id(Pid) when is_pid(Pid) -> list_to_binary(pid_to_list(Pid));
id(List) -> rabbit_management_format:pid(pget(pid, List)).

add(unknown, _) -> unknown;
add(_, unknown) -> unknown;
add(A, B)       -> A + B.

%%----------------------------------------------------------------------------

lookup_element(Table, Key) ->
    lookup_element(Table, Key, 2).

lookup_element(Table, Key, Pos) ->
    try ets:lookup_element(Table, Key, Pos)
    catch error:badarg -> []
    end.

result_or_error([]) -> error;
result_or_error(S)  -> S.

rates(Table, Stats, Timestamp, Keys) ->
    Stats ++
        [{list_to_atom(atom_to_list(Key) ++ "_rate"),
          rate(Table, Stats, Timestamp, Key)} || Key <- Keys].

rate(Table, Stats, Timestamp, Key) ->
    Old = lookup_element(Table, {id(Stats), stats}),
    OldTS = lookup_element(Table, {id(Stats), stats}, 3),
    case OldTS of
        [] ->
            unknown;
        _ ->
            Diff = pget(Key, Stats) - pget(Key, Old),
            Diff / (timer:now_diff(Timestamp, OldTS) / 1000000)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{queue_stats = ets:new(anon, [private]),
                connection_stats = ets:new(anon, [private])}}.

handle_call({get_queue_stats, QPids}, State = #state{queue_stats = Table}) ->
    {ok, [lookup_element(Table, id(QPid)) || QPid <- QPids], State};

handle_call(get_connections, State = #state{connection_stats = Table}) ->
    {ok, [Stats ++ lookup_element(Table, {Pid, stats}) ||
             {{Pid, create}, Stats} <- ets:tab2list(Table)], State};

handle_call({get_connection, Id}, State = #state{connection_stats = Table}) ->
    {ok, result_or_error(lookup_element(Table, {Id, create}) ++
                             lookup_element(Table, {Id, stats})), State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type = queue_stats, props = Stats},
             State = #state{queue_stats = Table}) ->
    ets:insert(Table, {id(Stats), Stats}),
    {ok, State};

handle_event(#event{type = queue_deleted, props = [{pid, Pid}]},
             State = #state{queue_stats = Table}) ->
    ets:delete(Table, Pid),
    {ok, State};

handle_event(#event{type = connection_created, props = Stats},
             State = #state{connection_stats = Table}) ->
    ets:insert(Table, {{id(Stats), create}, Stats}),
    {ok, State};

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State = #state{connection_stats = Table}) ->
    ets:insert(Table,
               {{id(Stats), stats},
                rates(Table, Stats, Timestamp, [recv_oct, send_oct]),
                Timestamp}),
    {ok, State};

handle_event(#event{type = connection_closed, props = [{pid, Pid}]},
             State = #state{connection_stats = Table}) ->
    ets:delete(Table, {Pid, create}),
    ets:delete(Table, {Pid, stats}),
    {ok, State};

handle_event(_Event, State) ->
    %% io:format("Got event ~p~n", [Event]),
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

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

-export([get_queue_stats/1, get_connection_stats/0, pget/2, add/2]).

-export([init/1, handle_call/2, handle_event/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queue_stats, connection_stats}).

%%----------------------------------------------------------------------------

start() ->
    gen_event:add_sup_handler(rabbit_event, ?MODULE, []).

get_queue_stats(QPids) ->
    gen_event:call(rabbit_event, ?MODULE, {get_queue_stats, QPids}, infinity).

get_connection_stats() ->
    gen_event:call(rabbit_event, ?MODULE, get_connection_stats, infinity).

pget(Key, List) ->
    case proplists:get_value(Key, List) of
        undefined -> unknown;
        Val -> Val
    end.

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

rate(Conn, Table, Key, Timestamp) ->
    Old = lookup_element(Table, {pget(pid, Conn), stats}),
    OldTS = lookup_element(Table, {pget(pid, Conn), stats}, 3),
    case OldTS of
        [] ->
            unknown;
        _ ->
            Diff = pget(Key, Conn) - pget(Key, Old),
            Diff / (timer:now_diff(Timestamp, OldTS) / 1000000)
    end.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, #state{queue_stats = ets:new(anon, [private]),
                connection_stats = ets:new(anon, [private])}}.

handle_call({get_queue_stats, QPids}, State = #state{queue_stats = Table}) ->
    {ok, [lookup_element(Table, QPid) || QPid <- QPids], State};

handle_call(get_connection_stats, State = #state{connection_stats = Table}) ->
    {ok, [Stats ++ lookup_element(Table, {Pid, stats}) ||
             {{Pid, create}, Stats} <- ets:tab2list(Table)], State};

handle_call(_Request, State) ->
    {ok, not_understood, State}.

handle_event(#event{type = queue_stats, props = Stats},
             State = #state{queue_stats = Table}) ->
    ets:insert(Table, {pget(pid, Stats), Stats}),
    {ok, State};

handle_event(#event{type = queue_deleted, props = [{pid, Pid}]},
             State = #state{queue_stats = Table}) ->
    ets:delete(Table, Pid),
    {ok, State};

handle_event(#event{type = connection_created, props = Stats},
             State = #state{connection_stats = Table}) ->
    ets:insert(Table, {{pget(pid, Stats), create}, Stats}),
    {ok, State};

handle_event(#event{type = connection_stats, props = Stats,
                    timestamp = Timestamp},
             State = #state{connection_stats = Table}) ->
    ets:insert(Table,
               {{pget(pid, Stats), stats},
                [{recv_rate, rate(Stats, Table, recv_oct, Timestamp)},
                 {send_rate, rate(Stats, Table, send_oct, Timestamp)}] ++
                    Stats,
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

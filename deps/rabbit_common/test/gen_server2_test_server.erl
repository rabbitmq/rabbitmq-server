%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(gen_server2_test_server).
-behaviour(gen_server2).
-record(gs2_state, {parent, name, state, mod, time,
                    timeout_state, queue, debug, prioritisers,
                    timer, emit_stats_fun, stop_stats_fun}).

-export([start_link/0, start_link/1, start_link/2, stats_count/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, handle_post_hibernate/1]).

start_link(count_stats) ->
    start_link(count_stats, infinity).

start_link(count_stats, Time) ->
    {ok, Server} = gen_server2:start_link(gen_server2_test_server, [Time], []),
    Counter = gen_server2:call(Server, get_counter),
    sys:replace_state(Server,
        fun(GSState) ->
            GSState#gs2_state{
                emit_stats_fun = fun(State) -> count_stats(Counter), State end
            }
        end),
    {ok, Server}.

start_link() ->
    gen_server2:start_link(gen_server2_test_server, [], []).

stats_count(Server) ->
    Counter = gen_server2:call(Server, get_counter),
    [{count, Count}] = ets:lookup(Counter, count),
    Count.

init([]) ->
    init([infinity]);
init([Time]) ->
    Counter = ets:new(stats_count, [public]),
    ets:insert(Counter, {count, 0}),
    case Time of
        {backoff, _, _, _} ->
            {ok, {counter, Counter}, hibernate, Time};
        _ ->
            {ok, {counter, Counter}, Time}
    end.

count_stats(Counter) ->
    ets:update_counter(Counter, count, {2, 1}).

handle_call(get_counter,_, {counter, Counter} = State) ->
    {reply, Counter, State};
handle_call(hibernate, _, State) ->
    {reply, ok, State, hibernate};
handle_call(_,_,State) ->
    {reply, ok, State}.

handle_cast({sleep, Time}, State) -> timer:sleep(Time), {noreply, State};
handle_cast(_,State) -> {noreply, State}.

handle_post_hibernate(State) -> {noreply, State}.

handle_info(_,State) -> {noreply, State}.

terminate(_,_State) -> ok.

code_change(_,State,_) -> {ok, State}.

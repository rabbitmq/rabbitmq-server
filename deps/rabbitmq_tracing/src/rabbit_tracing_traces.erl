%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracing_traces).

-behaviour(gen_server).

-import(rabbit_misc, [pget/2]).

-export([list/0, lookup/2, create/3, stop/2, announce/3]).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { table }).

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

list() ->
    gen_server:call(?MODULE, list, infinity).

lookup(VHost, Name) ->
    gen_server:call(?MODULE, {lookup, VHost, Name}, infinity).

create(VHost, Name, Trace) ->
    gen_server:call(?MODULE, {create, VHost, Name, Trace}, infinity).

stop(VHost, Name) ->
    gen_server:call(?MODULE, {stop, VHost, Name}, infinity).

announce(VHost, Name, Pid) ->
    gen_server:cast(?MODULE, {announce, {VHost, Name}, Pid}).

%%--------------------------------------------------------------------

init([]) ->
    {ok, #state{table = ets:new(anon, [private])}}.

handle_call(list, _From, State = #state{table = Table}) ->
    {reply, [augment(Trace) || {_K, Trace} <- ets:tab2list(Table)], State};

handle_call({lookup, VHost, Name}, _From, State = #state{table = Table}) ->
    {reply, case ets:lookup(Table, {VHost, Name}) of
                []            -> not_found;
                [{_K, Trace}] -> augment(Trace)
            end, State};

handle_call({create, VHost, Name, Trace0}, _From,
            State = #state{table = Table}) ->
    Already = vhost_tracing(VHost, Table),
    Trace = pset(vhost, VHost, pset(name, Name, Trace0)),
    true = ets:insert(Table, {{VHost, Name}, Trace}),
    case Already of
        true  -> ok;
        false -> rabbit_trace:start(VHost)
    end,
    {reply, rabbit_tracing_sup:start_child({VHost, Name}, Trace), State};

handle_call({stop, VHost, Name}, _From, State = #state{table = Table}) ->
    true = ets:delete(Table, {VHost, Name}),
    case vhost_tracing(VHost, Table) of
        true  -> ok;
        false -> rabbit_trace:stop(VHost)
    end,
    rabbit_tracing_sup:stop_child({VHost, Name}),
    {reply, ok, State};

handle_call(_Req, _From, State) ->
    {reply, unknown_request, State}.

handle_cast({announce, Key, Pid}, State = #state{table = Table}) ->
    case ets:lookup(Table, Key) of
        []           -> ok;
        [{_, Trace}] -> ets:insert(Table, {Key, pset(pid, Pid, Trace)})
    end,
    {noreply, State};

handle_cast(_C, State) ->
    {noreply, State}.

handle_info(_I, State) ->
    {noreply, State}.

terminate(_, _) -> ok.

code_change(_, State, _) -> {ok, State}.

%%--------------------------------------------------------------------

pset(Key, Value, List) -> [{Key, Value} | proplists:delete(Key, List)].

vhost_tracing(VHost, Table) ->
    case [true || {{V, _}, _} <- ets:tab2list(Table), V =:= VHost] of
        [] -> false;
        _  -> true
    end.

augment(Trace) ->
    Pid = pget(pid, Trace),
    Trace1 = lists:keydelete(tracer_connection_password, 1,
                             lists:keydelete(<<"tracer_connection_password">>, 1,
                                             lists:keydelete(pid, 1, Trace))),
    case Pid of
        undefined -> Trace1;
        _         -> rabbit_tracing_consumer:info_all(Pid) ++ Trace1
    end.

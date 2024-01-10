%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_presence).

-behaviour(gen_server).

-export([list_present/0]).
-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(CUTOFF, ?INTERVAL * 3).

-record(?MODULE, {tbl :: ets:table(),
                  nodes = [] :: [node()]}).

%%----------------------------------------------------------------------------
%% A presence server that heartbeats all configured servers with the goal of
%% providing a very quickly accessible idea of node availability without
%% having to use rabbit_nodes:all_running/1 which can block for a long time.
%%----------------------------------------------------------------------------

-spec list_present() -> [node()].
list_present() ->
    case whereis(?MODULE) of
        undefined ->
            %% TODO: change return type to ok | error?
            exit(presence_server_not_running);
        _ ->
            Cutoff = erlang:system_time(millisecond) - ?CUTOFF,
            [N || {N, SeenMs} <- ets:tab2list(?MODULE),
                  %% if it hasn't been seen since the cutoff
                  SeenMs > Cutoff,
                  %% if not in nodes() it is also considered not present
                  lists:member(N, nodes())]
    end.

-spec start_link() -> rabbit_types:ok_pid_or_error().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    Ref = ets:new(?MODULE, [set, named_table, protected]),
    _ = erlang:send_after(?INTERVAL, self(), beat),
    Nodes = rabbit_nodes:list_members(),
    _ = beat_all(Nodes),
    {ok, #?MODULE{tbl = Ref,
                  nodes = Nodes}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(beat, #?MODULE{tbl = _Tbl,
                           nodes = Nodes} = State) ->
    _ = erlang:send_after(?INTERVAL, self(), beat),
    _ = beat_all(Nodes),
    %% this will only be efficient to do this often once list_members
    %% make use of the ra_leaderboard rather than calling into the local
    %% khepri process
    case rabbit_nodes:list_members() of
        Nodes ->
            {noreply, State};
        NewNodes ->
            {noreply, State#?MODULE{nodes = NewNodes}}
    end;
handle_info({hb, Node}, #?MODULE{tbl = Tbl,
                                 nodes = _Nodes} = State) ->
    ets:insert(Tbl, {Node, erlang:system_time(millisecond)}),
    {noreply, State};
handle_info({terminate, Node}, #?MODULE{tbl = Tbl} = State) ->
    _ = ets:delete(Tbl, Node),
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #?MODULE{nodes = Nodes}) ->
    %% TODO: only send terminate if reason is `shutdown`?
    _ = send_terminate(Nodes),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% INTERNAL

beat_all(Nodes) ->
    [send(N, {hb, node()}) || N <- Nodes, N =/= node()].

send_terminate(Nodes) ->
    [send(N, {terminate, node()}) || N <- Nodes, N =/= node()].

send(Node, Msg) ->
    erlang:send({?SERVER, Node}, Msg, [noconnect, nosuspend]).

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_mgmt_storage).
-behaviour(gen_server2).
-record(state, {}).

-spec start_link() -> rabbit_types:ok_pid_or_error().

-export([start_link/0]).
-export([reset/0, reset_all/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("rabbit_mgmt_metrics.hrl").

%% ETS owner
start_link() ->
    gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    rabbit_log:warning("Resetting RabbitMQ management storage"),
    [ets:delete_all_objects(IndexTable) || IndexTable <- ?INDEX_TABLES],
    [ets:delete_all_objects(Table) || {Table, _} <- ?TABLES],
    _ = rabbit_mgmt_metrics_collector:reset_all(),
    ok.

reset_all() ->
    _ = [rpc:call(Node, rabbit_mgmt_storage, reset, [])
         || Node <- rabbit_nodes:all_running()],
    ok.

init(_) ->
    _ = [ets:new(IndexTable, [public, bag, named_table])
         || IndexTable <- ?INDEX_TABLES],
    _ = [ets:new(Table, [public, Type, named_table])
         || {Table, Type} <- ?TABLES],
    _ = ets:new(rabbit_mgmt_db_cache, [public, set, named_table]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

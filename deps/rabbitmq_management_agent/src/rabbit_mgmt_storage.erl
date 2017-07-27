%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
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

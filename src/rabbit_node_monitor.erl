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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_node_monitor).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-export([notify_cluster/0, rabbit_running_on/1]).

-define(SERVER, ?MODULE).
-define(RABBIT_UP_RPC_TIMEOUT, 2000).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(rabbit_running_on/1 :: (node()) -> 'ok').
-spec(notify_cluster/0 :: () -> 'ok').

-endif.

%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

rabbit_running_on(Node) ->
    gen_server:cast(rabbit_node_monitor, {rabbit_running_on, Node}).

notify_cluster() ->
    Node = node(),
    Nodes = rabbit_mnesia:running_clustered_nodes() -- [Node],
    %% notify other rabbits of this rabbit
    case rpc:multicall(Nodes, rabbit_node_monitor, rabbit_running_on,
                       [Node], ?RABBIT_UP_RPC_TIMEOUT) of
        {_, [] } -> ok;
        {_, Bad} -> rabbit_log:info("failed to contact nodes ~p~n", [Bad])
    end,
    %% register other active rabbits with this rabbit
    [ rabbit_running_on(N) || N <- Nodes ],
    ok.

%%--------------------------------------------------------------------

init([]) ->
    {ok, ordsets:new()}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({rabbit_running_on, Node}, Nodes) ->
    case ordsets:is_element(Node, Nodes) of
        true  -> {noreply, Nodes};
        false -> rabbit_log:info("rabbit on node ~p up~n", [Node]),
                 erlang:monitor(process, {rabbit, Node}),
                 ok = handle_live_rabbit(Node),
                 {noreply, ordsets:add_element(Node, Nodes)}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, {rabbit, Node}, _Reason}, Nodes) ->
    rabbit_log:info("rabbit on node ~p down~n", [Node]),
    ok = handle_dead_rabbit(Node),
    {noreply, ordsets:del_element(Node, Nodes)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------

%% TODO: This may turn out to be a performance hog when there are lots
%% of nodes.  We really only need to execute some of these statements
%% on *one* node, rather than all of them.
handle_dead_rabbit(Node) ->
    ok = rabbit_networking:on_node_down(Node),
    ok = rabbit_amqqueue:on_node_down(Node),
    ok = rabbit_alarm:on_node_down(Node),
    ok = rabbit_mnesia:on_node_down(Node).

handle_live_rabbit(Node) ->
    ok = rabbit_alarm:on_node_up(Node),
    ok = rabbit_mnesia:on_node_up(Node).

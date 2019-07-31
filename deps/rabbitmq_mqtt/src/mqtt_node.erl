%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(mqtt_node).

-export([start/0, node_id/0, leave/1]).

-define(START_TIMEOUT, 100000).
-define(RETRY_INTERVAL, 5000).

node_id() ->
    node_id(node()).

node_id(Node) ->
    {mqtt_node, Node}.

start() ->
    Name = mqtt_node,
    NodeId = node_id(),
    Nodes = [{Name, N} || N <- rabbit_mnesia:cluster_nodes(all),
                          can_participate_in_clientid_tracking(N)] -- [NodeId],
    Res = case ra_directory:uid_of(Name) of
              undefined ->
                  UId = ra:new_uid(ra_lib:to_binary(Name)),
                  Timeout = application:get_env(kernel, net_ticktime, 60) + 5,
                  Conf = #{cluster_name => Name,
                           id => NodeId,
                           uid => UId,
                           friendly_name => Name,
                           initial_members => Nodes,
                           log_init_args => #{uid => UId},
                           tick_timeout => Timeout,
                           machine => {module, mqtt_machine, #{}}},
                  ra:start_server(Conf),
                  %% Trigger an election.
                  %% This is required when we start a node for the first time.
                  %% Using default timeout because it supposed to reply fast.
                  case Nodes of
                    [] ->
                      rabbit_log:info("MQTT: observed no cluster peers that support client ID tracking, assuming we should start a new Raft leader election"),
                      ra:trigger_election(NodeId);
                    _        -> ok
                  end;
              _ ->
                  ra:restart_server(NodeId)
          end,
    case Res of
        ok ->
          spawn(fun() -> join_peers(NodeId, Nodes) end),
          ok;
        _  -> Res
    end.

join_peers(_NodeId, []) ->
    ok;
join_peers(NodeId, Nodes) ->
    join_peers(NodeId, Nodes, 100).
join_peers(_NodeId, [], _RetriesLeft) ->
    ok;
join_peers(_NodeId, _Nodes, RetriesLeft) when RetriesLeft =:= 0 ->
    rabbit_log:error("MQTT: exhausted all attempts while trying to rejoin cluster peers");
join_peers(NodeId, Nodes, RetriesLeft) ->
    case ra:members(Nodes, ?START_TIMEOUT) of
        {ok, Members, _} ->
            case lists:member(NodeId, Members) of
                true  -> ok;
                false -> ra:add_member(Members, NodeId)
            end;
        {timeout, _} ->
            rabbit_log:debug("MQTT: timed out contacting cluster peers, %s retries left", [RetriesLeft]),
            timer:sleep(?RETRY_INTERVAL),
            join_peers(NodeId, Nodes, RetriesLeft - 1);
        Err ->
            Err
    end.

-spec leave(node()) -> 'ok' | 'timeout' | 'nodedown'.
leave(Node) ->
    NodeId = node_id(),
    ToLeave = node_id(Node),
    try
        ra:leave_and_delete_server(NodeId, ToLeave)
    catch
        exit:{{nodedown, Node}, _} ->
            nodedown
    end.

can_participate_in_clientid_tracking(Node) ->
    case rpc:call(Node, mqtt_machine, module_info, []) of
        {badrpc, _} -> false;
        _           -> true
    end.

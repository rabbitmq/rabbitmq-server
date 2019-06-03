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

node_id() ->
    node_id(node()).

node_id(Node) ->
    {mqtt_node, Node}.

start() ->
    Name = mqtt_node,
    NodeId = node_id(),
    Nodes = [{Name, N} || N <- rabbit_mnesia:cluster_nodes(all)] -- [NodeId],
    Res = case ra_directory:uid_of(Name) of
              undefined ->
                  UId = ra:new_uid(ra_lib:to_binary(Name)),
                  Conf = #{cluster_name => Name,
                           id => NodeId,
                           uid => UId,
                           friendly_name => Name,
                           initial_members => Nodes,
                           log_init_args => #{uid => UId},
                           tick_timeout => 5000,
                           machine => {module, mqtt_machine, #{}}},
                  ra:start_server(Conf);
              _ ->
                  ra:restart_server(NodeId)
          end,
    case Res of
        ok ->
            case Nodes of
                [] -> ok;
                [Member | _] -> ra:add_member(Member, NodeId)
            end,
            %% Trigger election.
            %% This is required when we start a node for the first time.
            %% Using default timeout because it supposed to reply fast.
            ra:trigger_election(NodeId),
            case ra:members(NodeId, ?START_TIMEOUT) of
                {ok, _, _} ->
                    ok;
                {timeout, _} = Err ->
                    rabbit_log:warning("MQTT: timed out contacting cluster peers"),
                    Err;
                Err ->
                    Err
            end;
        _ ->
            Res
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

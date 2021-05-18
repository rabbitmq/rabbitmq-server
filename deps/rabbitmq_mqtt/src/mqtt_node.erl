%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(mqtt_node).

-export([start/0, node_id/0, server_id/0, all_node_ids/0, leave/1, trigger_election/0]).

-define(ID_NAME, mqtt_node).
-define(START_TIMEOUT, 100000).
-define(RETRY_INTERVAL, 5000).
-define(RA_OPERATION_TIMEOUT, 60000).

node_id() ->
    server_id(node()).

server_id() ->
    server_id(node()).

server_id(Node) ->
    {?ID_NAME, Node}.

all_node_ids() ->
    [server_id(N) || N <- rabbit_mnesia:cluster_nodes(all),
                   can_participate_in_clientid_tracking(N)].

start() ->
    %% 3s to 6s randomized
    Repetitions = rand:uniform(10) + 10,
    start(300, Repetitions).

start(_Delay, AttemptsLeft) when AttemptsLeft =< 0 ->
    start_server(),
    trigger_election();
start(Delay, AttemptsLeft) ->
    NodeId = server_id(),
    Nodes = compatible_peer_servers(),
    case ra_directory:uid_of(?ID_NAME) of
          undefined ->
              case Nodes of
                  [] ->
                      %% Since cluster members are not known ahead of time and initial boot can be happening in parallel,
                      %% we wait and check a few times (up to a few seconds) to see if we can discover any peers to
                      %% join before forming a cluster. This reduces the probability of N independent clusters being
                      %% formed in the common scenario of N nodes booting in parallel e.g. because they were started
                      %% at the same time by a deployment tool.
                      %%
                      %% This scenario does not guarantee single cluster formation but without knowing the list of members
                      %% ahead of time, this is a best effort workaround. Multi-node consensus is apparently hard
                      %% to achieve without having consensus around expected cluster members.
                      _ = rabbit_log:info("MQTT: will wait for ~p more ms for cluster members to join before triggering a Raft leader election", [Delay]),
                      timer:sleep(Delay),
                      start(Delay, AttemptsLeft - 1);
                  Peers ->
                      %% Trigger an election.
                      %% This is required when we start a node for the first time.
                      %% Using default timeout because it supposed to reply fast.
                      _ = rabbit_log:info("MQTT: discovered ~p cluster peers that support client ID tracking", [length(Peers)]),
                      start_server(),
                      join_peers(NodeId, Peers),
                      ra:trigger_election(NodeId, ?RA_OPERATION_TIMEOUT)
              end;
          _ ->
              join_peers(NodeId, Nodes),
              ra:restart_server(NodeId),
              ra:trigger_election(NodeId, ?RA_OPERATION_TIMEOUT)
    end,
    ok.

compatible_peer_servers() ->
    all_node_ids() -- [(node_id())].

start_server() ->
    NodeId = node_id(),
    Nodes = compatible_peer_servers(),
    UId = ra:new_uid(ra_lib:to_binary(?ID_NAME)),
    Timeout = application:get_env(kernel, net_ticktime, 60) + 5,
    Conf = #{cluster_name => ?ID_NAME,
             id => NodeId,
             uid => UId,
             friendly_name => ?ID_NAME,
             initial_members => Nodes,
             log_init_args => #{uid => UId},
             tick_timeout => Timeout,
             machine => {module, mqtt_machine, #{}}
    },
    ra:start_server(Conf).

trigger_election() ->
    ra:trigger_election(server_id(), ?RA_OPERATION_TIMEOUT).

join_peers(_NodeId, []) ->
    ok;
join_peers(NodeId, Nodes) ->
    join_peers(NodeId, Nodes, 100).
join_peers(_NodeId, [], _RetriesLeft) ->
    ok;
join_peers(_NodeId, _Nodes, RetriesLeft) when RetriesLeft =:= 0 ->
    _ = rabbit_log:error("MQTT: exhausted all attempts while trying to rejoin cluster peers");
join_peers(NodeId, Nodes, RetriesLeft) ->
    case ra:members(Nodes, ?START_TIMEOUT) of
        {ok, Members, _} ->
            case lists:member(NodeId, Members) of
                true  -> ok;
                false -> ra:add_member(Members, NodeId)
            end;
        {timeout, _} ->
            _ = rabbit_log:debug("MQTT: timed out contacting cluster peers, %s retries left", [RetriesLeft]),
            timer:sleep(?RETRY_INTERVAL),
            join_peers(NodeId, Nodes, RetriesLeft - 1);
        Err ->
            Err
    end.

-spec leave(node()) -> 'ok' | 'timeout' | 'nodedown'.
leave(Node) ->
    NodeId = server_id(),
    ToLeave = server_id(Node),
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

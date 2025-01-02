%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_maintenance).

-include_lib("rabbit_common/include/rabbit.hrl").

%% FIXME: Ra consistent queries are currently fragile in the sense that the
%% query function may run on a remote node and the function reference or MFA
%% may not be valid on that node. That's why consistent queries in this module
%% are in fact local queries when Khepri is enabled.
%%
%% See `rabbit_db_maintenance:get_consistent_in_khepri/1'.

-export([
    is_enabled/0,
    drain/0,
    revive/0,
    mark_as_being_drained/0,
    unmark_as_being_drained/0,
    is_being_drained_local_read/1,
    is_being_drained_consistent_read/1,
    status_local_read/1,
    status_consistent_read/1,
    filter_out_drained_nodes_local_read/1,
    filter_out_drained_nodes_consistent_read/1,
    suspend_all_client_listeners/0,
    resume_all_client_listeners/0,
    close_all_client_connections/0,
    primary_replica_transfer_candidate_nodes/0,
    random_primary_replica_transfer_candidate_node/2,
    transfer_leadership_of_quorum_queues/1,
    table_definitions/0
]).

-define(DEFAULT_STATUS,  regular).
-define(DRAINING_STATUS, draining).

-type maintenance_status() :: ?DEFAULT_STATUS | ?DRAINING_STATUS.

-export_type([
    maintenance_status/0
]).

%%
%% Boot
%%

table_definitions() ->
    rabbit_db_maintenance:table_definitions().

%%
%% API
%%

-spec is_enabled() -> boolean().
is_enabled() ->
    true.

-spec drain() -> ok.
drain() ->
    rabbit_log:warning("This node is being put into maintenance (drain) mode"),
    mark_as_being_drained(),
    rabbit_log:info("Marked this node as undergoing maintenance"),
    _ = suspend_all_client_listeners(),
    rabbit_log:warning("Suspended all listeners and will no longer accept client connections"),
    {ok, NConnections} = close_all_client_connections(),
    rabbit_log:warning("Closed ~b local client connections", [NConnections]),
    %% allow plugins to react e.g. by closing their protocol connections
    rabbit_event:notify(maintenance_connections_closed, #{
        reason => <<"node is being put into maintenance">>
    }),

    TransferCandidates = primary_replica_transfer_candidate_nodes(),
    %% Note: only QQ leadership is transferred because it is a reasonably quick thing to do a lot of queues
    %% in the cluster, unlike with CMQs.
    transfer_leadership_of_quorum_queues(TransferCandidates),
    stop_local_quorum_queue_followers(),

    case whereis(rabbit_stream_coordinator) of
        undefined -> ok;
        _Pid -> transfer_leadership_of_stream_coordinator(TransferCandidates)
    end,

    transfer_leadership_of_metadata_store(TransferCandidates),

    %% allow plugins to react
    rabbit_event:notify(maintenance_draining, #{
        reason => <<"node is being put into maintenance">>
    }),
    rabbit_log:info("Node is ready to be shut down for maintenance or upgrade"),

    ok.

-spec revive() -> ok.
revive() ->
    rabbit_log:info("This node is being revived from maintenance (drain) mode"),
    revive_local_quorum_queue_replicas(),
    rabbit_log:info("Resumed all listeners and will accept client connections again"),
    _ = resume_all_client_listeners(),
    rabbit_log:info("Resumed all listeners and will accept client connections again"),
    unmark_as_being_drained(),
    rabbit_log:info("Marked this node as back from maintenance and ready to serve clients"),

    %% allow plugins to react
    rabbit_event:notify(maintenance_revived, #{}),

    ok.

-spec mark_as_being_drained() -> boolean().
mark_as_being_drained() ->
    rabbit_log:debug("Marking the node as undergoing maintenance"),
    rabbit_db_maintenance:set(?DRAINING_STATUS).

-spec unmark_as_being_drained() -> boolean().
unmark_as_being_drained() ->
    rabbit_log:debug("Unmarking the node as undergoing maintenance"),
    rabbit_db_maintenance:set(?DEFAULT_STATUS).

-spec is_being_drained_local_read(node()) -> boolean().
is_being_drained_local_read(Node) ->
    Status = status_local_read(Node),
    Status =:= ?DRAINING_STATUS.

-spec is_being_drained_consistent_read(node()) -> boolean().
is_being_drained_consistent_read(Node) ->
    Status = status_consistent_read(Node),
    Status =:= ?DRAINING_STATUS.

-spec status_local_read(node()) -> maintenance_status().
status_local_read(Node) ->
    case rabbit_db_maintenance:get(Node) of
        undefined ->
            ?DEFAULT_STATUS;
        Status ->
            Status
    end.

-spec status_consistent_read(node()) -> maintenance_status().
status_consistent_read(Node) ->
    case rabbit_db_maintenance:get_consistent(Node) of
        undefined ->
            ?DEFAULT_STATUS;
        Status ->
            Status
    end.

 -spec filter_out_drained_nodes_local_read([node()]) -> [node()].
filter_out_drained_nodes_local_read(Nodes) ->
    lists:filter(fun(N) -> not is_being_drained_local_read(N) end, Nodes).

-spec filter_out_drained_nodes_consistent_read([node()]) -> [node()].
filter_out_drained_nodes_consistent_read(Nodes) ->
    lists:filter(fun(N) -> not is_being_drained_consistent_read(N) end, Nodes).

-spec suspend_all_client_listeners() -> rabbit_types:ok_or_error(any()).
 %% Pauses all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A resumed listener will not accept any new client connections
 %% but previously established connections won't be interrupted.
suspend_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    rabbit_log:info("Asked to suspend ~b client connection listeners. "
                    "No new client connections will be accepted until these listeners are resumed!", [length(Listeners)]),
    Results = lists:foldl(local_listener_fold_fun(fun ranch:suspend_listener/1), [], Listeners),
    lists:foldl(fun ok_or_first_error/2, ok, Results).

 -spec resume_all_client_listeners() -> rabbit_types:ok_or_error(any()).
 %% Resumes all listeners on the current node except for
 %% Erlang distribution (clustering and CLI tools).
 %% A resumed listener will accept new client connections.
resume_all_client_listeners() ->
    Listeners = rabbit_networking:node_client_listeners(node()),
    rabbit_log:info("Asked to resume ~b client connection listeners. "
                    "New client connections will be accepted from now on", [length(Listeners)]),
    Results = lists:foldl(local_listener_fold_fun(fun ranch:resume_listener/1), [], Listeners),
    lists:foldl(fun ok_or_first_error/2, ok, Results).

 -spec close_all_client_connections() -> {'ok', non_neg_integer()}.
close_all_client_connections() ->
    Pids = rabbit_networking:local_connections(),
    rabbit_networking:close_connections(Pids, "Node was put into maintenance mode"),
    {ok, length(Pids)}.

-spec transfer_leadership_of_quorum_queues([node()]) -> ok.
transfer_leadership_of_quorum_queues([]) ->
    rabbit_log:warning("Skipping leadership transfer of quorum queues: no candidate "
                       "(online, not under maintenance) nodes to transfer to!");
transfer_leadership_of_quorum_queues(_TransferCandidates) ->
    %% we only transfer leadership for QQs that have local leaders
    Queues = rabbit_amqqueue:list_local_leaders(),
    rabbit_log:info("Will transfer leadership of ~b quorum queues with current leader on this node",
                    [length(Queues)]),
    [begin
        Name = amqqueue:get_name(Q),
        rabbit_log:debug("Will trigger a leader election for local quorum queue ~ts",
                         [rabbit_misc:rs(Name)]),
        %% we trigger an election and exclude this node from the list of candidates
        %% by simply shutting its local QQ replica (Ra server)
        RaLeader = amqqueue:get_pid(Q),
        rabbit_log:debug("Will stop Ra server ~tp", [RaLeader]),
        case rabbit_quorum_queue:stop_server(RaLeader) of
            ok     ->
                rabbit_log:debug("Successfully stopped Ra server ~tp", [RaLeader]);
            {error, nodedown} ->
                rabbit_log:error("Failed to stop Ra server ~tp: target node was reported as down")
        end
     end || Q <- Queues],
    rabbit_log:info("Leadership transfer for quorum queues hosted on this node has been initiated").

transfer_leadership_of_metadata_store(TransferCandidates) ->
    rabbit_log:info("Will transfer leadership of metadata store with current leader on this node",
                    []),
    case rabbit_khepri:transfer_leadership(TransferCandidates) of
        {ok, Node} when Node == node(); Node == undefined ->
            rabbit_log:info("Skipping leadership transfer of metadata store: current leader is not on this node");
        {ok, Node} ->
            rabbit_log:info("Leadership transfer for metadata store on this node has been done. The new leader is ~p", [Node]);
        Error ->
            rabbit_log:warning("Skipping leadership transfer of metadata store: ~p", [Error])
    end.

-spec transfer_leadership_of_stream_coordinator([node()]) -> ok.
transfer_leadership_of_stream_coordinator([]) ->
    rabbit_log:warning("Skipping leadership transfer of stream coordinator: no candidate "
                       "(online, not under maintenance) nodes to transfer to!");
transfer_leadership_of_stream_coordinator(TransferCandidates) ->
    % try to transfer to the node with the lowest uptime; the assumption is that
    % nodes are usually restarted in a rolling fashion, in a consistent order;
    % therefore, the youngest node has already been restarted  or (if we are draining the first node)
    % that it will be restarted last. either way, this way we limit the number of transfers
    Uptimes = rabbit_misc:append_rpc_all_nodes(TransferCandidates, erlang, statistics, [wall_clock]),
    Candidates = lists:zipwith(fun(N, {U, _}) -> {N, U}  end, TransferCandidates, Uptimes),
    BestCandidate = element(1, hd(lists:keysort(2, Candidates))),
    case rabbit_stream_coordinator:transfer_leadership([BestCandidate]) of
        {ok, Node} ->
            rabbit_log:info("Leadership transfer for stream coordinator completed. The new leader is ~p", [Node]);
        Error ->
            rabbit_log:warning("Skipping leadership transfer of stream coordinator: ~p", [Error])
    end.

-spec stop_local_quorum_queue_followers() -> ok.
stop_local_quorum_queue_followers() ->
    Queues = rabbit_amqqueue:list_local_followers(),
    rabbit_log:info("Will stop local follower replicas of ~b quorum queues on this node",
                    [length(Queues)]),
    [begin
        Name = amqqueue:get_name(Q),
        rabbit_log:debug("Will stop a local follower replica of quorum queue ~ts",
                         [rabbit_misc:rs(Name)]),
        %% shut down Ra nodes so that they are not considered for leader election
        {RegisteredName, _LeaderNode} = amqqueue:get_pid(Q),
        RaNode = {RegisteredName, node()},
        rabbit_log:debug("Will stop Ra server ~tp", [RaNode]),
        case rabbit_quorum_queue:stop_server(RaNode) of
            ok     ->
                rabbit_log:debug("Successfully stopped Ra server ~tp", [RaNode]);
            {error, nodedown} ->
                rabbit_log:error("Failed to stop Ra server ~tp: target node was reported as down")
        end
     end || Q <- Queues],
    rabbit_log:info("Stopped all local replicas of quorum queues hosted on this node").

-spec primary_replica_transfer_candidate_nodes() -> [node()].
primary_replica_transfer_candidate_nodes() ->
    filter_out_drained_nodes_consistent_read(rabbit_nodes:list_running() -- [node()]).

-spec random_primary_replica_transfer_candidate_node([node()], [node()]) -> {ok, node()} | undefined.
random_primary_replica_transfer_candidate_node([], _Preferred) ->
    undefined;
random_primary_replica_transfer_candidate_node(Candidates, PreferredNodes) ->
    Overlap = sets:to_list(sets:intersection(sets:from_list(Candidates), sets:from_list(PreferredNodes))),
    Candidate = case Overlap of
                    [] ->
                        %% Since ownership transfer is meant to be run only when we are sure
                        %% there are in-sync replicas to transfer to, this is an edge case.
                        %% We skip the transfer.
                        undefined;
                    Nodes ->
                        random_nth(Nodes)
                end,
    {ok, Candidate}.

random_nth(Nodes) ->
    Nth = erlang:phash2(erlang:monotonic_time(), length(Nodes)),
    lists:nth(Nth + 1, Nodes).

revive_local_quorum_queue_replicas() ->
    Queues = rabbit_amqqueue:list_local_followers(),
    %% NB: this function ignores the first argument so we can just pass the
    %% empty binary as the vhost name.
    {Recovered, Failed} = rabbit_quorum_queue:recover(<<>>, Queues),
    rabbit_log:debug("Successfully revived ~b quorum queue replicas",
                     [length(Recovered)]),
    case length(Failed) of
        0 ->
            ok;
        NumFailed ->
            rabbit_log:error("Failed to revive ~b quorum queue replicas",
                             [NumFailed])
    end,

    rabbit_log:info("Restart of local quorum queue replicas is complete"),
    ok.

%%
%% Implementation
%%

local_listener_fold_fun(Fun) ->
    fun(#listener{node = Node, ip_address = Addr, port = Port}, Acc) when Node =:= node() ->
            RanchRef = rabbit_networking:ranch_ref(Addr, Port),
            [Fun(RanchRef) | Acc];
        (_, Acc) ->
            Acc
    end.

ok_or_first_error(ok, Acc) ->
    Acc;
ok_or_first_error({error, _} = Err, _Acc) ->
    Err.

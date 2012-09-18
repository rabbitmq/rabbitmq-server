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


-module(rabbit_mnesia).

-export([init/0,
         join_cluster/2,
         reset/0,
         force_reset/0,
         update_cluster_nodes/1,
         change_cluster_node_type/1,
         forget_cluster_node/2,

         status/0,
         is_db_empty/0,
         is_clustered/0,
         all_clustered_nodes/0,
         clustered_disc_nodes/0,
         running_clustered_nodes/0,
         node_type/0,
         dir/0,
         table_names/0,
         wait_for_tables/1,
         cluster_status_from_mnesia/0,

         init_db/3,
         empty_ram_only_tables/0,
         copy_db/1,
         wait_for_tables/0,
         check_cluster_consistency/0,
         ensure_mnesia_dir/0,

         on_node_up/1,
         on_node_down/1
        ]).

%% Used internally in rpc calls
-export([node_info/0,
         remove_node_if_mnesia_running/1,
         is_running_remote/0
        ]).

%% create_tables/0 exported for helping embed RabbitMQ in or alongside
%% other mnesia-using Erlang applications, such as ejabberd
-export([create_tables/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([node_type/0, cluster_status/0]).

-type(node_type() :: disc | ram).
-type(node_set() :: ordsets:ordset(node())).
-type(cluster_status() :: {node_set(), node_set(), node_set()}).

%% Main interface
-spec(init/0 :: () -> 'ok').
-spec(join_cluster/2 :: (node(), node_type()) -> 'ok').
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').
-spec(update_cluster_nodes/1 :: (node()) -> 'ok').
-spec(change_cluster_node_type/1 :: (node_type()) -> 'ok').
-spec(forget_cluster_node/2 :: (node(), boolean()) -> 'ok').

%% Various queries to get the status of the db
-spec(status/0 :: () -> [{'nodes', [{node_type(), node_set()}]} |
                         {'running_nodes', node_set()}]).
-spec(is_db_empty/0 :: () -> boolean()).
-spec(is_clustered/0 :: () -> boolean()).
-spec(all_clustered_nodes/0 :: () -> node_set()).
-spec(clustered_disc_nodes/0 :: () -> node_set()).
-spec(running_clustered_nodes/0 :: () -> node_set()).
-spec(node_type/0 :: () -> node_type()).
-spec(dir/0 :: () -> file:filename()).
-spec(table_names/0 :: () -> [atom()]).
-spec(cluster_status_from_mnesia/0 :: () -> {'ok', cluster_status()} |
                                             {'error', any()}).

%% Operations on the db and utils, mainly used in `rabbit_upgrade' and `rabbit'
-spec(init_db/3 :: (node_set(), node_type(), boolean()) -> 'ok').
-spec(empty_ram_only_tables/0 :: () -> 'ok').
-spec(create_tables/0 :: () -> 'ok').
-spec(copy_db/1 :: (file:filename()) ->  rabbit_types:ok_or_error(any())).
-spec(wait_for_tables/1 :: ([atom()]) -> 'ok').
-spec(check_cluster_consistency/0 :: () -> 'ok').
-spec(ensure_mnesia_dir/0 :: () -> 'ok').

%% Hooks used in `rabbit_node_monitor'
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

%% Functions used in internal rpc calls
-spec(node_info/0 :: () -> {string(), string(),
                            ({'ok', cluster_status()} | 'error')}).
-spec(remove_node_if_mnesia_running/1 :: (node()) -> 'ok' |
                                                     {'error', term()}).

-endif.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    case is_virgin_node() of
        true  -> init_from_config();
        false -> init(node_type(), all_clustered_nodes())
    end,
    %% We intuitively expect the global name server to be synced when
    %% Mnesia is up. In fact that's not guaranteed to be the case -
    %% let's make it so.
    ok = global:sync(),
    ok.

init(NodeType, AllNodes) ->
    init_db_and_upgrade(AllNodes, NodeType, NodeType =:= disc).

init_from_config() ->
    {ok, {TryNodes, NodeType}} =
        application:get_env(rabbit, cluster_nodes),
    case find_good_node(TryNodes -- [node()]) of
        {ok, Node} ->
            rabbit_log:info("Node '~p' selected for clustering from "
                            "configuration~n", [Node]),
            {ok, {_, DiscNodes, _}} = discover_cluster(Node),
            init_db_and_upgrade(DiscNodes, NodeType, false),
            rabbit_node_monitor:notify_joined_cluster();
        none ->
            rabbit_log:warning("Could not find any suitable node amongst the "
                               "ones provided in the configuration: ~p~n",
                               [TryNodes]),
            init(true, ordsets:from_list([node()]))
    end.

%% Make the node join a cluster. The node will be reset automatically
%% before we actually cluster it. The nodes provided will be used to
%% find out about the nodes in the cluster.
%%
%% This function will fail if:
%%
%%   * The node is currently the only disc node of its cluster
%%   * We can't connect to any of the nodes provided
%%   * The node is currently already clustered with the cluster of the nodes
%%     provided
%%
%% Note that we make no attempt to verify that the nodes provided are
%% all in the same cluster, we simply pick the first online node and
%% we cluster to its cluster.
join_cluster(DiscoveryNode, NodeType) ->
    case is_disc_and_clustered() andalso is_only_disc_node() of
        true -> e(clustering_only_disc_node);
        _    -> ok
    end,

    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),

    {ClusterNodes, _, _} = case discover_cluster(DiscoveryNode) of
                               {ok, Res}      -> Res;
                               E = {error, _} -> throw(E)
                           end,

    case me_in_nodes(ClusterNodes) of
        true  -> e(already_clustered);
        false -> ok
    end,

    %% reset the node. this simplifies things and it will be needed in
    %% this case - we're joining a new cluster with new nodes which
    %% are not in synch with the current node. I also lifts the burden
    %% of reseting the node from the user.
    reset(false),

    rabbit_misc:local_info_msg("Clustering with ~p~n", [ClusterNodes]),

    %% Join the cluster
    ok = init_db_with_mnesia(ClusterNodes, NodeType, false),

    rabbit_node_monitor:notify_joined_cluster(),

    ok.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages
reset()       -> reset(false).
force_reset() -> reset(true).

reset(Force) ->
    rabbit_misc:local_info_msg("Resetting Rabbit~s~n",
                               [if Force -> " forcefully";
                                   true  -> ""
                                end]),
    ensure_mnesia_not_running(),
    Node = node(),
    case Force of
        true ->
            disconnect_nodes(nodes());
        false ->
            AllNodes = all_clustered_nodes(),
            %% Reconnecting so that we will get an up to date nodes.
            %% We don't need to check for consistency because we are
            %% resetting.  Force=true here so that reset still works
            %% when clustered with a node which is down.
            init_db_with_mnesia(AllNodes, node_type(), false, true),
            case is_disc_and_clustered() andalso is_only_disc_node()
            of
                true  -> e(resetting_only_disc_node);
                false -> ok
            end,
            leave_cluster(),
            rabbit_misc:ensure_ok(mnesia:delete_schema([Node]),
                                  cannot_delete_schema),
            disconnect_nodes(all_clustered_nodes()),
            ok
    end,
    %% remove persisted messages and any other garbage we find
    ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

%% We need to make sure that we don't end up in a distributed Erlang
%% system with nodes while not being in an Mnesia cluster with
%% them. We don't handle that well.
disconnect_nodes(Nodes) -> [erlang:disconnect_node(N) || N <- Nodes].

change_cluster_node_type(Type) ->
    ensure_mnesia_dir(),
    ensure_mnesia_not_running(),
    case is_clustered() of
        false -> e(not_clustered);
        true  -> ok
    end,
    {_, _, RunningNodes} =
        case discover_cluster(all_clustered_nodes()) of
            {ok, Status}     -> Status;
            {error, _Reason} -> e(cannot_connect_to_cluster)
        end,
    Node = case ordsets:to_list(RunningNodes) of
               []        -> e(no_online_cluster_nodes);
               [Node0|_] -> Node0
           end,
    ok = reset(false),
    ok = join_cluster(Node, Type).

update_cluster_nodes(DiscoveryNode) ->
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),

    Status = {AllNodes, _, _} =
        case discover_cluster(DiscoveryNode) of
            {ok, Status0}    -> Status0;
            {error, _Reason} -> e(cannot_connect_to_node)
        end,
    case me_in_nodes(AllNodes) of
        true ->
            %% As in `check_consistency/0', we can safely delete the
            %% schema here, since it'll be replicated from the other
            %% nodes
            mnesia:delete_schema([node()]),
            rabbit_node_monitor:write_cluster_status(Status),
            init_db_with_mnesia(AllNodes, node_type(), false);
        false ->
            e(inconsistent_cluster)
    end,
    ok.

%% We proceed like this: try to remove the node locally. If the node
%% is offline, we remove the node if:
%%   * This node is a disc node
%%   * All other nodes are offline
%%   * This node was, at the best of our knowledge (see comment below)
%%     the last or second to last after the node we're removing to go
%%     down
forget_cluster_node(Node, RemoveWhenOffline) ->
    case ordsets:is_element(Node, all_clustered_nodes()) of
        true  -> ok;
        false -> e(not_a_cluster_node)
    end,
    case {mnesia:system_info(is_running), RemoveWhenOffline} of
        {yes, true} -> e(online_node_offline_flag);
        _           -> ok
    end,
    case remove_node_if_mnesia_running(Node) of
        ok ->
            ok;
        {error, mnesia_not_running} when RemoveWhenOffline ->
            remove_node_offline_node(Node);
        {error, mnesia_not_running} ->
            e(offline_node_no_offline_flag);
        Err = {error, _} ->
            throw(Err)
    end.

remove_node_offline_node(Node) ->
    case {empty_set(
            ordsets:del_element(Node, running_nodes(all_clustered_nodes()))),
          node_type()} of
        {true, disc} ->
            %% Note that while we check if the nodes was the last to
            %% go down, apart from the node we're removing from, this
            %% is still unsafe.  Consider the situation in which A and
            %% B are clustered. A goes down, and records B as the
            %% running node. Then B gets clustered with C, C goes down
            %% and B goes down. In this case, C is the second-to-last,
            %% but we don't know that and we'll remove B from A
            %% anyway, even if that will lead to bad things.
            case empty_set(ordsets:subtract(running_clustered_nodes(),
                                            ordsets:from_list([node(), Node])))
            of true -> start_mnesia(),
                       try
                           [mnesia:force_load_table(T) ||
                               T <- rabbit_mnesia:table_names()],
                           forget_cluster_node(Node, false),
                           ensure_mnesia_running()
                       after
                           stop_mnesia()
                       end;
                false -> e(not_last_node_to_go_down)
            end;
        {false, _} ->
            e(removing_node_from_offline_node)
    end.


%%----------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------

status() ->
    IfNonEmpty = fun (Type, Nodes) ->
                         case empty_set(Nodes) of
                             true  -> [];
                             false -> [{Type, ordsets:to_list(Nodes)}]
                         end
                 end,
    [{nodes, (IfNonEmpty(disc, clustered_disc_nodes()) ++
                  IfNonEmpty(ram, clustered_ram_nodes()))}] ++
        case mnesia:system_info(is_running) of
            yes -> [{running_nodes, running_clustered_nodes()}];
            no  -> []
        end.

is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

is_clustered() ->
    AllNodes = all_clustered_nodes(),
    not is_only_node(AllNodes) andalso not empty_set(AllNodes).

is_disc_and_clustered() -> node_type() =:= disc andalso is_clustered().

%% Functions that retrieve the nodes in the cluster will rely on the
%% status file if offline.

all_clustered_nodes() -> cluster_status(all).

clustered_disc_nodes() -> cluster_status(disc).

clustered_ram_nodes() -> ordsets:subtract(cluster_status(all),
                                          cluster_status(disc)).

running_clustered_nodes() -> cluster_status(running).

running_clustered_disc_nodes() ->
    {_, DiscNodes, RunningNodes} = cluster_status(),
    ordsets:intersection(DiscNodes, RunningNodes).

%% This function is the actual source of information, since it gets
%% the data from mnesia. Obviously it'll work only when mnesia is
%% running.
mnesia_nodes() ->
    case mnesia:system_info(is_running) of
        no ->
            {error, mnesia_not_running};
        yes ->
            %% If the tables are not present, it means that
            %% `init_db/3' hasn't been run yet. In other words, either
            %% we are a virgin node or a restarted RAM node. In both
            %% cases we're not interested in what mnesia has to say.
            NodeType = case mnesia:system_info(use_dir) of
                           true  -> disc;
                           false -> ram
                       end,
            Tables = mnesia:system_info(tables),
            {Table, _} = case table_definitions(NodeType) of [T|_] -> T end,
            case lists:member(Table, Tables) of
                true ->
                    AllNodes =
                        ordsets:from_list(mnesia:system_info(db_nodes)),
                    DiscCopies = ordsets:from_list(
                                   mnesia:table_info(schema, disc_copies)),
                    DiscNodes =
                        case NodeType of
                            disc -> nodes_incl_me(DiscCopies);
                            ram  -> DiscCopies
                        end,
                    {ok, {AllNodes, DiscNodes}};
                false ->
                    {error, tables_not_present}
            end
    end.

cluster_status(WhichNodes, ForceMnesia) ->
    %% I don't want to call `running_nodes/1' unless if necessary,
    %% since it can deadlock when stopping applications.
    Nodes = case mnesia_nodes() of
                {ok, {AllNodes, DiscNodes}} ->
                    {ok, {AllNodes, DiscNodes,
                          fun() -> running_nodes(AllNodes) end}};
                {error, _Reason} when not ForceMnesia ->
                    {AllNodes, DiscNodes, RunningNodes} =
                        rabbit_node_monitor:read_cluster_status(),
                    %% The cluster status file records the status when
                    %% the node is online, but we know for sure that
                    %% the node is offline now, so we can remove it
                    %% from the list of running nodes.
                    {ok, {AllNodes, DiscNodes,
                          fun() -> nodes_excl_me(RunningNodes) end}};
                Err = {error, _} ->
                    Err
            end,
    case Nodes of
        {ok, {AllNodes1, DiscNodes1, RunningNodesThunk}} ->
            {ok, case WhichNodes of
                     status  -> {AllNodes1, DiscNodes1, RunningNodesThunk()};
                     all     -> AllNodes1;
                     disc    -> DiscNodes1;
                     running -> RunningNodesThunk()
                 end};
        Err1 = {error, _} ->
            Err1
    end.

cluster_status(WhichNodes) ->
    {ok, Status} = cluster_status(WhichNodes, false),
    Status.

cluster_status() -> cluster_status(status).

cluster_status_from_mnesia() -> cluster_status(status, true).

node_info() ->
    {erlang:system_info(otp_release), rabbit_misc:version(),
     cluster_status_from_mnesia()}.

node_type() ->
    DiscNodes = clustered_disc_nodes(),
    case empty_set(DiscNodes) orelse me_in_nodes(DiscNodes) of
        true  -> disc;
        false -> ram
    end.

dir() -> mnesia:system_info(directory).

table_names() -> [Tab || {Tab, _} <- table_definitions()].

%%----------------------------------------------------------------------------
%% Operations on the db
%%----------------------------------------------------------------------------

%% Adds the provided nodes to the mnesia cluster, creating a new
%% schema if there is the need to and catching up if there are other
%% nodes in the cluster already. It also updates the cluster status
%% file.
init_db(ClusterNodes, NodeType, Force) ->
    Nodes = change_extra_db_nodes(ClusterNodes, Force),
    %% Note that we use `system_info' here and not the cluster status
    %% since when we start rabbit for the first time the cluster
    %% status will say we are a disc node but the tables won't be
    %% present yet.
    WasDiscNode = mnesia:system_info(use_dir),
    case {Nodes, WasDiscNode, NodeType} of
        {[], _, ram} ->
            %% Standalone ram node, we don't want that
            throw({error, cannot_create_standalone_ram_node});
        {[], false, disc} ->
            %% RAM -> disc, starting from scratch
            ok = create_schema();
        {[], true, disc} ->
            %% First disc node up
            ok;
        {[AnotherNode | _], _, _} ->
            %% Subsequent node in cluster, catch up
            ensure_version_ok(
              rpc:call(AnotherNode, rabbit_version, recorded, [])),
            ok = wait_for_replicated_tables(),
            %% The sequence in which we delete the schema and then the
            %% other tables is important: if we delete the schema
            %% first when moving to RAM mnesia will loudly complain
            %% since it doesn't make much sense to do that. But when
            %% moving to disc, we need to move the schema first.
            case NodeType of
                disc -> create_local_table_copy(schema, disc_copies),
                        create_local_table_copies(disc);
                ram  -> create_local_table_copies(ram),
                        create_local_table_copy(schema, ram_copies)
            end
    end,
    ensure_schema_integrity(),
    rabbit_node_monitor:update_cluster_status(),
    ok.

init_db_and_upgrade(ClusterNodes, NodeType, Force) ->
    ok = init_db(ClusterNodes, NodeType, Force),
    ok = case rabbit_upgrade:maybe_upgrade_local() of
             ok                    -> ok;
             starting_from_scratch -> rabbit_version:record_desired();
             version_not_available -> schema_ok_or_move()
         end,
    %% `maybe_upgrade_local' restarts mnesia, so ram nodes will forget
    %% about the cluster
    case NodeType of
        disc -> start_mnesia(),
                change_extra_db_nodes(ClusterNodes, true),
                wait_for_replicated_tables();
        ram  -> ok
    end,
    ok.

init_db_with_mnesia(ClusterNodes, NodeType, CheckConsistency, Force) ->
    start_mnesia(CheckConsistency),
    try
        init_db_and_upgrade(ClusterNodes, NodeType, Force)
    after
        stop_mnesia()
    end.

init_db_with_mnesia(ClusterNodes, NodeType, Force) ->
    init_db_with_mnesia(ClusterNodes, NodeType, true, Force).

ensure_mnesia_dir() ->
    MnesiaDir = dir() ++ "/",
    case filelib:ensure_dir(MnesiaDir) of
        {error, Reason} ->
            throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
        ok ->
            ok
    end.

ensure_mnesia_running() ->
    case mnesia:system_info(is_running) of
        yes ->
            ok;
        starting ->
            wait_for(mnesia_running),
            ensure_mnesia_running();
        Reason when Reason =:= no; Reason =:= stopping ->
            throw({error, mnesia_not_running})
    end.

ensure_mnesia_not_running() ->
    case mnesia:system_info(is_running) of
        no ->
            ok;
        stopping ->
            wait_for(mnesia_not_running),
            ensure_mnesia_not_running();
        Reason when Reason =:= yes; Reason =:= starting ->
            throw({error, mnesia_unexpectedly_running})
    end.

ensure_schema_integrity() ->
    case check_schema_integrity() of
        ok ->
            ok;
        {error, Reason} ->
            throw({error, {schema_integrity_check_failed, Reason}})
    end.

check_schema_integrity() ->
    Tables = mnesia:system_info(tables),
    case check_tables(fun (Tab, TabDef) ->
                              case lists:member(Tab, Tables) of
                                  false -> {error, {table_missing, Tab}};
                                  true  -> check_table_attributes(Tab, TabDef)
                              end
                      end) of
        ok     -> ok = wait_for_tables(),
                  check_tables(fun check_table_content/2);
        Other  -> Other
    end.

empty_ram_only_tables() ->
    Node = node(),
    lists:foreach(
      fun (TabName) ->
              case lists:member(Node, mnesia:table_info(TabName, ram_copies)) of
                  true  -> {atomic, ok} = mnesia:clear_table(TabName);
                  false -> ok
              end
      end, table_names()),
    ok.

create_tables() -> create_tables(disc).

create_tables(Type) ->
    lists:foreach(fun ({Tab, TabDef}) ->
                          TabDef1 = proplists:delete(match, TabDef),
                          case mnesia:create_table(Tab, TabDef1) of
                              {atomic, ok} -> ok;
                              {aborted, Reason} ->
                                  throw({error, {table_creation_failed,
                                                 Tab, TabDef1, Reason}})
                          end
                  end,
                  table_definitions(Type)),
    ok.

copy_db(Destination) ->
    ok = ensure_mnesia_not_running(),
    rabbit_file:recursive_copy(dir(), Destination).

wait_for_replicated_tables() -> wait_for_tables(replicated_table_names()).

wait_for_tables() -> wait_for_tables(table_names()).

wait_for_tables(TableNames) ->
    case mnesia:wait_for_tables(TableNames, 30000) of
        ok ->
            ok;
        {timeout, BadTabs} ->
            throw({error, {timeout_waiting_for_tables, BadTabs}});
        {error, Reason} ->
            throw({error, {failed_waiting_for_tables, Reason}})
    end.

%% This does not guarantee us much, but it avoids some situations that
%% will definitely end up badly
check_cluster_consistency() ->
    %% We want to find 0 or 1 consistent nodes.
    case lists:foldl(
           fun (Node,  {error, _})    -> check_cluster_consistency(Node);
               (_Node, {ok, Status})  -> {ok, Status}
           end, {error, not_found}, nodes_excl_me(all_clustered_nodes()))
    of
        {ok, Status = {RemoteAllNodes, _, _}} ->
            case ordsets:is_subset(all_clustered_nodes(), RemoteAllNodes) of
                true  ->
                    ok;
                false ->
                    %% We delete the schema here since we think we are
                    %% clustered with nodes that are no longer in the
                    %% cluster and there is no other way to remove
                    %% them from our schema. On the other hand, we are
                    %% sure that there is another online node that we
                    %% can use to sync the tables with. There is a
                    %% race here: if between this check and the
                    %% `init_db' invocation the cluster gets
                    %% disbanded, we're left with a node with no
                    %% mnesia data that will try to connect to offline
                    %% nodes.
                    mnesia:delete_schema([node()])
            end,
            rabbit_node_monitor:write_cluster_status(Status);
        {error, not_found} ->
            ok;
        E = {error, _} ->
            throw(E)
    end.

check_cluster_consistency(Node) ->
    case rpc:call(Node, rabbit_mnesia, node_info, []) of
        {badrpc, _Reason} ->
            {error, not_found};
        {_OTP, _Rabbit, {error, _}} ->
            {error, not_found};
        {OTP, Rabbit, {ok, Status}} ->
            case check_consistency(OTP, Rabbit, Node, Status) of
                E = {error, _} -> E;
                {ok, Res}      -> {ok, Res}
            end
    end.

%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------

on_node_up(Node) ->
    case is_only_node(Node, running_clustered_disc_nodes()) of
        true  -> rabbit_log:info("cluster contains disc nodes again~n");
        false -> ok
    end.

on_node_down(_Node) ->
    case empty_set(running_clustered_disc_nodes()) of
        true  -> rabbit_log:info("only running disc node went down~n");
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

discover_cluster(Nodes) when is_list(Nodes) ->
    lists:foldl(fun (_, {ok, Res})     -> {ok, Res};
                    (Node, {error, _}) -> discover_cluster(Node)
                end, {error, no_nodes_provided}, Nodes);
discover_cluster(Node) ->
    OfflineError =
        {error, {cannot_discover_cluster,
                 "The nodes provided is either offline or not running"}},
    case node() of
        Node->
            {error, {cannot_discover_cluster,
                     "You provided the current node as node to cluster with"}};
        _ ->
            case rpc:call(Node,
                          rabbit_mnesia, cluster_status_from_mnesia, []) of
                {badrpc, _Reason}           -> OfflineError;
                {error, mnesia_not_running} -> OfflineError;
                {ok, Res}                   -> {ok, Res}
            end
    end.

%% The tables aren't supposed to be on disk on a ram node
table_definitions(disc) ->
    table_definitions();
table_definitions(ram) ->
    [{Tab, copy_type_to_ram(TabDef)} || {Tab, TabDef} <- table_definitions()].

table_definitions() ->
    [{rabbit_user,
      [{record_name, internal_user},
       {attributes, record_info(fields, internal_user)},
       {disc_copies, [node()]},
       {match, #internal_user{_='_'}}]},
     {rabbit_user_permission,
      [{record_name, user_permission},
       {attributes, record_info(fields, user_permission)},
       {disc_copies, [node()]},
       {match, #user_permission{user_vhost = #user_vhost{_='_'},
                                permission = #permission{_='_'},
                                _='_'}}]},
     {rabbit_vhost,
      [{record_name, vhost},
       {attributes, record_info(fields, vhost)},
       {disc_copies, [node()]},
       {match, #vhost{_='_'}}]},
     {rabbit_listener,
      [{record_name, listener},
       {attributes, record_info(fields, listener)},
       {type, bag},
       {match, #listener{_='_'}}]},
     {rabbit_durable_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {disc_copies, [node()]},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_semi_durable_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {type, ordered_set},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {type, ordered_set},
       {match, #route{binding = binding_match(), _='_'}}]},
     {rabbit_reverse_route,
      [{record_name, reverse_route},
       {attributes, record_info(fields, reverse_route)},
       {type, ordered_set},
       {match, #reverse_route{reverse_binding = reverse_binding_match(),
                              _='_'}}]},
     {rabbit_topic_trie_node,
      [{record_name, topic_trie_node},
       {attributes, record_info(fields, topic_trie_node)},
       {type, ordered_set},
       {match, #topic_trie_node{trie_node = trie_node_match(), _='_'}}]},
     {rabbit_topic_trie_edge,
      [{record_name, topic_trie_edge},
       {attributes, record_info(fields, topic_trie_edge)},
       {type, ordered_set},
       {match, #topic_trie_edge{trie_edge = trie_edge_match(), _='_'}}]},
     {rabbit_topic_trie_binding,
      [{record_name, topic_trie_binding},
       {attributes, record_info(fields, topic_trie_binding)},
       {type, ordered_set},
       {match, #topic_trie_binding{trie_binding = trie_binding_match(),
                                   _='_'}}]},
     {rabbit_durable_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)},
       {disc_copies, [node()]},
       {match, #exchange{name = exchange_name_match(), _='_'}}]},
     {rabbit_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)},
       {match, #exchange{name = exchange_name_match(), _='_'}}]},
     {rabbit_exchange_serial,
      [{record_name, exchange_serial},
       {attributes, record_info(fields, exchange_serial)},
       {match, #exchange_serial{name = exchange_name_match(), _='_'}}]},
     {rabbit_runtime_parameters,
      [{record_name, runtime_parameters},
       {attributes, record_info(fields, runtime_parameters)},
       {disc_copies, [node()]},
       {match, #runtime_parameters{_='_'}}]},
     {rabbit_durable_queue,
      [{record_name, amqqueue},
       {attributes, record_info(fields, amqqueue)},
       {disc_copies, [node()]},
       {match, #amqqueue{name = queue_name_match(), _='_'}}]},
     {rabbit_queue,
      [{record_name, amqqueue},
       {attributes, record_info(fields, amqqueue)},
       {match, #amqqueue{name = queue_name_match(), _='_'}}]}]
        ++ gm:table_definitions()
        ++ mirrored_supervisor:table_definitions().

binding_match() ->
    #binding{source = exchange_name_match(),
             destination = binding_destination_match(),
             _='_'}.
reverse_binding_match() ->
    #reverse_binding{destination = binding_destination_match(),
                     source = exchange_name_match(),
                     _='_'}.
binding_destination_match() ->
    resource_match('_').
trie_node_match() ->
    #trie_node{   exchange_name = exchange_name_match(), _='_'}.
trie_edge_match() ->
    #trie_edge{   exchange_name = exchange_name_match(), _='_'}.
trie_binding_match() ->
    #trie_binding{exchange_name = exchange_name_match(), _='_'}.
exchange_name_match() ->
    resource_match(exchange).
queue_name_match() ->
    resource_match(queue).
resource_match(Kind) ->
    #resource{kind = Kind, _='_'}.

replicated_table_names() ->
    [Tab || {Tab, TabDef} <- table_definitions(),
            not lists:member({local_content, true}, TabDef)
    ].

check_table_attributes(Tab, TabDef) ->
    {_, ExpAttrs} = proplists:lookup(attributes, TabDef),
    case mnesia:table_info(Tab, attributes) of
        ExpAttrs -> ok;
        Attrs    -> {error, {table_attributes_mismatch, Tab, ExpAttrs, Attrs}}
    end.

check_table_content(Tab, TabDef) ->
    {_, Match} = proplists:lookup(match, TabDef),
    case mnesia:dirty_first(Tab) of
        '$end_of_table' ->
            ok;
        Key ->
            ObjList = mnesia:dirty_read(Tab, Key),
            MatchComp = ets:match_spec_compile([{Match, [], ['$_']}]),
            case ets:match_spec_run(ObjList, MatchComp) of
                ObjList -> ok;
                _       -> {error, {table_content_invalid, Tab, Match, ObjList}}
            end
    end.

check_tables(Fun) ->
    case [Error || {Tab, TabDef} <- table_definitions(node_type()),
                   case Fun(Tab, TabDef) of
                       ok             -> Error = none, false;
                       {error, Error} -> true
                   end] of
        []     -> ok;
        Errors -> {error, Errors}
    end.

schema_ok_or_move() ->
    case check_schema_integrity() of
        ok ->
            ok;
        {error, Reason} ->
            %% NB: we cannot use rabbit_log here since it may not have been
            %% started yet
            error_logger:warning_msg("schema integrity check failed: ~p~n"
                                     "moving database to backup location "
                                     "and recreating schema from scratch~n",
                                     [Reason]),
            ok = move_db(),
            ok = create_schema()
    end.

ensure_version_ok({ok, DiscVersion}) ->
    DesiredVersion = rabbit_version:desired(),
    case rabbit_version:matches(DesiredVersion, DiscVersion) of
        true  -> ok;
        false -> throw({error, {version_mismatch, DesiredVersion, DiscVersion}})
    end;
ensure_version_ok({error, _}) ->
    ok = rabbit_version:record_desired().

%% We only care about disc nodes since ram nodes are supposed to catch
%% up only
create_schema() ->
    stop_mnesia(),
    rabbit_misc:ensure_ok(mnesia:create_schema([node()]), cannot_create_schema),
    start_mnesia(),
    ok = create_tables(disc),
    ensure_schema_integrity(),
    ok = rabbit_version:record_desired().

move_db() ->
    stop_mnesia(),
    MnesiaDir = filename:dirname(dir() ++ "/"),
    {{Year, Month, Day}, {Hour, Minute, Second}} = erlang:universaltime(),
    BackupDir = rabbit_misc:format(
                  "~s_~w~2..0w~2..0w~2..0w~2..0w~2..0w",
                  [MnesiaDir, Year, Month, Day, Hour, Minute, Second]),
    case file:rename(MnesiaDir, BackupDir) of
        ok ->
            %% NB: we cannot use rabbit_log here since it may not have
            %% been started yet
            error_logger:warning_msg("moved database from ~s to ~s~n",
                                     [MnesiaDir, BackupDir]),
            ok;
        {error, Reason} -> throw({error, {cannot_backup_mnesia,
                                          MnesiaDir, BackupDir, Reason}})
    end,
    ensure_mnesia_dir(),
    start_mnesia(),
    ok.

copy_type_to_ram(TabDef) ->
    [{disc_copies, []}, {ram_copies, [node()]}
     | proplists:delete(ram_copies, proplists:delete(disc_copies, TabDef))].

table_has_copy_type(TabDef, DiscType) ->
    lists:member(node(), proplists:get_value(DiscType, TabDef, [])).

create_local_table_copies(Type) ->
    lists:foreach(
      fun ({Tab, TabDef}) ->
              HasDiscCopies     = table_has_copy_type(TabDef, disc_copies),
              HasDiscOnlyCopies = table_has_copy_type(TabDef, disc_only_copies),
              LocalTab          = proplists:get_bool(local_content, TabDef),
              StorageType =
                  if
                      Type =:= disc orelse LocalTab ->
                          if
                              HasDiscCopies     -> disc_copies;
                              HasDiscOnlyCopies -> disc_only_copies;
                              true              -> ram_copies
                          end;
                      Type =:= ram ->
                          ram_copies
                  end,
              ok = create_local_table_copy(Tab, StorageType)
      end,
      table_definitions(Type)),
    ok.

create_local_table_copy(Tab, Type) ->
    StorageType = mnesia:table_info(Tab, storage_type),
    {atomic, ok} =
        if
            StorageType == unknown ->
                mnesia:add_table_copy(Tab, node(), Type);
            StorageType /= Type ->
                mnesia:change_table_copy_type(Tab, node(), Type);
            true -> {atomic, ok}
        end,
    ok.

remove_node_if_mnesia_running(Node) ->
    case mnesia:system_info(is_running) of
        yes ->
            %% Deleting the the schema copy of the node will result in
            %% the node being removed from the cluster, with that
            %% change being propagated to all nodes
            case mnesia:del_table_copy(schema, Node) of
                {atomic, ok} ->
                    rabbit_node_monitor:notify_left_cluster(Node),
                    ok;
                {aborted, Reason} ->
                    {error, {failed_to_remove_node, Node, Reason}}
            end;
        no  ->
            {error, mnesia_not_running}
    end.

leave_cluster() ->
    RunningNodes = running_nodes(nodes_excl_me(all_clustered_nodes())),
    case not is_clustered() andalso empty_set(RunningNodes) of
        true ->
            ok;
        false ->
            case lists:any(fun leave_cluster/1,
                           ordsets:to_list(RunningNodes)) of
                true  -> ok;
                false -> e(no_running_cluster_nodes)
            end
    end.

leave_cluster(Node) ->
    case rpc:call(Node,
                  rabbit_mnesia, remove_node_if_mnesia_running, [node()]) of
        ok                          -> true;
        {error, mnesia_not_running} -> false;
        {error, Reason}             -> throw({error, Reason});
        {badrpc, nodedown}          -> false
    end.

wait_for(Condition) ->
    error_logger:info_msg("Waiting for ~p...~n", [Condition]),
    timer:sleep(1000).

start_mnesia(CheckConsistency) ->
    case CheckConsistency of
        true  -> check_cluster_consistency();
        false -> ok
    end,
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

start_mnesia() ->
    start_mnesia(true).

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

change_extra_db_nodes(ClusterNodes0, Force) ->
    ClusterNodes = ordsets:to_list(nodes_excl_me(ClusterNodes0)),
    case {mnesia:change_config(extra_db_nodes, ClusterNodes), ClusterNodes} of
        {{ok, []}, [_|_]} when not Force ->
            throw({error, {failed_to_cluster_with, ClusterNodes,
                           "Mnesia could not connect to any nodes."}});
        {{ok, Nodes}, _} ->
            Nodes
    end.

%% What we really want is nodes running rabbit, not running
%% mnesia. Using `mnesia:system_info(running_db_nodes)' will
%% return false positives when we are actually just doing cluster
%% operations (e.g. joining the cluster).
running_nodes(Nodes) ->
    {Replies, _BadNodes} = rpc:multicall(ordsets:to_list(Nodes), rabbit_mnesia,
                                         is_running_remote, []),
    ordsets:from_list([Node || {Running, Node} <- Replies, Running]).

is_running_remote() ->
    {proplists:is_defined(rabbit, application:which_applications(infinity)),
     node()}.

is_only_node(Node, Nodes) -> ordsets:to_list(Nodes) == [Node].

is_only_node(Nodes) -> is_only_node(node(), Nodes).

is_only_disc_node() -> is_only_node(clustered_disc_nodes()).

me_in_nodes(Nodes) -> ordsets:is_element(node(), Nodes).

nodes_incl_me(Nodes) -> ordsets:add_element(node(), Nodes).

nodes_excl_me(Nodes) -> ordsets:del_element(node(), Nodes).

empty_set(Set) -> ordsets:size(Set) =:= 0.

check_consistency(OTP, Rabbit) ->
    rabbit_misc:sequence_error(
      [check_otp_consistency(OTP), check_rabbit_consistency(Rabbit)]).

check_consistency(OTP, Rabbit, Node, Status) ->
    rabbit_misc:sequence_error(
      [check_otp_consistency(OTP),
       check_rabbit_consistency(Rabbit),
       check_nodes_consistency(Node, Status)]).

check_nodes_consistency(Node, RemoteStatus = {RemoteAllNodes, _, _}) ->
    ThisNode = node(),
    case ordsets:is_element(ThisNode, RemoteAllNodes) of
        true ->
            {ok, RemoteStatus};
        false ->
            {error, {inconsistent_cluster,
                     rabbit_misc:format("Node ~p thinks it's clustered "
                                        "with node ~p, but ~p disagrees",
                                        [ThisNode, Node, Node])}}
    end.

check_version_consistency(This, Remote, _) when This =:= Remote ->
    ok;
check_version_consistency(This, Remote, Name) ->
    {error, {inconsistent_cluster,
             rabbit_misc:format("~s version mismatch: local node is ~s, "
                                "remote node ~s", [Name, This, Remote])}}.

check_otp_consistency(Remote) ->
    check_version_consistency(erlang:system_info(otp_release), Remote, "OTP").

check_rabbit_consistency(Remote) ->
    check_version_consistency(rabbit_misc:version(), Remote, "Rabbit").

%% This is fairly tricky.  We want to know if the node is in the state
%% that a `reset' would leave it in.  We cannot simply check if the
%% mnesia tables aren't there because restarted RAM nodes won't have
%% tables while still being non-virgin.  What we do instead is to
%% check if the mnesia directory is non existant or empty, with the
%% exception of the cluster status file, which will be there thanks to
%% `rabbit_node_monitor:prepare_cluster_status_file/0'.
is_virgin_node() ->
    case rabbit_file:list_dir(dir()) of
        {error, enoent} -> true;
        {ok, []}        -> true;
        {ok, [File]}    -> (dir() ++ "/" ++ File) =:=
                               [rabbit_node_monitor:cluster_status_filename(),
                                rabbit_node_monitor:running_nodes_filename()];
        {ok, _}         -> false
    end.

find_good_node([]) ->
    none;
find_good_node([Node | Nodes]) ->
    case rpc:call(Node, rabbit_mnesia, node_info, []) of
        {badrpc, _Reason} -> find_good_node(Nodes);
        {OTP, Rabbit, _}  -> case check_consistency(OTP, Rabbit) of
                                 {error, _} -> find_good_node(Nodes);
                                 ok         -> {ok, Node}
                             end
    end.

e(Tag) -> throw({error, {Tag, error_description(Tag)}}).

error_description(clustering_only_disc_node) ->
    "You cannot cluster a node if it is the only disc node in its existing "
        " cluster. If new nodes joined while this node was offline, use "
        "\"update_cluster_nodes\" to add them manually.";
error_description(resetting_only_disc_node) ->
    "You cannot reset a node when it is the only disc node in a cluster. "
        "Please convert another node of the cluster to a disc node first.";
error_description(already_clustered) ->
    "You are already clustered with the nodes you have selected.";
error_description(not_clustered) ->
    "Non-clustered nodes can only be disc nodes.";
error_description(cannot_connect_to_cluster) ->
    "Could not connect to the cluster nodes present in this node's "
        "status file. If the cluster has changed, you can use the "
        "\"update_cluster_nodes\" command to point to the new cluster nodes.";
error_description(no_online_cluster_nodes) ->
    "Could not find any online cluster nodes. If the cluster has changed, "
        "you can use the 'recluster' command.";
error_description(cannot_connect_to_node) ->
    "Could not connect to the cluster node provided.";
error_description(inconsistent_cluster) ->
    "The nodes provided do not have this node as part of the cluster.";
error_description(not_a_cluster_node) ->
    "The node selected is not in the cluster.";
error_description(online_node_offline_flag) ->
    "You set the --offline flag, which is used to remove nodes remotely from "
        "offline nodes, but this node is online.";
error_description(offline_node_no_offline_flag) ->
    "You are trying to remove a node from an offline node. That is dangerous, "
        "but can be done with the --offline flag. Please consult the manual "
        "for rabbitmqctl for more information.";
error_description(not_last_node_to_go_down) ->
    "The node you're trying to remove from was not the last to go down "
        "(excluding the node you are removing). Please use the the last node "
        "to go down to remove nodes when the cluster is offline.";
error_description(removing_node_from_offline_node) ->
    "To remove a node remotely from an offline node, the node you're removing "
        "from must be a disc node and all the other nodes must be offline.";
error_description(no_running_cluster_nodes) ->
    "You cannot leave a cluster if no online nodes are present.".

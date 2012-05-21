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

-export([ensure_mnesia_dir/0, dir/0, status/0, init/0, is_db_empty/0,
         join_cluster/2, check_cluster_consistency/0, reset/0, force_reset/0,
         init_db/4, is_clustered/0, running_clustered_nodes/0,
         all_clustered_nodes/0, empty_ram_only_tables/0, copy_db/1,
         wait_for_tables/1, initialize_cluster_nodes_status/0,
         write_cluster_nodes_status/1, read_cluster_nodes_status/0,
         update_cluster_nodes_status/0, is_disc_node/0, on_node_down/1,
         on_node_up/1, should_be_disc_node/1, change_node_type/1,
         recluster/1]).

-export([table_names/0]).

%% Used internally in rpc calls, see `discover_nodes/1'
-export([cluster_status_if_running/0]).

%% create_tables/0 exported for helping embed RabbitMQ in or alongside
%% other mnesia-using Erlang applications, such as ejabberd
-export([create_tables/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([node_type/0, node_status/0]).

-type(node_type() :: disc_only | disc | ram | unknown).
-type(node_status() :: {[node()], [node()], [node()]}).

%% Main interface
-spec(init/0 :: () -> 'ok').
-spec(join_cluster/2 :: ([node()], boolean()) -> 'ok').
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').

%% Various queries to get the status of the db
-spec(status/0 :: () -> [{'nodes', [{node_type(), [node()]}]} |
                         {'running_nodes', [node()]}]).
-spec(is_db_empty/0 :: () -> boolean()).
-spec(is_clustered/0 :: () -> boolean()).
-spec(running_clustered_nodes/0 :: () -> [node()]).
-spec(all_clustered_nodes/0 :: () -> [node()]).
-spec(is_disc_node/0 :: () -> boolean()).
-spec(dir/0 :: () -> file:filename()).
-spec(table_names/0 :: () -> [atom()]).
-spec(cluster_status_if_running/0 :: () -> 'error' | node_status()).

%% Operations on the db and utils, mainly used in `rabbit_upgrade' and `rabbit'
-spec(init_db/4 :: ([node()], boolean(), boolean(), boolean()) -> 'ok').
-spec(ensure_mnesia_dir/0 :: () -> 'ok').
-spec(empty_ram_only_tables/0 :: () -> 'ok').
-spec(create_tables/0 :: () -> 'ok').
-spec(copy_db/1 :: (file:filename()) ->  rabbit_types:ok_or_error(any())).
-spec(wait_for_tables/1 :: ([atom()]) -> 'ok').
-spec(should_be_disc_node/1 :: ([node()]) -> boolean()).
-spec(check_cluster_consistency/0 :: () -> 'ok' | no_return()).

%% Functions to handle the cluster status file
-spec(write_cluster_nodes_status/1 :: (node_status()) ->  'ok').
-spec(read_cluster_nodes_status/0 :: () ->  node_status()).
-spec(initialize_cluster_nodes_status/0 :: () -> 'ok').
-spec(update_cluster_nodes_status/0 :: () -> 'ok').

%% Hooks used in `rabbit_node_monitor'
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    {DiscNodes, WantDiscNode} = read_cluster_nodes_config(),
    ok = init_db(DiscNodes, WantDiscNode, WantDiscNode),
    %% We intuitively expect the global name server to be synced when
    %% Mnesia is up. In fact that's not guaranteed to be the case - let's
    %% make it so.
    ok = global:sync(),
    ok.

%% Make the node join a cluster. The node will be reset automatically before we
%% actually cluster it. The nodes provided will be used to find out about the
%% nodes in the cluster.
%% This function will fail if:
%%
%%   * The node is currently the only disc node of its cluster
%%   * We can't connect to any of the nodes provided
%%   * The node is currently already clustered with the cluster of the nodes
%%     provided
%%
%% Note that we make no attempt to verify that the nodes provided are all in the
%% same cluster, we simply pick the first online node and we cluster to its
%% cluster.
join_cluster(DiscoveryNode, WantDiscNode) ->
    case is_disc_and_clustered() andalso is_only_disc_node(node()) of
        true -> throw({error,
                       {standalone_ram_node,
                        "You can't cluster a node if it's the only "
                        "disc node in its existing cluster. If new nodes "
                        "joined while this node was offline, use \"recluster\" "
                        "to add them manually"}});
        _    -> ok
    end,

    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),

    Status = {ClusterNodes, DiscNodes, _} =
        case discover_cluster(DiscoveryNode) of
            {ok, Res}       -> Res;
            {error, Reason} -> throw({error, Reason})
        end,

    case lists:member(node(), ClusterNodes) of
        true  -> throw({error, {already_clustered,
                                "You are already clustered with the nodes you "
                                "have selected"}}),
                 write_cluster_nodes_status(Status);
        false -> ok
    end,

    %% reset the node. this simplifies things and it will be needed in this case
    %% - we're joining a new cluster with new nodes which are not in synch with
    %% the current node. I also lifts the burden of reseting the node from the
    %% user.
    reset(false),

    rabbit_misc:local_info_msg("Clustering with ~p~s~n", [ClusterNodes]),

    %% Join the cluster
    ok = start_and_init_db(DiscNodes, WantDiscNode, false),

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
    case not Force andalso is_disc_and_clustered() andalso
         is_only_disc_node(node())
    of
        true  -> throw({error, {standalone_ram_node,
                                "You can't reset a node if it's the only disc "
                                "node in a cluster. Please convert another node"
                                " of the cluster to a disc node first."}});
        false -> ok
    end,
    Node = node(),
    Nodes = all_clustered_nodes(),
    case Force of
        true ->
            ok;
        false ->
            ensure_mnesia_dir(),
            start_mnesia(),
            %% Reconnecting so that we will get an up to date RunningNodes
            RunningNodes =
                try
                    %% Force=true here so that reset still works when clustered
                    %% with a node which is down
                    {_, DiscNodes, _} = read_cluster_nodes_status(),
                    ok = init_db(DiscNodes, should_be_disc_node(DiscNodes),
                                 true),
                    running_clustered_nodes()
                after
                    stop_mnesia()
                end,
            leave_cluster(Nodes, RunningNodes),
            rabbit_misc:ensure_ok(mnesia:delete_schema([Node]),
                                  cannot_delete_schema)
    end,
    %% We need to make sure that we don't end up in a distributed Erlang system
    %% with nodes while not being in an Mnesia cluster with them. We don't
    %% handle that well.
    [erlang:disconnect_node(N) || N <- Nodes],
    ok = reset_cluster_nodes_status(),
    %% remove persisted messages and any other garbage we find
    ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
    ok.

change_node_type(Type) ->
    ensure_mnesia_dir(),
    ensure_mnesia_not_running(),
    case is_clustered() of
        false -> throw({error, {not_clustered,
                                "Non-clustered nodes can only be disc nodes"}});
        true  -> ok
    end,

    check_cluster_consistency(),
    DiscoveryNodes = all_clustered_nodes(),
    ClusterNodes =
        case discover_cluster(DiscoveryNodes) of
            {ok, {ClusterNodes0, _, _}} ->
                ClusterNodes0;
            {error, _Reason} ->
                throw({error,
                       {cannot_connect_to_cluster,
                        "Could not connect to the cluster nodes present in "
                        "this node status file. If the cluster has changed, "
                        "you can use the \"recluster\" command to point to the "
                        "new cluster nodes"}})
    end,

    WantDiscNode = case Type of
                       ram  -> false;
                       disc -> true
                   end,

    ok = start_and_init_db(ClusterNodes, WantDiscNode, false),

    ok.

recluster(DiscoveryNode) ->
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),

    ClusterNodes =
        case discover_cluster(DiscoveryNode) of
            {ok, {ClusterNodes0, _, _}} ->
                ClusterNodes0;
            {error, _Reason} ->
                throw({error,
                       {cannot_connect_to_node,
                        "Could not connect to the cluster node provided"}})
        end,

    case lists:member(node(), ClusterNodes) of
        true  -> start_and_init_db(ClusterNodes, is_disc_node(), false);
        false -> throw({error,
                        {inconsistent_cluster,
                         "The nodes provided do not have this node as part of "
                         "the cluster"}})
    end,

    ok.

%%----------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------

status() ->
    [{nodes, case mnesia:system_info(is_running) of
                 yes -> [{Key, Nodes} ||
                            {Key, CopyType} <- [{disc_only, disc_only_copies},
                                                {disc,      disc_copies},
                                                {ram,       ram_copies}],
                            begin
                                Nodes = nodes_of_type(CopyType),
                                Nodes =/= []
                            end];
                 no -> [{unknown, all_clustered_nodes()}];
                 Reason when Reason =:= starting; Reason =:= stopping ->
                     exit({rabbit_busy, try_again_later})
             end},
     {running_nodes, running_clustered_nodes()}].


is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

is_clustered() ->
    Nodes = all_clustered_nodes(),
    [node()] /= Nodes andalso [] /= Nodes.

is_disc_and_clustered() ->
    is_disc_node() andalso is_clustered().

%% The situation with functions that retrieve the nodes in the cluster is
%% messy.
%%
%%   * If we want to get all the nodes or the running nodes, we can do that
%%     while mnesia is offline *if* the node is a disc node. If the node is ram,
%%     the result will always be [node()].
%%   * If we want to get the cluster disc nodes (running or not), we need to
%%     start mnesia in any case.
%%
%% In the following functions we try to get the data from mnesia when we can,
%% otherwise we fall back to the cluster status file.

check_mnesia_running(Fun) ->
    case mnesia:system_info(is_running) of
        yes -> {ok, Fun()};
        no  -> error
    end.

check_disc_or_mnesia_running(Fun) ->
    case is_disc_node() of
        true  -> {ok, Fun()};
        false -> case check_mnesia_running(Fun) of
                     {ok, Res} -> {ok, Res};
                     error     -> error
                 end
    end.

check_or_cluster_status(Fun, Check) ->
    case Check(Fun) of
        {ok, Res} -> {ok, Res};
        error     -> {status, read_cluster_nodes_status()}
    end.

all_clustered_nodes() ->
    case check_or_cluster_status(
           fun () -> mnesia:system_info(db_nodes) end,
           fun check_disc_or_mnesia_running/1)
    of
        {ok, Nodes}             -> Nodes;
        {status, {Nodes, _, _}} -> Nodes
    end.

all_clustered_disc_nodes() ->
    case check_or_cluster_status(
           fun () -> nodes_of_type(disc_copies) end,
           fun check_mnesia_running/1)
    of
        {ok, Nodes}             -> Nodes;
        {status, {_, Nodes, _}} -> Nodes
    end.

running_clustered_nodes() ->
    case check_or_cluster_status(
           fun () -> mnesia:system_info(running_db_nodes) end,
           fun check_disc_or_mnesia_running/1)
    of
        {ok, Nodes}             -> Nodes;
        {status, {_, _, Nodes}} -> Nodes
    end.

running_clustered_disc_nodes() ->
    {DiscNodes, RunningNodes} =
        case check_or_cluster_status(
               fun () ->
                       {all_clustered_disc_nodes(), running_clustered_nodes()}
               end,
               fun check_mnesia_running/1)
        of
            {ok, Nodes} ->
                Nodes;
            {status, {_, DiscNodes0, RunningNodes0}} ->
                {DiscNodes0, RunningNodes0}
        end,
    sets:to_list(sets:intersection(sets:from_list(DiscNodes),
                                   sets:from_list(RunningNodes))).

%% This function is a bit different, we want it to return correctly only when
%% the node is actually online. This is because when we discover the nodes we
%% want online, "working" nodes only.
cluster_status_if_running() ->
    check_mnesia_running(
      fun () ->
              {mnesia:system_info(db_nodes), nodes_of_type(disc_copies),
               mnesia:system_info(running_db_nodes)}
      end).

is_disc_node() -> mnesia:system_info(use_dir).

dir() -> mnesia:system_info(directory).

table_names() ->
    [Tab || {Tab, _} <- table_definitions()].

%%----------------------------------------------------------------------------
%% Operations on the db
%%----------------------------------------------------------------------------

init_db(ClusterNodes, WantDiscNode, Force) ->
    init_db(ClusterNodes, WantDiscNode, Force, true).

%% Take a cluster node config and create the right kind of node - a
%% standalone disk node, or disk or ram node connected to the
%% specified cluster nodes.  If Force is false, don't allow
%% connections to offline nodes.
init_db(ClusterNodes, WantDiscNode, Force, Upgrade) ->
    UClusterNodes = lists:usort(ClusterNodes),
    ProperClusterNodes = UClusterNodes -- [node()],
    case mnesia:change_config(extra_db_nodes, ProperClusterNodes) of
        {ok, []} when not Force andalso ProperClusterNodes =/= [] ->
            throw({error, {failed_to_cluster_with, ProperClusterNodes,
                           "Mnesia could not connect to any disc nodes."}});
        {ok, Nodes} ->
            WasDiscNode = is_disc_node(),
            %% We create a new db (on disk, or in ram) in the first
            %% two cases and attempt to upgrade the in the other two
            case {Nodes, WasDiscNode, WantDiscNode} of
                {[], _, false} ->
                    %% New ram node; start from scratch
                    ok = create_schema(ram);
                {[], false, true} ->
                    %% Nothing there at all, start from scratch
                    ok = create_schema(disc);
                {[], true, true} ->
                    %% We're the first node up
                    case rabbit_upgrade:maybe_upgrade_local() of
                        ok                    -> ensure_schema_integrity();
                        version_not_available -> ok = schema_ok_or_move()
                    end;
                {[AnotherNode|_], _, _} ->
                    %% Subsequent node in cluster, catch up
                    ensure_version_ok(
                      rpc:call(AnotherNode, rabbit_version, recorded, [])),
                    ok = wait_for_replicated_tables(),

                    %% The sequence in which we delete the schema and then the
                    %% other tables is important: if we delete the schema first
                    %% when moving to RAM mnesia will loudly complain since it
                    %% doesn't make much sense to do that. But when moving to
                    %% disc, we need to move the schema first.
                    case WantDiscNode of
                        true  -> create_local_table_copy(schema, disc_copies),
                                 create_local_table_copies(disc);
                        false -> create_local_table_copies(ram),
                                 create_local_table_copy(schema, ram_copies)
                    end,

                    %% Write the status now that mnesia is running and clustered
                    update_cluster_nodes_status(),

                    ok = case Upgrade of
                             true ->
                                 case rabbit_upgrade:maybe_upgrade_local() of
                                     ok ->
                                         ok;
                                     %% If we're just starting up a new node we
                                     %% won't have a version
                                     starting_from_scratch ->
                                         rabbit_version:record_desired()
                                 end;
                             false ->
                                 ok
                         end,

                    %% We've taken down mnesia, so ram nodes will need
                    %% to re-sync
                    case is_disc_node() of
                        false -> start_mnesia(),
                                 mnesia:change_config(extra_db_nodes,
                                                      ProperClusterNodes),
                                 wait_for_replicated_tables();
                        true  -> ok
                    end,

                    ensure_schema_integrity(),
                    ok
            end;
        {error, Reason} ->
            %% one reason we may end up here is if we try to join
            %% nodes together that are currently running standalone or
            %% are members of a different cluster
            throw({error, {unable_to_join_cluster, ClusterNodes, Reason}})
    end.

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

should_be_disc_node(DiscNodes) ->
    DiscNodes == [] orelse lists:member(node(), DiscNodes).

%% This does not guarantee us much, but it avoids some situations that will
%% definitely end in disaster (a node starting and trying to merge its schema
%% to another node which is not clustered with it).
check_cluster_consistency() ->
    ThisNode = node(),
    lists:foreach(
      fun(Node) ->
              case rpc:call(Node, rabbit_mnesia, cluster_status_if_running, [])
              of
                  {badrpc, _Reason} -> ok;
                  error -> ok;
                  {ok, {AllNodes, _, _}}  ->
                      case lists:member(ThisNode, AllNodes) of
                          true ->
                              ok;
                          false ->
                              throw({error,
                                     {inconsistent_cluster,
                                      rabbit_misc:format(
                                        "Node ~p thinks it's clustered with "
                                        "node ~p, but ~p disagrees",
                                        [ThisNode, Node, Node])}})
                      end
              end
      end, all_clustered_nodes()).

%%----------------------------------------------------------------------------
%% Cluster status file functions
%%----------------------------------------------------------------------------

%% The cluster node status file contains all we need to know about the cluster:
%%
%%   * All the clustered nodes
%%   * The disc nodes
%%   * The running nodes.
%%
%% If the current node is a disc node it will be included in the disc nodes
%% list.
%%
%% We strive to keep the file up to date and we rely on this assumption in
%% various situations. Obviously when mnesia is offline the information we have
%% will be outdated, but it can't be otherwise.

cluster_nodes_status_filename() ->
    dir() ++ "/cluster_nodes.config".

%% Creates a status file with the default data (one disc node), only if an
%% existing cluster does not exist.
initialize_cluster_nodes_status() ->
    try read_cluster_nodes_status() of
        _ -> ok
    catch
        throw:{error, {cannot_read_cluster_nodes_status, _, enoent}} ->
            write_cluster_nodes_status({[node()], [node()], [node()]})
    end.

write_cluster_nodes_status(Status) ->
    FileName = cluster_nodes_status_filename(),
    case rabbit_file:write_term_file(FileName, [Status]) of
        ok -> ok;
        {error, Reason} ->
            throw({error, {cannot_write_cluster_nodes_status,
                           FileName, Reason}})
    end.

read_cluster_nodes_status() ->
    FileName = cluster_nodes_status_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [{_, _, _} = Status]} -> Status;
        {error, Reason} ->
            throw({error, {cannot_read_cluster_nodes_status, FileName, Reason}})
    end.

reset_cluster_nodes_status() ->
    FileName = cluster_nodes_status_filename(),
    case file:delete(FileName) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} ->
            throw({error, {cannot_delete_cluster_nodes_status,
                           FileName, Reason}})
    end,
    write_cluster_nodes_status({[node()], [node()], [node()]}).

%% To update the cluster status when mnesia is running.
update_cluster_nodes_status() ->
    {ok, Status} = cluster_status_if_running(),
    write_cluster_nodes_status(Status).

%% The cluster config contains the nodes that the node should try to contact to
%% form a cluster, and whether the node should be a disc node. When starting the
%% database, if the nodes in the cluster status are the initial ones, we try to
%% read the cluster config.
read_cluster_nodes_config() ->
    {AllNodes, DiscNodes, _} = read_cluster_nodes_status(),
    Node = node(),
    case AllNodes of
        [Node] -> {ok, Config} = application:get_env(rabbit, cluster_nodes),
                  Config;
        _      -> {AllNodes, should_be_disc_node(DiscNodes)}
    end.

%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------

on_node_up(Node) ->
    update_cluster_nodes_status(),
    case is_only_disc_node(Node) of
        true  -> rabbit_log:info("cluster contains disc nodes again~n");
        false -> ok
    end.

on_node_down(Node) ->
    case is_only_disc_node(Node) of
        true  -> rabbit_log:info("only running disc node went down~n");
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

discover_cluster(Nodes) when is_list(Nodes) ->
    lists:foldl(fun (_, {ok, Res})     -> {ok, Res};
                    (Node, {error, _}) -> discover_cluster(Node)
                end,
                {error, {cannot_discover_cluster,
                         "The nodes provided is either offline or not running"}},
                Nodes);
discover_cluster(Node) ->
    case Node =:= node() of
        true ->
            {error, {cannot_discover_cluster,
                     "You provided the current node as node to cluster with"}};
        false ->
            case rpc:call(Node, rabbit_mnesia, cluster_status_if_running, []) of
                {badrpc, _Reason} -> discover_cluster([]);
                error             -> discover_cluster([]);
                {ok, Res}         -> {ok, Res}
            end
    end.

nodes_of_type(Type) ->
    %% This function should return the nodes of a certain type (ram,
    %% disc or disc_only) in the current cluster.  The type of nodes
    %% is determined when the cluster is initially configured.
    mnesia:table_info(schema, Type).

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
    case [Error || {Tab, TabDef} <- table_definitions(
                                      case is_disc_node() of
                                          true  -> disc;
                                          false -> ram
                                      end),
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
            ok = create_schema(disc)
    end.

ensure_version_ok({ok, DiscVersion}) ->
    DesiredVersion = rabbit_version:desired(),
    case rabbit_version:matches(DesiredVersion, DiscVersion) of
        true  -> ok;
        false -> throw({error, {version_mismatch, DesiredVersion, DiscVersion}})
    end;
ensure_version_ok({error, _}) ->
    ok = rabbit_version:record_desired().

create_schema(Type) ->
    stop_mnesia(),
    case Type of
        disc -> rabbit_misc:ensure_ok(mnesia:create_schema([node()]),
                                      cannot_create_schema);
        ram  -> %% remove the disc schema since this is a ram node
                rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                                      cannot_delete_schema)
    end,
    start_mnesia(),
    ok = create_tables(Type),
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
%%% unused code - commented out to keep dialyzer happy
%%%                      Type =:= disc_only ->
%%%                          if
%%%                              HasDiscCopies or HasDiscOnlyCopies ->
%%%                                  disc_only_copies;
%%%                              true -> ram_copies
%%%                          end;
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

leave_cluster([], _) -> ok;
leave_cluster(Nodes, RunningNodes0) ->
    case RunningNodes0 -- [node()] of
        [] -> ok;
        RunningNodes ->
            %% find at least one running cluster node and instruct it to remove
            %% our schema copy which will in turn result in our node being
            %% removed as a cluster node from the schema, with that change being
            %% propagated to all nodes
            case lists:any(
                   fun (Node) ->
                           case rpc:call(Node, mnesia, del_table_copy,
                                         [schema, node()]) of
                               {atomic, ok} -> true;
                               {badrpc, nodedown} -> false;
                               {aborted, {node_not_running, _}} -> false;
                               {aborted, Reason} ->
                                   throw({error, {failed_to_leave_cluster,
                                                  Nodes, RunningNodes, Reason}})
                           end
                   end,
                   RunningNodes) of
                true -> ok;
                false -> throw({error, {no_running_cluster_nodes,
                                        Nodes, RunningNodes}})
            end
    end.

wait_for(Condition) ->
    error_logger:info_msg("Waiting for ~p...~n", [Condition]),
    timer:sleep(1000).

is_only_disc_node(Node) ->
    Nodes = running_clustered_disc_nodes(),
    [Node] =:= Nodes.

start_mnesia() ->
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

start_and_init_db(ClusterNodes, WantDiscNode, Force) ->
    mnesia:start(),
    try
        ok = init_db(ClusterNodes, WantDiscNode, Force)
    after
        mnesia:stop()
    end,
    ok.

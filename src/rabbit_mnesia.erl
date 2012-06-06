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

-export([prepare/0,
         init/0,
         join_cluster/2,
         reset/0,
         force_reset/0,
         recluster/1,
         change_node_type/1,
         remove_node/1,

         status/0,
         is_db_empty/0,
         is_clustered/0,
         all_clustered_nodes/0,
         all_clustered_disc_nodes/0,
         running_clustered_nodes/0,
         is_disc_node/0,
         dir/0,
         table_names/0,
         wait_for_tables/1,

         init_db/3,
         empty_ram_only_tables/0,
         copy_db/1,
         wait_for_tables/0,
         update_cluster_nodes_status/0,

         on_node_up/2,
         on_node_down/1
        ]).

%% Used internally in rpc calls
-export([cluster_status_if_running/0,
         node_info/0,
         remove_node_if_mnesia_running/1
        ]).

%% create_tables/0 exported for helping embed RabbitMQ in or alongside
%% other mnesia-using Erlang applications, such as ejabberd
-export([create_tables/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([node_type/0]).

-type(node_type() :: disc | ram).
-type(node_status() :: {[ordsets:ordset(node())], [ordsets:ordset(node())],
                        [ordsets:ordset(node())]}).

%% Main interface
-spec(prepare/0 :: () -> 'ok').
-spec(init/0 :: () -> 'ok').
-spec(join_cluster/2 :: ([node()], boolean()) -> 'ok').
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').
-spec(recluster/1 :: (node()) -> 'ok').
-spec(change_node_type/1 :: (node_type()) -> 'ok').
-spec(remove_node/1 :: (node()) -> 'ok').

%% Various queries to get the status of the db
-spec(status/0 :: () -> [{'nodes', [{node_type(), [node()]}]} |
                         {'running_nodes', [node()]}]).
-spec(is_db_empty/0 :: () -> boolean()).
-spec(is_clustered/0 :: () -> boolean()).
-spec(all_clustered_nodes/0 :: () -> [node()]).
-spec(all_clustered_disc_nodes/0 :: () -> [node()]).
-spec(running_clustered_nodes/0 :: () -> [node()]).
-spec(is_disc_node/0 :: () -> boolean()).
-spec(dir/0 :: () -> file:filename()).
-spec(table_names/0 :: () -> [atom()]).

%% Operations on the db and utils, mainly used in `rabbit_upgrade' and `rabbit'
-spec(init_db/3 :: ([node()], boolean(), boolean()) -> 'ok').
-spec(empty_ram_only_tables/0 :: () -> 'ok').
-spec(create_tables/0 :: () -> 'ok').
-spec(copy_db/1 :: (file:filename()) ->  rabbit_types:ok_or_error(any())).
-spec(wait_for_tables/1 :: ([atom()]) -> 'ok').
-spec(update_cluster_nodes_status/0 :: () -> 'ok').

%% Hooks used in `rabbit_node_monitor'
-spec(on_node_up/2 :: (node(), boolean()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

%% Functions used in internal rpc calls
-spec(cluster_status_if_running/0 :: () -> {'ok', node_status()} | 'error').
-spec(node_info/0 :: () -> {string(), string(),
                            ({'ok', node_status()} | 'error')}).
-spec(remove_node_if_mnesia_running/1 :: (node()) -> 'ok' |
                                                     {'error', term()}).

-endif.

%%----------------------------------------------------------------------------
%% Main interface
%%----------------------------------------------------------------------------

%% Sets up the cluster status file when needed, taking care of the legacy
%% files
prepare() ->
    ensure_mnesia_dir(),
    NotPresent =
        fun (AllNodes0, WantDiscNode) ->
            ThisNode = [node()],

            RunningNodes0 = legacy_read_previously_running_nodes(),
            legacy_delete_previously_running_nodes(),

            RunningNodes = lists:usort(RunningNodes0 ++ ThisNode),
            AllNodes =
                lists:usort(AllNodes0 ++ RunningNodes),
            DiscNodes = case WantDiscNode of
                            true  -> ThisNode;
                            false -> []
                        end,

            ok = write_cluster_nodes_status({AllNodes, DiscNodes, RunningNodes})
        end,
    case try_read_cluster_nodes_status() of
        {ok, _} ->
            %% We check the consistency only when the cluster status exists,
            %% since when it doesn't exist it means that we just started a fresh
            %% node, and when we have a legacy node with an old
            %% "cluster_nodes.config" we can't check the consistency anyway
            check_cluster_consistency(),
            ok;
        {error, {invalid_term, _, [AllNodes]}} ->
            %% Legacy file
            NotPresent(AllNodes, should_be_disc_node(AllNodes));
        {error, {cannot_read_file, _, enoent}} ->
            {ok, {AllNodes, WantDiscNode}} =
                application:get_env(rabbit, cluster_nodes),
            NotPresent(AllNodes, WantDiscNode)
    end.

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    ok = reinit_db(should_be_disc_node(all_clustered_disc_nodes())),
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

    {ClusterNodes, DiscNodes, _} = case discover_cluster(DiscoveryNode) of
                                       {ok, Res}       -> Res;
                                       {error, Reason} -> throw({error, Reason})
                                   end,

    case lists:member(node(), ClusterNodes) of
        true  -> throw({error, {already_clustered,
                                "You are already clustered with the nodes you "
                                "have selected"}});
        false -> ok
    end,

    %% reset the node. this simplifies things and it will be needed in this case
    %% - we're joining a new cluster with new nodes which are not in synch with
    %% the current node. I also lifts the burden of reseting the node from the
    %% user.
    reset(false),

    rabbit_misc:local_info_msg("Clustering with ~p~s~n", [ClusterNodes]),

    %% Join the cluster
    ok = init_db_and_upgrade(DiscNodes, WantDiscNode, false),
    stop_mnesia(),

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
    case Force of
        true ->
            ok;
        false ->
            %% Reconnecting so that we will get an up to date nodes
            try
                %% Force=true here so that reset still works when
                %% clustered with a node which is down
                ok = reinit_db(true)
            after
                stop_mnesia()
            end,
            leave_cluster(),
            rabbit_misc:ensure_ok(mnesia:delete_schema([Node]),
                                  cannot_delete_schema)
    end,
    %% We need to make sure that we don't end up in a distributed Erlang system
    %% with nodes while not being in an Mnesia cluster with them. We don't
    %% handle that well.
    [erlang:disconnect_node(N) || N <- all_clustered_nodes()],
    %% remove persisted messages and any other garbage we find
    ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
    ok = write_cluster_nodes_status(initial_cluster_status()),
    ok.

change_node_type(Type) ->
    ensure_mnesia_dir(),
    ensure_mnesia_not_running(),
    case is_clustered() of
        false -> throw({error, {not_clustered,
                                "Non-clustered nodes can only be disc nodes"}});
        true  -> ok
    end,

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

    ok = init_db_and_upgrade(ClusterNodes, WantDiscNode, false),
    stop_mnesia(),

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
        true  -> init_db_and_upgrade(ClusterNodes, is_disc_node(), false);
        false -> throw({error,
                        {inconsistent_cluster,
                         "The nodes provided do not have this node as part of "
                         "the cluster"}})
    end,
    stop_mnesia(),

    ok.

%% We proceed like this: try to remove the node locally. If mnesia is offline
%% then we try to remove it remotely on some other node. If there are no other
%% nodes running, then *if the current node is a disk node* we force-load mnesia
%% and remove the node.
remove_node(Node) ->
    case remove_node_if_mnesia_running(Node) of
        ok ->
            ok;
        {error, mnesia_not_running} ->
            case remove_node_remotely(Node) of
                ok ->
                    ok;
                {error, no_running_cluster_nodes} ->
                    case is_disc_node() of
                        false ->
                            throw({error,
                                   {removing_node_from_ram_node,
                                    "There are no nodes running and this is a "
                                    "RAM node"}});
                        true ->
                            start_mnesia(),
                            try
                                [mnesia:force_load_table(T) ||
                                    T <- rabbit_mnesia:table_names()],
                                remove_node(Node),
                                ensure_mnesia_running()
                            after
                                stop_mnesia()
                            end
                    end
            end;
        {error, Reason} ->
            throw({error, Reason})
    end.

%%----------------------------------------------------------------------------
%% Queries
%%----------------------------------------------------------------------------

status() ->
    IfNonEmpty = fun (_, [])       -> [];
                     (Type, Nodes) -> [{Type, Nodes}]
                 end,
    [{nodes, (IfNonEmpty(disc, all_clustered_disc_nodes()) ++
                  IfNonEmpty(ram, all_clustered_ram_nodes()))},
     {running_nodes, running_clustered_nodes()}].

is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

is_clustered() ->
    Nodes = all_clustered_nodes(),
    [node()] /= Nodes andalso [] /= Nodes.

is_disc_and_clustered() ->
    is_disc_node() andalso is_clustered().

%% Functions that retrieve the nodes in the cluster completely rely on the
%% cluster status file. For obvious reason, if rabbit is down, they might return
%% out of date information.

all_clustered_nodes() ->
    {AllNodes, _, _} = read_cluster_nodes_status(),
    AllNodes.

all_clustered_disc_nodes() ->
    {_, DiscNodes, _} = read_cluster_nodes_status(),
    DiscNodes.

all_clustered_ram_nodes() ->
    {AllNodes, DiscNodes, _} = read_cluster_nodes_status(),
    sets:to_list(sets:subtract(sets:from_list(AllNodes),
                               sets:from_list(DiscNodes))).

running_clustered_nodes() ->
    {_, _, RunningNodes} = read_cluster_nodes_status(),
    RunningNodes.

running_clustered_disc_nodes() ->
    {_, DiscNodes, RunningNodes} = read_cluster_nodes_status(),
    sets:to_list(sets:intersection(sets:from_list(DiscNodes),
                                   sets:from_list(RunningNodes))).

%% This function is a bit different, we want it to return correctly only when
%% the node is actually online. This is because when we discover the nodes we
%% want online, "working" nodes only.
cluster_status_if_running() ->
    case mnesia:system_info(is_running) of
        no  -> error;
        yes -> {ok, {ordsets:from_list(mnesia:system_info(db_nodes)),
                     ordsets:from_list(mnesia:table_info(schema, disc_copies)),
                     ordsets:from_list(mnesia:system_info(running_db_nodes))}}
    end.

node_info() ->
    {erlang:system_info(otp_release), rabbit_misc:rabbit_version(),
     cluster_status_if_running()}.

is_disc_node() -> mnesia:system_info(use_dir).

dir() -> mnesia:system_info(directory).

table_names() ->
    [Tab || {Tab, _} <- table_definitions()].

%%----------------------------------------------------------------------------
%% Operations on the db
%%----------------------------------------------------------------------------

%% Starts mnesia if necessary, adds the provided nodes to the mnesia cluster,
%% creating a new schema if there is the need to and catching up if there are
%% other nodes in the cluster already. It also updates the cluster status file.
init_db(ClusterNodes, WantDiscNode, Force) ->
    case mnesia:system_info(is_running) of
        yes -> ok;
        no  -> start_mnesia()
    end,
    case change_extra_db_nodes(ClusterNodes, Force) of
        {error, Reason} ->
            throw({error, Reason});
        {ok, Nodes} ->
            WasDiscNode = is_disc_node(),
            case {Nodes, WasDiscNode, WantDiscNode} of
                {[], _, false} ->
                    %% Standalone ram node, we don't want that
                    throw({error, cannot_create_standalone_ram_node});
                {[], false, true} ->
                    %% RAM -> disc, starting from scratch
                    ok = create_schema();
                {[], true, true} ->
                    %% First disc node up
                    ok;
                {[AnotherNode | _], _, _} ->
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
                    end
            end,
            ensure_schema_integrity(),
            update_cluster_nodes_status(),
            ok
    end.

init_db_and_upgrade(ClusterNodes, WantDiscNode, Force) ->
    ok = init_db(ClusterNodes, WantDiscNode, Force),
    ok = case rabbit_upgrade:maybe_upgrade_local() of
             ok                    -> ok;
             starting_from_scratch -> rabbit_version:record_desired();
             version_not_available -> schema_ok_or_move()
         end,
    %% `maybe_upgrade_local' restarts mnesia, so ram nodes will forget about the
    %% cluster
    case WantDiscNode of
        false -> start_mnesia(),
                 {ok, _} = change_extra_db_nodes(ClusterNodes, true),
                 wait_for_replicated_tables();
        true  -> ok
    end,
    ok.

reinit_db(Force) ->
    {AllNodes, DiscNodes, _} = read_cluster_nodes_status(),
    init_db_and_upgrade(AllNodes, should_be_disc_node(DiscNodes), Force).

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
%% definitely end up badly
check_cluster_consistency() ->
    CheckVsn = fun (This, This, _) ->
                       ok;
                   (This, Remote, Name) ->
                       throw({error,
                              {inconsistent_cluster,
                               rabbit_misc:format(
                                 "~s version mismatch: local node is ~s, "
                                 "remote node ~s", [Name, This, Remote])}})
               end,
    CheckOTP =
        fun (OTP) -> CheckVsn(erlang:system_info(otp_release), OTP, "OTP") end,
    CheckRabbit =
        fun (Rabbit) ->
                CheckVsn(rabbit_misc:rabbit_version(), Rabbit, "Rabbit")
        end,

    CheckNodes = fun (Node, AllNodes) ->
                         ThisNode = node(),
                         case lists:member(ThisNode, AllNodes) of
                             true ->
                                 ok;
                             false ->
                                 throw({error,
                                        {inconsistent_cluster,
                                         rabbit_misc:format(
                                           "Node ~p thinks it's clustered "
                                           "with node ~p, but ~p disagrees",
                                           [ThisNode, Node, Node])}})
                         end
                 end,

    lists:foreach(
      fun(Node) ->
              case rpc:call(Node, rabbit_mnesia, node_info, []) of
                  {badrpc, _Reason} ->
                      ok;
                  {OTP, Rabbit, error} ->
                      CheckOTP(OTP),
                      CheckRabbit(Rabbit);
                  {OTP, Rabbit, {ok, {AllNodes, _, _}}} ->
                      CheckOTP(OTP),
                      CheckRabbit(Rabbit),
                      CheckNodes(Node, AllNodes)
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

initial_cluster_status() ->
    {[node()], [node()], [node()]}.

write_cluster_nodes_status(Status) ->
    FileName = cluster_nodes_status_filename(),
    case rabbit_file:write_term_file(FileName, [Status]) of
        ok -> ok;
        {error, Reason} ->
            throw({error, {cannot_write_cluster_nodes_status,
                           FileName, Reason}})
    end.

try_read_cluster_nodes_status() ->
    FileName = cluster_nodes_status_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [{_, _, _} = Status]} ->
            {ok, Status};
        {ok, Term} ->
            {error, {invalid_term, FileName, Term}};
        {error, Reason} ->
            {error, {cannot_read_file, FileName, Reason}}
    end.

read_cluster_nodes_status() ->
    case try_read_cluster_nodes_status() of
        {ok, Status} ->
            Status;
        {error, Reason} ->
            throw({error, {cannot_read_cluster_nodes_status, Reason}})
    end.

%% To update the cluster status when mnesia is running.
update_cluster_nodes_status() ->
    {ok, Status} = cluster_status_if_running(),
    write_cluster_nodes_status(Status).

%%--------------------------------------------------------------------
%% Hooks for `rabbit_node_monitor'
%%--------------------------------------------------------------------

on_node_up(Node, IsDiscNode) ->
    {ok, _} = rabbit_file:map_term_file(
                fun ([{AllNodes, DiscNodes, RunningNodes}]) ->
                        [{ordsets:add_element(Node, AllNodes),
                          case IsDiscNode of
                              true  -> ordsets:add_element(Node, DiscNodes);
                              false -> DiscNodes
                          end,
                          ordsets:add_element(Node, RunningNodes)}]
                end, cluster_nodes_status_filename()),
    case is_only_disc_node(Node) of
        true  -> rabbit_log:info("cluster contains disc nodes again~n");
        false -> ok
    end.

on_node_down(Node) ->
    case is_only_disc_node(Node) of
        true  -> rabbit_log:info("only running disc node went down~n");
        false -> ok
    end,
    {ok, _} = rabbit_file:map_term_file(
                fun ([{AllNodes, DiscNodes, RunningNodes}]) ->
                        [{ordsets:del_element(Node, AllNodes),
                          ordsets:del_element(Node, DiscNodes),
                          ordsets:del_element(Node, RunningNodes)}]
                end, cluster_nodes_status_filename()),
    ok.

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

%% We only care about disc nodes since ram nodes are supposed to catch up only
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

remove_node_if_mnesia_running(Node) ->
    case mnesia:system_info(is_running) of
        yes -> %% Deleting the the schema copy of the node will result in the
               %% node being removed from the cluster, with that change being
               %% propagated to all nodes
               case mnesia:del_table_copy(schema, Node) of
                   {atomic, ok} ->
                       on_node_down(Node),
                       {_, []} = rpc:multicall(running_clustered_nodes(),
                                               rabbit_mnesia, on_node_down, [Node]),
                       ok;
                   {aborted, Reason} ->
                       {error, {failed_to_remove_node, Node, Reason}}
               end;
        no  -> {error, mnesia_not_running}
    end.

leave_cluster() ->
    remove_node_remotely(node()).

remove_node_remotely(Removee) ->
    case running_clustered_nodes() -- [Removee] of
        [] ->
            ok;
        RunningNodes ->
            case lists:any(
                   fun (Node) ->
                           case rpc:call(Node, rabbit_mnesia,
                                         remove_node_if_mnesia_running,
                                         [Removee])
                           of
                               ok ->
                                   true;
                               {error, mnesia_not_running} ->
                                   false;
                               {error, Reason} ->
                                   throw({error, Reason});
                               {badrpc, nodedown} ->
                                   false
                           end
                   end,
                   RunningNodes)
            of
                true  -> ok;
                false -> {error, no_running_cluster_nodes}
            end
    end.

wait_for(Condition) ->
    error_logger:info_msg("Waiting for ~p...~n", [Condition]),
    timer:sleep(1000).

is_only_disc_node(Node) ->
    Nodes = running_clustered_disc_nodes(),
    [Node] =:= Nodes.

start_mnesia() ->
    check_cluster_consistency(),
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

change_extra_db_nodes(ClusterNodes0, Force) ->
    ClusterNodes = lists:usort(ClusterNodes0) -- [node()],
    case mnesia:change_config(extra_db_nodes, ClusterNodes) of
        {ok, []} when not Force andalso ClusterNodes =/= [] ->
            {error, {failed_to_cluster_with, ClusterNodes,
                     "Mnesia could not connect to any disc nodes."}};
        {ok, Nodes} ->
            {ok, Nodes}
    end.

%%--------------------------------------------------------------------
%% Legacy functions related to the "running nodes" file
%%--------------------------------------------------------------------

legacy_running_nodes_filename() ->
    filename:join(dir(), "nodes_running_at_shutdown").

legacy_read_previously_running_nodes() ->
    FileName = legacy_running_nodes_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [Nodes]}   -> Nodes;
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_previous_nodes_file,
                                          FileName, Reason}})
    end.

legacy_delete_previously_running_nodes() ->
    FileName = legacy_running_nodes_filename(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({error, {cannot_delete_previous_nodes_file,
                                          FileName, Reason}})
    end.

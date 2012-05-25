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
         cluster/1, force_cluster/1, reset/0, force_reset/0, init_db/3,
         is_clustered/0, running_clustered_nodes/0, all_clustered_nodes/0,
         empty_ram_only_tables/0, copy_db/1, wait_for_tables/1,
         create_cluster_nodes_config/1, read_cluster_nodes_config/0,
         record_running_nodes/0, read_previously_running_nodes/0,
         running_nodes_filename/0, is_disc_node/0, on_node_down/1,
         on_node_up/1]).

-export([table_names/0]).

%% create_tables/0 exported for helping embed RabbitMQ in or alongside
%% other mnesia-using Erlang applications, such as ejabberd
-export([create_tables/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([node_type/0]).

-type(node_type() :: disc_only | disc | ram | unknown).
-spec(status/0 :: () -> [{'nodes', [{node_type(), [node()]}]} |
                         {'running_nodes', [node()]}]).
-spec(dir/0 :: () -> file:filename()).
-spec(ensure_mnesia_dir/0 :: () -> 'ok').
-spec(init/0 :: () -> 'ok').
-spec(init_db/3 :: ([node()], boolean(), rabbit_misc:thunk('ok')) -> 'ok').
-spec(is_db_empty/0 :: () -> boolean()).
-spec(cluster/1 :: ([node()]) -> 'ok').
-spec(force_cluster/1 :: ([node()]) -> 'ok').
-spec(cluster/2 :: ([node()], boolean()) -> 'ok').
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').
-spec(is_clustered/0 :: () -> boolean()).
-spec(running_clustered_nodes/0 :: () -> [node()]).
-spec(all_clustered_nodes/0 :: () -> [node()]).
-spec(empty_ram_only_tables/0 :: () -> 'ok').
-spec(create_tables/0 :: () -> 'ok').
-spec(copy_db/1 :: (file:filename()) ->  rabbit_types:ok_or_error(any())).
-spec(wait_for_tables/1 :: ([atom()]) -> 'ok').
-spec(create_cluster_nodes_config/1 :: ([node()]) ->  'ok').
-spec(read_cluster_nodes_config/0 :: () ->  [node()]).
-spec(record_running_nodes/0 :: () ->  'ok').
-spec(read_previously_running_nodes/0 :: () ->  [node()]).
-spec(running_nodes_filename/0 :: () -> file:filename()).
-spec(is_disc_node/0 :: () -> boolean()).
-spec(on_node_up/1 :: (node()) -> 'ok').
-spec(on_node_down/1 :: (node()) -> 'ok').

-spec(table_names/0 :: () -> [atom()]).

-endif.

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
                 no -> case all_clustered_nodes() of
                           [] -> [];
                           Nodes -> [{unknown, Nodes}]
                       end;
                 Reason when Reason =:= starting; Reason =:= stopping ->
                     exit({rabbit_busy, try_again_later})
             end},
     {running_nodes, running_clustered_nodes()}].

init() ->
    ensure_mnesia_running(),
    ensure_mnesia_dir(),
    Nodes = read_cluster_nodes_config(),
    ok = init_db(Nodes, should_be_disc_node(Nodes)),
    %% We intuitively expect the global name server to be synced when
    %% Mnesia is up. In fact that's not guaranteed to be the case - let's
    %% make it so.
    ok = global:sync(),
    ok = delete_previously_running_nodes(),
    ok.

is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

cluster(ClusterNodes) ->
    cluster(ClusterNodes, false).
force_cluster(ClusterNodes) ->
    cluster(ClusterNodes, true).

%% Alter which disk nodes this node is clustered with. This can be a
%% subset of all the disk nodes in the cluster but can (and should)
%% include the node itself if it is to be a disk rather than a ram
%% node.  If Force is false, only connections to online nodes are
%% allowed.
cluster(ClusterNodes, Force) ->
    rabbit_misc:local_info_msg("Clustering with ~p~s~n",
                               [ClusterNodes, if Force -> " forcefully";
                                                 true  -> ""
                                              end]),
    ensure_mnesia_not_running(),
    ensure_mnesia_dir(),

    case not Force andalso is_clustered() andalso
         is_only_disc_node(node(), false) andalso
         not should_be_disc_node(ClusterNodes)
    of
        true -> log_both("last running disc node leaving cluster");
        _    -> ok
    end,

    %% Wipe mnesia if we're changing type from disc to ram
    case {is_disc_node(), should_be_disc_node(ClusterNodes)} of
        {true, false} -> rabbit_misc:with_local_io(
                           fun () -> error_logger:warning_msg(
                                       "changing node type; wiping "
                                       "mnesia...~n~n")
                           end),
                         rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                                               cannot_delete_schema);
        _             -> ok
    end,

    %% Pre-emptively leave the cluster
    %%
    %% We're trying to handle the following two cases:
    %% 1. We have a two-node cluster, where both nodes are disc nodes.
    %% One node is re-clustered as a ram node.  When it tries to
    %% re-join the cluster, but before it has time to update its
    %% tables definitions, the other node will order it to re-create
    %% its disc tables.  So, we need to leave the cluster before we
    %% can join it again.
    %% 2. We have a two-node cluster, where both nodes are disc nodes.
    %% One node is forcefully reset (so, the other node thinks its
    %% still a part of the cluster).  The reset node is re-clustered
    %% as a ram node.  Same as above, we need to leave the cluster
    %% before we can join it.  But, since we don't know if we're in a
    %% cluster or not, we just pre-emptively leave it before joining.
    ProperClusterNodes = ClusterNodes -- [node()],
    try
        ok = leave_cluster(ProperClusterNodes, ProperClusterNodes)
    catch
        {error, {no_running_cluster_nodes, _, _}} when Force ->
            ok
    end,

    %% Join the cluster
    start_mnesia(),
    try
        ok = init_db(ClusterNodes, Force),
        ok = create_cluster_nodes_config(ClusterNodes)
    after
        stop_mnesia()
    end,

    ok.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages
reset()       -> reset(false).
force_reset() -> reset(true).

is_clustered() ->
    RunningNodes = running_clustered_nodes(),
    [node()] /= RunningNodes andalso [] /= RunningNodes.

all_clustered_nodes() ->
    mnesia:system_info(db_nodes).

running_clustered_nodes() ->
    mnesia:system_info(running_db_nodes).

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

%%--------------------------------------------------------------------

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

table_names() ->
    [Tab || {Tab, _} <- table_definitions()].

replicated_table_names() ->
    [Tab || {Tab, TabDef} <- table_definitions(),
            not lists:member({local_content, true}, TabDef)
    ].

dir() -> mnesia:system_info(directory).

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

%% The cluster node config file contains some or all of the disk nodes
%% that are members of the cluster this node is / should be a part of.
%%
%% If the file is absent, the list is empty, or only contains the
%% current node, then the current node is a standalone (disk)
%% node. Otherwise it is a node that is part of a cluster as either a
%% disk node, if it appears in the cluster node config, or ram node if
%% it doesn't.

cluster_nodes_config_filename() ->
    dir() ++ "/cluster_nodes.config".

create_cluster_nodes_config(ClusterNodes) ->
    FileName = cluster_nodes_config_filename(),
    case rabbit_file:write_term_file(FileName, [ClusterNodes]) of
        ok -> ok;
        {error, Reason} ->
            throw({error, {cannot_create_cluster_nodes_config,
                           FileName, Reason}})
    end.

read_cluster_nodes_config() ->
    FileName = cluster_nodes_config_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [ClusterNodes]} -> ClusterNodes;
        {error, enoent} ->
            {ok, ClusterNodes} = application:get_env(rabbit, cluster_nodes),
            ClusterNodes;
        {error, Reason} ->
            throw({error, {cannot_read_cluster_nodes_config,
                           FileName, Reason}})
    end.

delete_cluster_nodes_config() ->
    FileName = cluster_nodes_config_filename(),
    case file:delete(FileName) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} ->
            throw({error, {cannot_delete_cluster_nodes_config,
                           FileName, Reason}})
    end.

running_nodes_filename() ->
    filename:join(dir(), "nodes_running_at_shutdown").

record_running_nodes() ->
    FileName = running_nodes_filename(),
    Nodes = running_clustered_nodes() -- [node()],
    %% Don't check the result: we're shutting down anyway and this is
    %% a best-effort-basis.
    rabbit_file:write_term_file(FileName, [Nodes]),
    ok.

read_previously_running_nodes() ->
    FileName = running_nodes_filename(),
    case rabbit_file:read_term_file(FileName) of
        {ok, [Nodes]}   -> Nodes;
        {error, enoent} -> [];
        {error, Reason} -> throw({error, {cannot_read_previous_nodes_file,
                                          FileName, Reason}})
    end.

delete_previously_running_nodes() ->
    FileName = running_nodes_filename(),
    case file:delete(FileName) of
        ok              -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> throw({error, {cannot_delete_previous_nodes_file,
                                          FileName, Reason}})
    end.

init_db(ClusterNodes, Force) ->
    init_db(
      ClusterNodes, Force,
      fun () ->
              case rabbit_upgrade:maybe_upgrade_local() of
                  ok                    -> ok;
                  %% If we're just starting up a new node we won't have a
                  %% version
                  starting_from_scratch -> ok = rabbit_version:record_desired()
              end
      end).

%% Take a cluster node config and create the right kind of node - a
%% standalone disk node, or disk or ram node connected to the
%% specified cluster nodes.  If Force is false, don't allow
%% connections to offline nodes.
init_db(ClusterNodes, Force, SecondaryPostMnesiaFun) ->
    UClusterNodes = lists:usort(ClusterNodes),
    ProperClusterNodes = UClusterNodes -- [node()],
    case mnesia:change_config(extra_db_nodes, ProperClusterNodes) of
        {ok, []} when not Force andalso ProperClusterNodes =/= [] ->
            throw({error, {failed_to_cluster_with, ProperClusterNodes,
                           "Mnesia could not connect to any disc nodes."}});
        {ok, Nodes} ->
            WasDiscNode = is_disc_node(),
            WantDiscNode = should_be_disc_node(ClusterNodes),
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
                    {CopyType, CopyTypeAlt} =
                        case WantDiscNode of
                            true  -> {disc, disc_copies};
                            false -> {ram, ram_copies}
                        end,
                    ok = wait_for_replicated_tables(),
                    ok = create_local_table_copy(schema, CopyTypeAlt),
                    ok = create_local_table_copies(CopyType),

                    ok = SecondaryPostMnesiaFun(),
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

is_disc_node() -> mnesia:system_info(use_dir).

should_be_disc_node(ClusterNodes) ->
    ClusterNodes == [] orelse lists:member(node(), ClusterNodes).

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

copy_db(Destination) ->
    ok = ensure_mnesia_not_running(),
    rabbit_file:recursive_copy(dir(), Destination).

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

reset(Force) ->
    rabbit_misc:local_info_msg("Resetting Rabbit~s~n", [if Force -> " forcefully";
                                                           true  -> ""
                                                        end]),
    ensure_mnesia_not_running(),
    case not Force andalso is_clustered() andalso
         is_only_disc_node(node(), false)
    of
        true  -> log_both("no other disc nodes running");
        false -> ok
    end,
    Node = node(),
    Nodes = all_clustered_nodes() -- [Node],
    case Force of
        true  -> ok;
        false ->
            ensure_mnesia_dir(),
            start_mnesia(),
            RunningNodes =
                try
                    %% Force=true here so that reset still works when clustered
                    %% with a node which is down
                    ok = init_db(read_cluster_nodes_config(), true),
                    running_clustered_nodes() -- [Node]
                after
                    stop_mnesia()
                end,
            leave_cluster(Nodes, RunningNodes),
            rabbit_misc:ensure_ok(mnesia:delete_schema([Node]),
                                  cannot_delete_schema)
    end,
    %% We need to make sure that we don't end up in a distributed
    %% Erlang system with nodes while not being in an Mnesia cluster
    %% with them. We don't handle that well.
    [erlang:disconnect_node(N) || N <- Nodes],
    ok = delete_cluster_nodes_config(),
    %% remove persisted messages and any other garbage we find
    ok = rabbit_file:recursive_delete(filelib:wildcard(dir() ++ "/*")),
    ok.

leave_cluster([], _) -> ok;
leave_cluster(Nodes, RunningNodes) ->
    %% find at least one running cluster node and instruct it to
    %% remove our schema copy which will in turn result in our node
    %% being removed as a cluster node from the schema, with that
    %% change being propagated to all nodes
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
    end.

wait_for(Condition) ->
    error_logger:info_msg("Waiting for ~p...~n", [Condition]),
    timer:sleep(1000).

on_node_up(Node) ->
    case is_only_disc_node(Node, true) of
        true  -> rabbit_log:info("cluster contains disc nodes again~n");
        false -> ok
    end.

on_node_down(Node) ->
    case is_only_disc_node(Node, true) of
        true  -> rabbit_log:info("only running disc node went down~n");
        false -> ok
    end.

is_only_disc_node(Node, _MnesiaRunning = true) ->
    RunningSet = sets:from_list(running_clustered_nodes()),
    DiscSet = sets:from_list(nodes_of_type(disc_copies)),
    [Node] =:= sets:to_list(sets:intersection(RunningSet, DiscSet));
is_only_disc_node(Node, false) ->
    start_mnesia(),
    Res = is_only_disc_node(Node, true),
    stop_mnesia(),
    Res.

log_both(Warning) ->
    io:format("Warning: ~s~n", [Warning]),
    rabbit_misc:with_local_io(
      fun () -> error_logger:warning_msg("~s~n", [Warning]) end).

start_mnesia() ->
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ensure_mnesia_running().

stop_mnesia() ->
    stopped = mnesia:stop(),
    ensure_mnesia_not_running().

%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mnesia).

-export([ensure_mnesia_dir/0, dir/0, status/0, init/0, is_db_empty/0,
         cluster/1, reset/0, force_reset/0]).

-export([table_names/0]).

%% create_tables/0 exported for helping embed RabbitMQ in or alongside
%% other mnesia-using Erlang applications, such as ejabberd
-export([create_tables/0]).

-include("rabbit.hrl").

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(status/0 :: () -> [{'nodes' | 'running_nodes', [erlang_node()]}]).
-spec(dir/0 :: () -> string()).
-spec(ensure_mnesia_dir/0 :: () -> 'ok').
-spec(init/0 :: () -> 'ok').
-spec(is_db_empty/0 :: () -> bool()).
-spec(cluster/1 :: ([erlang_node()]) -> 'ok').
-spec(reset/0 :: () -> 'ok').
-spec(force_reset/0 :: () -> 'ok').
-spec(create_tables/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------

status() ->
    [{nodes, mnesia:system_info(db_nodes)},
     {running_nodes, mnesia:system_info(running_db_nodes)}].

init() ->
    ok = ensure_mnesia_running(),
    ok = ensure_mnesia_dir(),
    ok = init_db(read_cluster_nodes_config()),
    ok = wait_for_tables(),
    ok.

is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

%% Alter which disk nodes this node is clustered with. This can be a
%% subset of all the disk nodes in the cluster but can (and should)
%% include the node itself if it is to be a disk rather than a ram
%% node.
cluster(ClusterNodes) ->
    ok = ensure_mnesia_not_running(),
    ok = ensure_mnesia_dir(),
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    try
        ok = init_db(ClusterNodes),
        ok = wait_for_tables(),
        ok = create_cluster_nodes_config(ClusterNodes)
    after
        mnesia:stop()
    end,
    ok.

%% return node to its virgin state, where it is not member of any
%% cluster, has no cluster configuration, no local database, and no
%% persisted messages
reset()       -> reset(false).
force_reset() -> reset(true).

%%--------------------------------------------------------------------

table_definitions() ->
    [{rabbit_user,
      [{record_name, user},
       {attributes, record_info(fields, user)},
       {disc_copies, [node()]}]},
     {rabbit_user_permission,
      [{record_name, user_permission},
       {attributes, record_info(fields, user_permission)},
       {disc_copies, [node()]}]},
     {rabbit_vhost,
      [{record_name, vhost},
       {attributes, record_info(fields, vhost)},
       {disc_copies, [node()]}]},
     {rabbit_config,
      [{disc_copies, [node()]}]},
     {rabbit_listener,
      [{record_name, listener},
       {attributes, record_info(fields, listener)},
       {type, bag}]},
     {rabbit_durable_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {disc_copies, [node()]}]},
     {rabbit_route,
      [{record_name, route},
       {attributes, record_info(fields, route)},
       {type, ordered_set}]},
     {rabbit_reverse_route,
      [{record_name, reverse_route},
       {attributes, record_info(fields, reverse_route)},
       {type, ordered_set}]},
     {rabbit_durable_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)},
       {disc_copies, [node()]}]},
     {rabbit_exchange,
      [{record_name, exchange},
       {attributes, record_info(fields, exchange)}]},
     {rabbit_durable_queue,
      [{record_name, amqqueue},
       {attributes, record_info(fields, amqqueue)},
       {disc_copies, [node()]}]},
     {rabbit_queue,
      [{record_name, amqqueue},
       {attributes, record_info(fields, amqqueue)}]}].

table_names() ->
    [Tab || {Tab, _} <- table_definitions()].

dir() -> mnesia:system_info(directory).
    
ensure_mnesia_dir() ->
    MnesiaDir = dir() ++ "/",
    case filelib:ensure_dir(MnesiaDir) of
        {error, Reason} ->
            throw({error, {cannot_create_mnesia_dir, MnesiaDir, Reason}});
        ok -> ok
    end.

ensure_mnesia_running() ->
    case mnesia:system_info(is_running) of
        yes -> ok;
        no -> throw({error, mnesia_not_running})
    end.

ensure_mnesia_not_running() ->
    case mnesia:system_info(is_running) of
        no -> ok;
        yes -> throw({error, mnesia_unexpectedly_running})
    end.

check_schema_integrity() ->
    %%TODO: more thorough checks
    case catch [mnesia:table_info(Tab, version) || Tab <- table_names()] of
        {'EXIT', Reason} -> {error, Reason};
        _ -> ok
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
    Handle = case file:open(FileName, [write]) of
                 {ok, Device} -> Device;
                 {error, Reason} ->
                     throw({error, {cannot_create_cluster_nodes_config,
                                    FileName, Reason}})
             end,
    try
        ok = io:write(Handle, ClusterNodes),
        ok = io:put_chars(Handle, [$.])
    after
        case file:close(Handle) of
            ok -> ok;
            {error, Reason1} ->
                throw({error, {cannot_close_cluster_nodes_config,
                               FileName, Reason1}})
        end
    end,
    ok.

read_cluster_nodes_config() ->
    FileName = cluster_nodes_config_filename(),
    case file:consult(FileName) of
        {ok, [ClusterNodes]} -> ClusterNodes;
        {error, enoent} ->
            case application:get_env(cluster_config) of
                undefined -> [];
                {ok, DefaultFileName} ->
                    case file:consult(DefaultFileName) of
                        {ok, [ClusterNodes]} -> ClusterNodes;
                        {error, enoent} ->
                            error_logger:warning_msg(
                              "default cluster config file ~p does not exist~n",
                              [DefaultFileName]),
                            [];
                        {error, Reason} ->
                            throw({error, {cannot_read_cluster_nodes_config,
                                           DefaultFileName, Reason}})
                    end
            end;
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

%% Take a cluster node config and create the right kind of node - a
%% standalone disk node, or disk or ram node connected to the
%% specified cluster nodes.
init_db(ClusterNodes) ->
    WasDiskNode = mnesia:system_info(use_dir),
    IsDiskNode = ClusterNodes == [] orelse
        lists:member(node(), ClusterNodes),
    case mnesia:change_config(extra_db_nodes, ClusterNodes -- [node()]) of
        {ok, []} ->
            if WasDiskNode and IsDiskNode ->
                    case check_schema_integrity() of
                        ok ->
                            ok;
                        {error, Reason} ->
                            %% NB: we cannot use rabbit_log here since
                            %% it may not have been started yet
                            error_logger:warning_msg(
                              "schema integrity check failed: ~p~n"
                              "moving database to backup location "
                              "and recreating schema from scratch~n",
                              [Reason]),
                            ok = move_db(),
                            ok = create_schema()
                    end;
               WasDiskNode ->
                    throw({error, {cannot_convert_disk_node_to_ram_node,
                                   ClusterNodes}});
               IsDiskNode ->
                    ok = create_schema();
               true ->
                    throw({error, {unable_to_contact_cluster_nodes,
                                   ClusterNodes}})
            end;
        {ok, [_|_]} ->
            ok = wait_for_tables(),
            ok = create_local_table_copies(
                   case IsDiskNode of
                       true  -> disc;
                       false -> ram
                   end);
        {error, Reason} ->
            %% one reason we may end up here is if we try to join
            %% nodes together that are currently running standalone or
            %% are members of a different cluster
            throw({error, {unable_to_join_cluster,
                           ClusterNodes, Reason}})
    end.

create_schema() ->
    mnesia:stop(),
    rabbit_misc:ensure_ok(mnesia:create_schema([node()]),
                          cannot_create_schema),
    rabbit_misc:ensure_ok(mnesia:start(),
                          cannot_start_mnesia),
    create_tables().

move_db() ->
    mnesia:stop(),
    MnesiaDir = filename:dirname(dir() ++ "/"),
    {{Year, Month, Day}, {Hour, Minute, Second}} = erlang:universaltime(),
    BackupDir = lists:flatten(
                  io_lib:format("~s_~w~2..0w~2..0w~2..0w~2..0w~2..0w",
                                [MnesiaDir,
                                 Year, Month, Day, Hour, Minute, Second])),
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
    ok = ensure_mnesia_dir(),
    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    ok.

create_tables() ->
    lists:foreach(fun ({Tab, TabArgs}) ->
                          case mnesia:create_table(Tab, TabArgs) of
                              {atomic, ok} -> ok;
                              {aborted, Reason} ->
                                  throw({error, {table_creation_failed,
                                                 Tab, TabArgs, Reason}})
                          end
                  end,
                  table_definitions()),
    ok.

create_local_table_copies(Type) ->
    ok = if Type /= ram -> create_local_table_copy(schema, disc_copies);
            true -> ok
         end,
    lists:foreach(
      fun({Tab, TabDef}) ->
              HasDiscCopies =
                  lists:keymember(disc_copies, 1, TabDef),
              HasDiscOnlyCopies =
                  lists:keymember(disc_only_copies, 1, TabDef),
              StorageType =
                  case Type of
                      disc ->
                          if
                              HasDiscCopies -> disc_copies;
                              HasDiscOnlyCopies -> disc_only_copies;
                              true -> ram_copies
                          end;
%% unused code - commented out to keep dialyzer happy
%%                      disc_only ->
%%                          if
%%                              HasDiscCopies or HasDiscOnlyCopies ->
%%                                  disc_only_copies;
%%                              true -> ram_copies
%%                          end;
                      ram ->
                          ram_copies
                  end,
              ok = create_local_table_copy(Tab, StorageType)
      end,
      table_definitions()),
    ok = if Type == ram -> create_local_table_copy(schema, ram_copies);
            true -> ok
         end,
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

wait_for_tables() -> 
    case check_schema_integrity() of
        ok ->
            case mnesia:wait_for_tables(table_names(), 30000) of
                ok -> ok;
                {timeout, BadTabs} ->
                    throw({error, {timeout_waiting_for_tables, BadTabs}});
                {error, Reason} ->
                    throw({error, {failed_waiting_for_tables, Reason}})
            end;
        {error, Reason} ->
            throw({error, {schema_integrity_check_failed, Reason}})
    end.

reset(Force) ->
    ok = ensure_mnesia_not_running(),
    Node = node(),
    case Force of
        true  -> ok;
        false ->
            ok = ensure_mnesia_dir(),
            rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
            {Nodes, RunningNodes} =
                try
                    ok = init(),
                    {mnesia:system_info(db_nodes) -- [Node],
                     mnesia:system_info(running_db_nodes) -- [Node]}
                after
                    mnesia:stop()
                end,
            leave_cluster(Nodes, RunningNodes),
            rabbit_misc:ensure_ok(mnesia:delete_schema([Node]),
                                  cannot_delete_schema)
    end,
    ok = delete_cluster_nodes_config(),
    %% remove persistet messages and any other garbage we find
    lists:foreach(fun file:delete/1,
                  filelib:wildcard(dir() ++ "/*")),
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

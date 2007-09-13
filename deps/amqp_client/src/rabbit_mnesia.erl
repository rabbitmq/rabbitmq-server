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
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
%%   Technologies Ltd.; 
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_mnesia).

-export([start/0, stop/0, status/0, init/0, is_db_empty/0, clear_db/0,
         join_cluster/2, leave_cluster/0]).

-include("rabbit.hrl").

%%--------------------------------------------------------------------

start() ->
    ok = ensure_mnesia_dir(),
    mnesia:start().

stop() ->
    case mnesia:stop() of
        stopped -> ok;
        Other -> Other
    end.

status() ->
    [{nodes, mnesia:system_info(db_nodes)},
     {running_nodes, mnesia:system_info(running_db_nodes)}].

init() ->
    ok = ensure_mnesia_running(),
    ok = ensure_mnesia_dir(),
    UseDir = mnesia:system_info(use_dir),
    ClusterNodes = read_cluster_nodes_config(),
    HasClusterNodes = ClusterNodes /= [],
    if
        UseDir and HasClusterNodes ->
            throw({error, attempt_to_start_disk_node_as_ram_node});
        UseDir ->
            ok;
        HasClusterNodes ->
            ok = add_extra_db_nodes(ClusterNodes),
            %% allow ram nodes to appear "out of nowhere", i.e.
            %% without having been explicitly joined to the cluster
            ok = ensure_schema_integrity(),
            ok = create_local_table_copies(ram);
        true ->
            mnesia:stop(),
            rabbit_misc:ensure_ok(mnesia:create_schema([node()]),
                                  cannot_create_schema),
            rabbit_misc:ensure_ok(mnesia:start(),
                                  cannot_start_mnesia),
            ok = populate_schema()
    end,
    ok = ensure_schema_integrity(),
    ok = wait_for_tables(),
    ok.

is_db_empty() ->
    lists:all(fun (Tab) -> mnesia:dirty_first(Tab) == '$end_of_table' end,
              table_names()).

clear_db() ->
    ok = ensure_mnesia_not_running(),
    ok = ensure_mnesia_dir(),
    ok = ensure_not_clustered(),
    case mnesia:system_info(use_dir) of
        false -> throw({error, {db_not_found,
                                mnesia:system_info(directory)}});
        true -> ok
    end,
    ok = wipe_out_db(),
    ok.

join_cluster(Type, ClusterNodes = [_|_]) ->
    ok = ensure_mnesia_not_running(),
    ok = ensure_mnesia_dir(),
    ok = ensure_not_clustered(),
    case mnesia:system_info(use_dir) of
        true -> throw({error, {db_found,
                               mnesia:system_info(directory)}});
        false -> ok
    end,
    if Type == ram -> ok = create_cluster_nodes_config(ClusterNodes);
       true -> ok
    end,
    rabbit_misc:with_error_handler(
      fun delete_cluster_nodes_config/0,
      fun () ->
              rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
              try
                  ok = add_extra_db_nodes(ClusterNodes),
                  ok = ensure_schema_integrity(),
                  ok = create_local_table_copies(Type),
                  ok = wait_for_tables()
              after
                  mnesia:stop()
              end
      end),
    ok.

leave_cluster() ->

    ok = ensure_mnesia_not_running(),
    ok = ensure_mnesia_dir(),
    ok = ensure_clustered(),

    rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia),
    {Nodes, RunningNodes} =
        try
            case read_cluster_nodes_config() of
                [] -> ok;
                ClusterNodes = [_|_] ->
                    ok = add_extra_db_nodes(ClusterNodes)
            end,
            ok = ensure_schema_integrity(),
            ok = wait_for_tables(),
            {mnesia:system_info(db_nodes) -- [node()],
             mnesia:system_info(running_db_nodes) -- [node()]}
        after
            mnesia:stop()
        end,

    %% find at least one running cluster node and instruct it to
    %% remove our schema copy which will in turn result in our node
    %% being removed as a cluster node from the schema, with that
    %% change being propagated to all nodes
    case lists:any(fun (Node) ->
                           case rpc:call(Node, mnesia, del_table_copy,
                                         [schema, node()]) of
                               {atomic, ok} -> true;
                               {badrpc, nodedown} -> false;
                               {aborted, Reason} ->
                                   throw({error, {failed_to_leave_cluster,
                                                  Nodes, RunningNodes,
                                                  Reason}})
                           end
                   end,
                   RunningNodes) of
        true -> ok;
        false -> throw({error, {no_running_cluster_nodes,
                                Nodes, RunningNodes}})
    end,

    ok = delete_cluster_nodes_config(),
    ok = wipe_out_db(),

    ok.


%%--------------------------------------------------------------------

table_definitions() ->
    [{user, [{disc_copies, [node()]},
             {attributes, record_info(fields, user)}]},
     {user_vhost, [{type, bag},
                   {disc_copies, [node()]},
                   {attributes, record_info(fields, user_vhost)},
                   {index, [virtual_host]}]},
     {vhost, [{disc_copies, [node()]},
              {attributes, record_info(fields, vhost)}]},
     {vhost_realm, [{type, bag},
                    {disc_copies, [node()]},
                    {attributes, record_info(fields, vhost_realm)},
                    {index, [realm]}]},
     {realm, [{disc_copies, [node()]},
              {attributes, record_info(fields, realm)}]},
     {user_realm, [{type, bag},
                   {disc_copies, [node()]},
                   {attributes, record_info(fields, user_realm)},
                   {index, [realm]}]},
     {exclusive_realm_visitor,
      [{record_name, realm_visitor},
       {attributes, record_info(fields, realm_visitor)},
       {index, [pid]}]},
     {realm_visitor, [{type, bag},
                      {attributes, record_info(fields, realm_visitor)},
                      {index, [pid]}]},
     {rabbit_config, [{disc_copies, [node()]}]},
     {listener, [{type, bag},
                 {attributes, record_info(fields, listener)}]},
     {binding, [{attributes, record_info(fields, binding)}]},
     {durable_exchanges, [{disc_copies, [node()]},
                          {record_name, exchange},
                          {attributes, record_info(fields, exchange)}]},
     {exchange, [{attributes, record_info(fields, exchange)}]},
     {durable_queues, [{disc_copies, [node()]},
                       {record_name, amqqueue},
                       {attributes, record_info(fields, amqqueue)}]},
     {amqqueue, [{attributes, record_info(fields, amqqueue)},
                 {index, [pid]}]}].

table_names() ->
    lists:map(fun ({Tab, _}) -> Tab end,
              table_definitions()).

ensure_mnesia_dir() ->
    MnesiaDir = mnesia:system_info(directory) ++ "/",
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

ensure_schema_integrity() ->
    case catch lists:foreach(fun (Tab) ->
                                     mnesia:table_info(Tab, version)
                             end,
                             table_names()) of
        {'EXIT', Reason} -> throw({error, {schema_integrity_check_failed,
                                           Reason}});
        _ -> ok
    end,
    %%TODO: more thorough checks
    ok.

ensure_clustered() ->
    case other_cluster_nodes() of
        [] -> throw({error, node_is_not_clustered});
        [_|_] -> ok
    end.

ensure_not_clustered() ->
    case other_cluster_nodes() of
        [] -> ok;
        Nodes = [_|_] -> throw({error, {node_is_clustered, Nodes}})
    end.

other_cluster_nodes() ->
    (mnesia:system_info(db_nodes) ++
     mnesia:system_info(extra_db_nodes) ++
     read_cluster_nodes_config())
        -- [node()].

cluster_nodes_config_filename() ->
    mnesia:system_info(directory) ++ "/cluster_nodes.config".

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
        {error, enoent} -> [];
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

add_extra_db_nodes(ClusterNodes) ->
    case mnesia:change_config(extra_db_nodes, ClusterNodes) of
        {ok, []} -> throw({error, {unable_to_contact_cluster_nodes,
                                   ClusterNodes}});
        {ok, [_|_]} -> ok;
        {error, Reason} -> throw({error, {unable_to_join_cluster,
                                          ClusterNodes, Reason}})
    end.

populate_schema() ->
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
                      disc_only ->
                          if
                              HasDiscCopies or HasDiscOnlyCopies ->
                                  disc_only_copies;
                              true -> ram_copies
                          end;
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
    case mnesia:wait_for_tables(table_names(), 30000) of
        ok -> ok;
        {timeout, BadTabs} ->
            throw({error, {timeout_waiting_for_tables, BadTabs}});
        {error, Reason} ->
            throw({error, {failed_waiting_for_tables, Reason}})
    end.

wipe_out_db() ->
    rabbit_misc:ensure_ok(mnesia:delete_schema([node()]),
                          cannot_delete_schema),
    %% remove persistet messages and any other garbage we find
    lists:foreach(fun file:delete/1,
                  filelib:wildcard(mnesia:system_info(directory) ++ "/*")),
    ok.

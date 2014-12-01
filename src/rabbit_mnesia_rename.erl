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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_mnesia_rename).
-include("rabbit.hrl").

-export([rename/2]).
-export([maybe_finish/1]).

-define(CONVERT_TABLES, [schema, rabbit_durable_queue]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(rename/2 :: (node(), [{node(), node()}]) -> 'ok').
-spec(maybe_finish/1 :: ([node()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

rename(Node, NodeMapList) ->
    try
        NodeMap = dict:from_list(NodeMapList),
        {FromNodes, ToNodes} = lists:unzip(NodeMapList),
        case length(FromNodes) - length(lists:usort(ToNodes)) of
            0 -> ok;
            _ -> exit({duplicate_node, ToNodes})
        end,
        FromNode = case [From || {From, To} <- NodeMapList,
                                 To =:= Node] of
                       [N] -> N;
                       []  -> Node
                   end,
        ToNode = case dict:find(FromNode, NodeMap) of
                     {ok, N2} -> N2;
                     error    -> FromNode
                 end,
        rabbit_control_main:become(FromNode),
        start_mnesia(),
        Nodes = rabbit_mnesia:cluster_nodes(all),
        case {FromNodes -- Nodes, ToNodes -- (ToNodes -- Nodes)} of
            {[], []} -> ok;
            {F,  []} -> exit({nodes_not_in_cluster,     F});
            {_,  T}  -> exit({nodes_already_in_cluster, T})
        end,
        rabbit_table:force_load(),
        rabbit_table:wait_for_replicated(),
        FromBackup = from_backup_name(),
        ToBackup = to_backup_name(),
        ok = rabbit_file:ensure_dir(FromBackup),
        ok = mnesia:backup(FromBackup),
        stop_mnesia(),
        convert_backup(NodeMap, FromBackup, ToBackup),
        ok = rabbit_file:write_term_file(rename_config_name(),
                                         [{FromNode, ToNode}]),
        convert_config_files(NodeMap),
        ok
    after
        stop_mnesia()
    end.

maybe_finish(AllNodes) ->
    case rabbit_file:read_term_file(rename_config_name()) of
        {ok, [{FromNode, ToNode}]} -> finish(FromNode, ToNode, AllNodes);
        _                          -> ok
    end.

finish(FromNode, ToNode, AllNodes) ->
    case node() of
        ToNode ->
            case rabbit_upgrade:nodes_running(AllNodes) of
                [] -> finish_primary(FromNode, ToNode);
                _  -> finish_secondary(FromNode, ToNode, AllNodes)
            end;
        FromNode ->
            rabbit_log:info(
              "Abandoning rename from ~s to ~s since we are still ~s~n",
              [FromNode, ToNode, FromNode]),
            [{ok, _} = file:copy(backup_of_conf(F), F) || F <- config_files()],
            delete_rename_files();
        _ ->
            %% Boot will almost certainly fail but we might as
            %% well just log this
            rabbit_log:info(
              "Rename attempted from ~s to ~s but we are ~s - ignoring.~n",
              [FromNode, ToNode, node()])
    end.

finish_primary(FromNode, ToNode) ->
    rabbit_log:info("Restarting as primary after rename from ~s to ~s~n",
                    [FromNode, ToNode]),
    %% We are alone, restore the backup we previously took
    ToBackup = to_backup_name(),
    ok = mnesia:install_fallback(ToBackup, [{scope, local}]),
    start_mnesia(),
    rabbit_table:force_load(),
    rabbit_table:wait_for_replicated(),
    stop_mnesia(),
    delete_rename_files(),
    rabbit_mnesia:force_load_next_boot(),
    ok.

finish_secondary(FromNode, ToNode, AllNodes) ->
    rabbit_log:info("Restarting as secondary after rename from ~s to ~s~n",
                    [FromNode, ToNode]),
    rabbit_upgrade:secondary_upgrade(AllNodes),
    rename_remote_node(FromNode, ToNode),
    delete_rename_files(),
    ok.

temp_dir_name()      -> "rename".
dir()                -> rabbit_mnesia:dir() ++ "/" ++ temp_dir_name().
from_backup_name()   -> dir() ++ "/backup-from".
to_backup_name()     -> dir() ++ "/backup-to".
rename_config_name() -> dir() ++ "/pending.config".

delete_rename_files() -> ok = rabbit_file:recursive_delete([dir()]).

start_mnesia() -> rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia).
stop_mnesia()  -> stopped = mnesia:stop().

convert_backup(NodeMap, FromBackup, ToBackup) ->
    mnesia:traverse_backup(
      FromBackup, ToBackup,
      fun
          (Row, Acc) ->
              case lists:member(element(1, Row), ?CONVERT_TABLES) of
                  true  -> {[update_term(NodeMap, Row)], Acc};
                  false -> {[Row], Acc}
              end
      end, switched).

config_files() ->
    [rabbit_node_monitor:running_nodes_filename(),
     rabbit_node_monitor:cluster_status_filename()].

backup_of_conf(Path) ->
    filename:join([filename:dirname(Path),
                   temp_dir_name(),
                   filename:basename(Path)]).

convert_config_files(NodeMap) ->
    [convert_config_file(NodeMap, Path) || Path <- config_files()].

convert_config_file(NodeMap, Path) ->
    {ok, Term} = rabbit_file:read_term_file(Path),
    {ok, _} = file:copy(Path, backup_of_conf(Path)),
    ok = rabbit_file:write_term_file(Path, update_term(NodeMap, Term)).

lookup_node(OldNode, NodeMap) ->
    case dict:find(OldNode, NodeMap) of
        {ok, NewNode} -> NewNode;
        error         -> OldNode
    end.

mini_map(FromNode, ToNode) -> dict:from_list([{FromNode, ToNode}]).

update_term(NodeMap, L) when is_list(L) ->
    [update_term(NodeMap, I) || I <- L];
update_term(NodeMap, T) when is_tuple(T) ->
    list_to_tuple(update_term(NodeMap, tuple_to_list(T)));
update_term(NodeMap, Node) when is_atom(Node) ->
    lookup_node(Node, NodeMap);
update_term(NodeMap, Pid) when is_pid(Pid) ->
    rabbit_misc:pid_change_node(Pid, lookup_node(node(Pid), NodeMap));
update_term(_NodeMap, Term) ->
    Term.

rename_remote_node(FromNode, ToNode) ->
    All = rabbit_mnesia:cluster_nodes(all),
    Running = rabbit_mnesia:cluster_nodes(running),
    case {lists:member(FromNode, Running), lists:member(ToNode, All)} of
        {false, true}  -> ok;
        {true,  _}     -> exit({old_node_running,        FromNode});
        {_,     false} -> exit({new_node_not_in_cluster, ToNode})
    end,
    {atomic, ok} = mnesia:del_table_copy(schema, FromNode),
    Map = mini_map(FromNode, ToNode),
    {atomic, _} = transform_table(rabbit_durable_queue, Map),
    ok.

transform_table(Table, Map) ->
    mnesia:sync_transaction(
      fun () ->
              mnesia:lock({table, Table}, write),
              transform_table(Table, Map, mnesia:first(Table))
      end).

transform_table(_Table, _Map, '$end_of_table') ->
    ok;
transform_table(Table, Map, Key) ->
    [Term] = mnesia:read(Table, Key, write),
    ok = mnesia:write(Table, update_term(Map, Term), write),
    transform_table(Table, Map, mnesia:next(Table, Key)).

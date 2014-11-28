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

-export([rename/3]).
-export([maybe_complete_rename/1]).

-define(CONVERT_TABLES, [schema, rabbit_durable_queue]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(rename/3 :: (node(), node(), [node()]) -> 'ok').
-spec(maybe_complete_rename/1 :: ([node()]) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

rename(FromNode, ToNode, Others) ->
    NodeMap = dict:from_list(split_others([FromNode, ToNode | Others])),
    try
        rabbit_control_main:become(FromNode),
        rabbit_log:info("Renaming node ~s to ~s~n", [FromNode, ToNode]),
        start_mnesia(),
        Nodes = rabbit_mnesia:cluster_nodes(all),
        case {lists:member(FromNode, Nodes), lists:member(ToNode, Nodes)} of
            {true,  false} -> ok;
            {false, _}     -> exit({node_not_in_cluster,     FromNode});
            {_,     true}  -> exit({node_already_in_cluster, ToNode})
        end,
        rabbit_table:force_load(),
        rabbit_table:wait_for_replicated(),
        FromBackup = from_backup_name(),
        ToBackup = to_backup_name(),
        io:format("  * Backing up to '~s'~n", [FromBackup]),
        ok = mnesia:backup(FromBackup),
        stop_mnesia(),
        io:format("  * Converting backup '~s'~n", [ToBackup]),
        convert_backup(NodeMap, FromBackup, ToBackup),
        ok = rabbit_file:write_term_file(rename_config_name(),
                                         [{FromNode, ToNode}]),
        io:format("  * Converting config files~n", []),
        convert_config_file(NodeMap,
                            rabbit_node_monitor:running_nodes_filename()),
        convert_config_file(NodeMap,
                            rabbit_node_monitor:cluster_status_filename()),
        ok
    after
        stop_mnesia()
    end.

split_others([])         -> [];
split_others([_])        -> exit(even_list_needed);
split_others([A, B | T]) -> [{A, B} | split_others(T)].

maybe_complete_rename(AllNodes) ->
    case rabbit_file:read_term_file(rename_config_name()) of
        {ok, [{FromNode, ToNode}]} ->
            case rabbit_upgrade:nodes_running(AllNodes) of
                [] -> complete_rename_primary();
                _  -> complete_rename_secondary(FromNode, ToNode, AllNodes)
            end;
        _ ->
            ok
    end.

complete_rename_primary() ->
    %% We are alone, restore the backup we previously took
    ToBackup = to_backup_name(),
    io:format("  * Loading backup '~s'~n", [ToBackup]),
    ok = mnesia:install_fallback(ToBackup, [{scope, local}]),
    start_mnesia(),
    stop_mnesia(),
    rabbit_file:delete(rename_config_name()),
    rabbit_file:delete(from_backup_name()),
    rabbit_file:delete(to_backup_name()),
    rabbit_mnesia:force_load_next_boot(),
    ok.

complete_rename_secondary(FromNode, ToNode, AllNodes) ->
    rabbit_upgrade:secondary_upgrade(AllNodes),
    rename_remote_node(FromNode, ToNode),
    rabbit_file:delete(rename_config_name()),
    rabbit_file:delete(from_backup_name()),
    rabbit_file:delete(to_backup_name()),
    ok.

from_backup_name()   -> rabbit_mnesia:dir() ++ "/rename-backup-from".
to_backup_name()     -> rabbit_mnesia:dir() ++ "/rename-backup-to".
rename_config_name() -> rabbit_mnesia:dir() ++ "/rename-pending.config".

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

convert_config_file(NodeMap, Path) ->
    {ok, Term} = rabbit_file:read_term_file(Path),
    ok = rabbit_file:write_term_file(Path, update_term(NodeMap, Term)).

lookup_node(OldNode, NodeMap) ->
    case dict:find(OldNode, NodeMap) of
        {ok, NewNode} -> NewNode;
        error         -> OldNode
    end.

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

%%----------------------------------------------------------------------------

rename_remote_node(FromNode, ToNode) ->
    All = rabbit_mnesia:cluster_nodes(all),
    Running = rabbit_mnesia:cluster_nodes(running),
    case {lists:member(FromNode, Running), lists:member(ToNode, All)} of
        {false, true}  -> ok;
        {true,  _}     -> exit({old_node_running,        FromNode});
        {_,     false} -> exit({new_node_not_in_cluster, ToNode})
    end,
    mnesia:del_table_copy(schema, FromNode),
    NodeMap = dict:from_list([{FromNode, ToNode}]),
    {atomic, ok} = mnesia:transform_table(
                     rabbit_durable_queue,
                     fun (Q) -> update_term(NodeMap, Q) end,
                     record_info(fields, amqqueue)),
    ok.

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

-module(rabbit_mnesia_offline).

-export([rename_local_node/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(rename_local_node/2 :: (node(), node()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

rename_local_node(FromNode, ToNode) ->
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
        FromBackup = rabbit_mnesia:dir() ++ "/rename-backup-from",
        ToBackup = rabbit_mnesia:dir() ++ "/rename-backup-to",
        io:format("  * Backing up to '~s'~n", [FromBackup]),
        ok = mnesia:backup(FromBackup),
        stop_mnesia(),
        rabbit_control_main:become(ToNode),
        io:format("  * Converting backup '~s'~n", [ToBackup]),
        convert_backup(FromNode, ToNode, FromBackup, ToBackup),
        ok = mnesia:install_fallback(ToBackup),
        io:format("  * Loading backup '~s'~n", [ToBackup]),
        start_mnesia(),
        io:format("  * Converting config files~n", []),
        convert_config_file(FromNode, ToNode,
                            rabbit_node_monitor:running_nodes_filename()),
        convert_config_file(FromNode, ToNode,
                            rabbit_node_monitor:cluster_status_filename()),
        ok
    after
        stop_mnesia()
    end.

start_mnesia() -> rabbit_misc:ensure_ok(mnesia:start(), cannot_start_mnesia).
stop_mnesia()  -> stopped = mnesia:stop().

convert_backup(FromNode, ToNode, FromBackup, ToBackup) ->
    Switch = fun
                 (Node) when Node == FromNode ->
                     ToNode;
                 (Node) when Node == ToNode ->
                     throw({error, {already_exists, Node}});
                 (Node) ->
                     Node
             end,
    Convert =
        fun
            %% TODO do we ever hit these three heads?
            ({schema, db_nodes, Nodes}, Acc) ->
                io:format(" +++ db_nodes ~p~n", [Nodes]),
                {[{schema, db_nodes, lists:map(Switch,Nodes)}], Acc};
            ({schema, version, Version}, Acc) ->
                io:format(" +++ version: ~p~n", [Version]),
                {[{schema, version, Version}], Acc};
            ({schema, cookie, Cookie}, Acc) ->
                io:format(" +++ cookie: ~p~n", [Cookie]),
                {[{schema, cookie, Cookie}], Acc};
            ({schema, Tab, CreateList}, Acc) ->
                %% io:format("~n * Checking table: '~p'~n", [Tab]),
                %%io:format("  . Initial content: ~p~n", [CreateList]),
                Keys = [ram_copies, disc_copies, disc_only_copies],
                OptSwitch =
                    fun({Key, Val}) ->
                            case lists:member(Key, Keys) of
                                true ->
                                    %%io:format("   + Checking key: '~p'~n", [Key]),
                                    {Key, lists:map(Switch, Val)};
                                false->
                                    {Key, Val}
                            end
                    end,
                Res = {[{schema, Tab, lists:map(OptSwitch, CreateList)}], Acc},
                %%io:format("  . Resulting content: ~p~n", [Res]),
                Res;
            (Other, Acc) ->
                case lists:member(element(1, Other), [rabbit_durable_queue]) of
                    true  -> Other1 = update_term(FromNode, ToNode, Other),
                             io:format(" --- ~p~n +++ ~p~n", [Other, Other1]),
                             {[Other1], Acc};
                    false -> {[Other], Acc}
                end
        end,
    mnesia:traverse_backup(FromBackup, ToBackup, Convert, switched).

convert_config_file(FromNode, ToNode, Path) ->
    {ok, Term} = rabbit_file:read_term_file(Path),
    ok = rabbit_file:write_term_file(Path, update_term(FromNode, ToNode, Term)).

update_term(N1, N2, L) when is_list(L) ->
    [update_term(N1, N2, I) || I <- L];
update_term(N1, N2, T) when is_tuple(T) ->
    list_to_tuple(update_term(N1, N2, tuple_to_list(T)));
update_term(N1, N2, N1) ->
    N2;
update_term(N1, N2, Pid) when is_pid(Pid), node(Pid) == N1 ->
    rabbit_misc:pid_change_node(Pid, N2);
update_term(_N1, _N2, Term) ->
    Term.

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracking).

%% Common behaviour and processing functions for tracking components
%%
%% See in use:
%%  * rabbit_connection_tracking
%%  * rabbit_channel_tracking

-callback boot() -> ok.
-callback update_tracked(term()) -> ok.
-callback handle_cast(term()) -> ok.
-callback register_tracked(
              rabbit_types:tracked_connection() |
                  rabbit_types:tracked_channel()) -> 'ok'.
-callback unregister_tracked(
              rabbit_types:tracked_connection_id() |
                  rabbit_types:tracked_channel_id()) -> 'ok'.
-callback count_tracked_items_in(term()) -> non_neg_integer().
-callback clear_tracking_tables() -> 'ok'.
-callback shutdown_tracked_items(list(), term()) -> ok.

-export([id/2, count_tracked_items/3, match_tracked_items/2,
         clear_tracking_table/1, delete_tracking_table/3,
         delete_tracked_entry/3]).

%%----------------------------------------------------------------------------

-spec id(atom(), term()) ->
    rabbit_types:tracked_connection_id() | rabbit_types:tracked_channel_id().

id(Node, Name) -> {Node, Name}.

-spec count_tracked_items(function(), term(), string()) ->
    non_neg_integer().

count_tracked_items(Tab, Key, ContextMsg) ->
    lists:foldl(fun (Node, Acc) when Node == node() ->
                        N = case ets:lookup(Tab, Key) of
                                []         -> 0;
                                [{_, Val}] -> Val
                            end,
                        Acc + N;
                    (Node, Acc) ->
                        N = case rabbit_misc:rpc_call(Node, ets, lookup, [Tab, Key]) of
                                [] -> 0;
                                [{_, Val}] -> Val;
                                {badrpc, Err} ->
                                    rabbit_log:error(
                                      "Failed to fetch number of ~p ~p on node ~p:~n~p",
                                      [ContextMsg, Key, Node, Err]),
                                    0
                            end,
                        Acc + N
                end, 0, rabbit_nodes:all_running()).

-spec match_tracked_items(function(), tuple()) -> term().

match_tracked_items(Tab, MatchSpec) ->
    lists:foldl(
        fun (Node, Acc) when Node == node() ->
                Acc ++ ets:match_object(
                         Tab,
                         MatchSpec);
            (Node, Acc) ->
                case rabbit_misc:rpc_call(Node, ets, match_object, [Tab, MatchSpec]) of
                    List when is_list(List) ->
                        Acc ++ List;
                    _ ->
                        Acc
                end
        end, [], rabbit_nodes:all_running()).

-spec clear_tracking_table(atom()) -> ok.

clear_tracking_table(TableName) ->
    try
        true = ets:delete_all_objects(TableName),
        ok
    catch
        error:badarg ->
            ok
    end.

-spec delete_tracking_table(atom(), node(), string()) -> ok.

delete_tracking_table(TableName, Node, _ContextMsg) when Node == node() ->
    true = ets:delete(TableName),
    ok;
delete_tracking_table(TableName, Node, ContextMsg) ->
    case rabbit_misc:rpc_call(Node, ets, delete, [TableName]) of
        true -> ok;
        {badrpc, Error} ->
            rabbit_log:error("Failed to delete a ~p table for node ~p: ~p",
                [ContextMsg, Node, Error]),
            ok
    end.

-spec delete_tracked_entry({atom(), atom(), list()}, function(), term()) -> ok.

delete_tracked_entry(_ExistsCheckSpec = {M, F, A}, TableName, Key) ->
    ClusterNodes = rabbit_nodes:all_running(),
    ExistsInCluster =
        lists:any(fun(Node) -> rpc:call(Node, M, F, A) end, ClusterNodes),
    case ExistsInCluster of
        false ->
            [delete_tracked_entry0(Node, TableName, Key) || Node <- ClusterNodes];
        true ->
            ok
    end.

delete_tracked_entry0(Node, Tab, Key) when Node == node() ->
    true = ets:delete(Tab, Key);
delete_tracked_entry0(Node, Tab, Key) ->
    _ = rabbit_misc:rpc_call(Node, ets, delete, [Tab, Key]),
    ok.

%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

-export([id/2, count_tracked_items/4, match_tracked_items/2,
         clear_tracking_table/1, delete_tracking_table/3,
         delete_tracked_entry/3]).

%%----------------------------------------------------------------------------

-spec id(atom(), term()) ->
    rabbit_types:tracked_connection_id() | rabbit_types:tracked_channel_id().

id(Node, Name) -> {Node, Name}.

-spec count_tracked_items(function(), integer(), term(), string()) ->
    non_neg_integer().

count_tracked_items(TableNameFun, CountRecPosition, Key, ContextMsg) ->
    lists:foldl(fun (Node, Acc) ->
                        Tab = TableNameFun(Node),
                        try
                            N = case mnesia:dirty_read(Tab, Key) of
                                    []    -> 0;
                                    [Val] ->
                                        element(CountRecPosition, Val)
                                end,
                            Acc + N
                        catch _:Err  ->
                                rabbit_log:error(
                                  "Failed to fetch number of ~p ~p on node ~p:~n~p~n",
                                  [ContextMsg, Key, Node, Err]),
                                Acc
                        end
                end, 0, rabbit_mnesia:cluster_nodes(running)).

-spec match_tracked_items(function(), tuple()) -> term().

match_tracked_items(TableNameFun, MatchSpec) ->
    lists:foldl(
        fun (Node, Acc) ->
                Tab = TableNameFun(Node),
                Acc ++ mnesia:dirty_match_object(
                         Tab,
                         MatchSpec)
        end, [], rabbit_mnesia:cluster_nodes(running)).

-spec clear_tracking_table(atom()) -> ok.

clear_tracking_table(TableName) ->
    case mnesia:clear_table(TableName) of
        {atomic, ok} -> ok;
        {aborted, _} -> ok
    end.

-spec delete_tracking_table(atom(), node(), string()) -> ok.

delete_tracking_table(TableName, Node, ContextMsg) ->
    case mnesia:delete_table(TableName) of
        {atomic, ok}              -> ok;
        {aborted, {no_exists, _}} -> ok;
        {aborted, Error} ->
            rabbit_log:error("Failed to delete a ~p table for node ~p: ~p",
                [ContextMsg, Node, Error]),
            ok
    end.

-spec delete_tracked_entry({atom(), atom(), list()}, function(), term()) -> ok.

delete_tracked_entry(_ExistsCheckSpec = {M, F, A}, TableNameFun, Key) ->
    ClusterNodes = rabbit_mnesia:cluster_nodes(running),
    ExistsInCluster =
        lists:any(fun(Node) -> rpc:call(Node, M, F, A) end, ClusterNodes),
    case ExistsInCluster of
        false ->
            [mnesia:dirty_delete(TableNameFun(Node), Key) || Node <- ClusterNodes];
        true ->
            ok
    end.

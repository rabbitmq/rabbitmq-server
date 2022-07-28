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

-export([id/2, delete_tracked_entry/4, delete_tracked_entry_internal/4,
         clear_tracking_table/1, delete_tracking_table/3]).
-export([count_tracked_items_ets/3, match_tracked_items_ets/2]).
-export([count_tracked_items_local/2, match_tracked_items_local/2]).
-export([count_tracked_items_mnesia/4, match_tracked_items_mnesia/2]).

%%----------------------------------------------------------------------------

-spec id(atom(), term()) ->
    rabbit_types:tracked_connection_id() | rabbit_types:tracked_channel_id().

id(Node, Name) -> {Node, Name}.

-spec count_tracked_items_ets(atom(), term(), string()) ->
    non_neg_integer().
count_tracked_items_ets(Tab, Key, ContextMsg) ->
    lists:foldl(fun (Node, Acc) when Node == node() ->
                        N = count_tracked_items_local(Tab, Key),
                        Acc + N;
                    (Node, Acc) ->
                        N = case rabbit_misc:rpc_call(Node, ?MODULE, count_tracked_items_local,
                                                      [Tab, Key]) of
                                Int when is_integer(Int) -> Int;
                                {badrpc, Err} ->
                                    rabbit_log:error(
                                      "Failed to fetch number of ~p ~p on node ~p:~n~p",
                                      [ContextMsg, Key, Node, Err]),
                                    0
                            end,
                        Acc + N
                end, 0, rabbit_nodes:all_running()).

count_tracked_items_local(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        []         -> 0;
        [{_, Val}] -> Val
    end.

count_tracked_items_mnesia(TableNameFun, CountRecPosition, Key, ContextMsg) ->
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
                                  "Failed to fetch number of ~p ~p on node ~p:~n~p",
                                  [ContextMsg, Key, Node, Err]),
                                Acc
                        end
                end, 0, rabbit_nodes:all_running()).

-spec match_tracked_items_ets(atom(), tuple()) -> term().
match_tracked_items_ets(Tab, MatchSpec) ->
    lists:foldl(
      fun (Node, Acc) when Node == node() ->
              Acc ++ match_tracked_items_local(Tab, MatchSpec);
          (Node, Acc) ->
              case rabbit_misc:rpc_call(Node, ?MODULE, match_tracked_items_local,
                                        [Tab, MatchSpec]) of
                  List when is_list(List) ->
                      Acc ++ List;
                  _ ->
                      Acc
              end
      end, [], rabbit_nodes:all_running()).

match_tracked_items_local(Tab, MatchSpec) ->
    ets:match_object(Tab, MatchSpec).

match_tracked_items_mnesia(TableNameFun, MatchSpec) ->
    lists:foldl(
        fun (Node, Acc) ->
                Tab = TableNameFun(Node),
                Acc ++ mnesia:dirty_match_object(
                         Tab,
                         MatchSpec)
        end, [], rabbit_nodes:all_running()).

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

-spec delete_tracked_entry({atom(), atom(), list()}, atom(), function(), term()) -> ok.
delete_tracked_entry(_ExistsCheckSpec = {M, F, A}, TableName, TableNameFun, Key) ->
    ClusterNodes = rabbit_nodes:all_running(),
    ExistsInCluster =
        lists:any(fun(Node) -> rpc:call(Node, M, F, A) end, ClusterNodes),
    case ExistsInCluster of
        false ->
            [delete_tracked_entry_internal(Node, TableName, TableNameFun, Key)
             || Node <- ClusterNodes];
        true ->
            ok
    end.

delete_tracked_entry_internal(Node, Tab, TableNameFun, Key) when Node == node() ->
    case rabbit_feature_flags:is_enabled(tracking_records_in_ets) of
        true ->
            true = ets:delete(Tab, Key),
            ok;
        false ->
            mnesia:dirty_delete(TableNameFun(Node), Key),
            ok
    end;
delete_tracked_entry_internal(Node, Tab, TableNameFun, Key) ->
    case rabbit_misc:rpc_call(Node, ?MODULE, delete_tracked_entry_internal, [Node, Tab, TableNameFun, Key]) of
        ok ->
            ok;
        _ ->
            %% Node could be down, but also in a mixed version cluster this function is not
            %% implemented on pre 3.11.x releases. Ensure that we clean up any mnesia table
            mnesia:dirty_delete(TableNameFun(Node), Key)
    end,
    ok.

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_tracking).

%% Common behaviour and processing functions for tracking components
%%
%% See in use:
%%  * rabbit_connection_tracking
%%  * rabbit_channel_tracking

-callback update_tracked(term()) -> ok.
-callback handle_cast(term()) -> ok.
-callback register_tracked(
              rabbit_types:tracked_connection() |
                  rabbit_types:tracked_channel()) -> 'ok'.
-callback unregister_tracked(
              rabbit_types:tracked_connection_id() |
                  rabbit_types:tracked_channel_id()) -> 'ok'.
-callback count_tracked_items_in(term()) -> non_neg_integer().
-callback shutdown_tracked_items(list(), term()) -> ok.

-export([id/2, delete_tracked_entry/4, delete_tracked_entry_internal/4]).
-export([count_on_all_nodes/4, match_tracked_items/2]).
-export([read_ets_counter/2, match_tracked_items_local/2]).

%%----------------------------------------------------------------------------

-spec id(atom(), term()) ->
    rabbit_types:tracked_connection_id() | rabbit_types:tracked_channel_id().

id(Node, Name) -> {Node, Name}.

-spec count_on_all_nodes(module(), atom(), [term()], iodata()) ->
    non_neg_integer().
count_on_all_nodes(Mod, Fun, Args, ContextMsg) ->
    Nodes = rabbit_nodes:list_running(),
    ResL = erpc:multicall(Nodes, Mod, Fun, Args),
    sum_rpc_multicall_result(ResL, Nodes, ContextMsg, 0).

sum_rpc_multicall_result([{ok, Int}|ResL], [_N|Nodes], ContextMsg, Acc) when is_integer(Int) ->
    sum_rpc_multicall_result(ResL, Nodes, ContextMsg, Acc + Int);
sum_rpc_multicall_result([{ok, BadValue}|ResL], [BadNode|Nodes], ContextMsg, Acc) ->
    rabbit_log:error(
      "Failed to fetch number of ~ts on node ~tp:~n not an integer ~tp",
      [ContextMsg, BadNode, BadValue]),
    sum_rpc_multicall_result(ResL, Nodes, ContextMsg, Acc);
sum_rpc_multicall_result([{Class, Reason}|ResL], [BadNode|Nodes], ContextMsg, Acc) ->
    rabbit_log:error(
      "Failed to fetch number of ~ts on node ~tp:~n~tp:~tp",
      [ContextMsg, BadNode, Class, Reason]),
    sum_rpc_multicall_result(ResL, Nodes, ContextMsg, Acc);
sum_rpc_multicall_result([], [], _, Acc) ->
    Acc.

read_ets_counter(Tab, Key) ->
    case ets:lookup(Tab, Key) of
        []         -> 0;
        [{_, Val}] -> Val
    end.

-spec match_tracked_items(atom(), tuple()) -> term().
match_tracked_items(Tab, MatchSpec) ->
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
      end, [], rabbit_nodes:list_running()).

match_tracked_items_local(Tab, MatchSpec) ->
    ets:match_object(Tab, MatchSpec).

-spec delete_tracked_entry({atom(), atom(), list()}, atom(), function(), term()) -> ok.
delete_tracked_entry(_ExistsCheckSpec = {M, F, A}, TableName, TableNameFun, Key) ->
    ClusterNodes = rabbit_nodes:list_running(),
    ExistsInCluster =
        lists:any(fun(Node) -> rpc:call(Node, M, F, A) end, ClusterNodes),
    case ExistsInCluster of
        false ->
            [delete_tracked_entry_internal(Node, TableName, TableNameFun, Key)
             || Node <- ClusterNodes];
        true ->
            ok
    end.

delete_tracked_entry_internal(Node, Tab, _TableNameFun, Key) when Node == node() ->
    true = ets:delete(Tab, Key),
    ok;
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

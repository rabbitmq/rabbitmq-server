%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_maintenance).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         table_definitions/0,
         set/1,
         get/1,
         get_consistent/1
        ]).

-export([
         khepri_maintenance_path/1,
         khepri_maintenance_path/0
        ]).

-define(TABLE, rabbit_node_maintenance_states).

%% -------------------------------------------------------------------
%% table_definitions().
%% -------------------------------------------------------------------

-spec table_definitions() -> [Def] when
      Def :: {Name :: atom(), term()}.

table_definitions() ->
    [{?TABLE, maps:to_list(#{
                             record_name => node_maintenance_state,
                             attributes  => record_info(fields, node_maintenance_state),
                             match => #node_maintenance_state{_ = '_'}
                            })}].

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Status) -> Ret when
      Status :: rabbit_maintenance:maintenance_status(),
      Ret :: boolean().
%% @doc Sets the maintenance status for the local node
%%
%% @private

set(Status) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> set_in_mnesia(Status) end,
        khepri => fun() -> set_in_khepri(Status) end
       }).

set_in_mnesia(Status) ->
    Res = mnesia:transaction(
            fun () ->
                    case mnesia:wread({?TABLE, node()}) of
                        [] ->
                            Row = #node_maintenance_state{
                                     node   = node(),
                                     status = Status
                                    },
                            mnesia:write(?TABLE, Row, write);
                        [Row0] ->
                            Row = Row0#node_maintenance_state{
                                    node   = node(),
                                    status = Status
                                   },
                            mnesia:write(?TABLE, Row, write)
                    end
            end),
    case Res of
        {atomic, ok} -> true;
        _            -> false
    end.

set_in_khepri(Status) ->
    Node = node(),
    Path = khepri_maintenance_path(Node),
    Record = #node_maintenance_state{
                node   = Node,
                status = Status
               },
    case rabbit_khepri:put(Path, Record) of
        ok -> true;
        _ -> false
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(Node) -> Status when
      Node :: node(),
      Status :: undefined | rabbit_maintenance:maintenance_status().
%% @doc Returns the status for the given node using a local query.
%%
%% @returns the status if any, or `undefined'.
%%
%% @private

get(Node) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(Node) end,
        khepri => fun() -> get_in_khepri(Node) end
       }).

get_in_mnesia(Node) ->
    case catch mnesia:dirty_read(?TABLE, Node) of
        []  -> undefined;
        [#node_maintenance_state{node = Node, status = Status}] ->
            Status;
        _   -> undefined
    end.

get_in_khepri(Node) ->
    Path = khepri_maintenance_path(Node),
    case rabbit_khepri:get(Path) of
        {ok, #node_maintenance_state{status = Status}} ->
            Status;
        _ ->
            undefined
    end.

%% -------------------------------------------------------------------
%% get_consistent().
%% -------------------------------------------------------------------

-spec get_consistent(Node) -> Status when
      Node :: node(),
      Status :: undefined | rabbit_maintenance:maintenance_status().
%% @doc Returns the status for the given node using a consistent query.
%%
%% @returns the status if any, or `undefined'.
%%
%% @private

get_consistent(Node) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_consistent_in_mnesia(Node) end,
        khepri => fun() -> get_consistent_in_khepri(Node) end
       }).

get_consistent_in_mnesia(Node) ->
    case mnesia:transaction(fun() -> mnesia:read(?TABLE, Node) end) of
        {atomic, []} -> undefined;
        {atomic, [#node_maintenance_state{node = Node, status = Status}]} ->
            Status;
        {atomic, _}  -> undefined;
        {aborted, _Reason} -> undefined
    end.

get_consistent_in_khepri(Node) ->
    Path = khepri_maintenance_path(Node),
    case rabbit_khepri:get(Path, #{favor => consistency}) of
        {ok, #node_maintenance_state{status = Status}} ->
            Status;
        _ ->
            undefined
    end.

%% -------------------------------------------------------------------
%% Khepri paths
%% -------------------------------------------------------------------

khepri_maintenance_path() ->
    [?MODULE, maintenance].

khepri_maintenance_path(Node) ->
    [?MODULE, maintenance, Node].

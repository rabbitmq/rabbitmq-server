%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_maintenance).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

-export([
         set/1,
         get/1,
         get_consistent/1
        ]).

-export([
         khepri_maintenance_path/1
        ]).

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
    Path = khepri_maintenance_path(Node),
    Options = #{favor => consistency},
    case rabbit_khepri:get(Path, Options) of
        {ok, #node_maintenance_state{status = Status}} ->
            Status;
        _ ->
            undefined
    end.

%% -------------------------------------------------------------------
%% Khepri paths
%% -------------------------------------------------------------------

khepri_maintenance_path(Node) when ?IS_KHEPRI_PATH_CONDITION(Node) ->
    ?RABBITMQ_KHEPRI_MAINTENANCE_PATH(Node).

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_maintenance).

-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         setup_schema/0,
         set/1,
         get/1,
         get_consistent/1
        ]).

-type mnesia_table() :: atom().

-define(TABLE, rabbit_node_maintenance_states).

%% -------------------------------------------------------------------
%% setup_schema().
%% -------------------------------------------------------------------

-spec setup_schema() -> ok.
%% @doc Creates the internal schema used by the selected metadata store
%%
%% @private

setup_schema() ->
    rabbit_db:run(
      #{mnesia => fun() -> setup_schema_in_mnesia() end
       }).

setup_schema_in_mnesia() ->
    TableName = status_table_name(),
    rabbit_log:info(
      "Creating table ~ts for maintenance mode status",
      [TableName]),
    try
        _ = rabbit_table:create(
              TableName,
              status_table_definition())
    catch throw:Reason  ->
            rabbit_log:error(
              "Failed to create maintenance status table: ~tp",
              [Reason])
    end.

-spec status_table_name() -> mnesia_table().
status_table_name() ->
    ?TABLE.

-spec status_table_definition() -> list().
status_table_definition() ->
    maps:to_list(#{
        record_name => node_maintenance_state,
        attributes  => record_info(fields, node_maintenance_state)
    }).

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
    rabbit_db:run(
      #{mnesia => fun() -> set_in_mnesia(Status) end
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
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Node) end
       }).

get_in_mnesia(Node) ->
    case catch mnesia:dirty_read(?TABLE, Node) of
        []  -> undefined;
        [#node_maintenance_state{node = Node, status = Status}] ->
            Status;
        _   -> undefined
    end.

%% -------------------------------------------------------------------
%% get().
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
    rabbit_db:run(
      #{mnesia => fun() -> get_consistent_in_mnesia(Node) end
       }).

get_consistent_in_mnesia(Node) ->
    case mnesia:transaction(fun() -> mnesia:read(?TABLE, Node) end) of
        {atomic, []} -> undefined;
        {atomic, [#node_maintenance_state{node = Node, status = Status}]} ->
            Status;
        {atomic, _}  -> undefined;
        {aborted, _Reason} -> undefined
    end.

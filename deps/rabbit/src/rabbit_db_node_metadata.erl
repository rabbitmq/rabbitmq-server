%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_node_metadata).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-include("include/rabbit_khepri.hrl").
-include("include/node_metadata.hrl").

-export([
         set/2,
         get/1,
         delete/1
        ]).

-export([
         khepri_node_metadata_path/1
        ]).

-define(KHEPRI_PROJECTION, rabbit_khepri_node_metadata).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(node(), node_metadata_map()) -> ok | {error, any()}.
%% @doc Sets the node metadata map for the given node.
set(Node, Metadata) when is_map(Metadata) ->
    Path = khepri_node_metadata_path(Node),
    Record = #node_metadata{node = Node, metadata = Metadata},
    case rabbit_khepri:put(Path, Record) of
        ok = Ok ->
            Ok;
        {error, _} = Error ->
            ?LOG_WARNING("Failed to store node metadata for ~ts in Khepri: ~tp",
                         [Node, Error],
                         #{domain => ?RMQLOG_DOMAIN_DB}),
            Error
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(node()) -> node_metadata_map().
%% @doc Returns the node metadata map for the given node using the local ETS projection.
get(Node) ->
    try
        case ets:lookup(?KHEPRI_PROJECTION, Node) of
            [#node_metadata{metadata = Metadata}] ->
                Metadata;
            _ ->
                #{}
        end
    catch
        error:badarg ->
            #{}
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(node()) -> ok | {error, any()}.
%% @doc Deletes the node metadata for the given node.
delete(Node) ->
    Path = khepri_node_metadata_path(Node),
    rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% Khepri paths
%% -------------------------------------------------------------------

khepri_node_metadata_path(Node) when ?IS_KHEPRI_PATH_CONDITION(Node) ->
    ?RABBITMQ_KHEPRI_NODE_METADATA_PATH(Node).

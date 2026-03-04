%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_nodes).

-export([
    node_name_from_req/1,
    node_exists/1
]).

%%
%% API
%%

-spec node_name_from_req(cowboy_req:req()) ->
    {ok, node()} | {error, not_a_cluster_member}.
node_name_from_req(ReqData) ->
    try binary_to_existing_atom(rabbit_mgmt_util:id(node, ReqData), utf8) of
        Node ->
            case rabbit_nodes:is_member(Node) of
                true  -> {ok, Node};
                false -> {error, not_a_cluster_member}
            end
    catch
        error:badarg -> {error, not_a_cluster_member}
    end.

-spec node_exists(cowboy_req:req()) -> boolean().
node_exists(ReqData) ->
    case node_name_from_req(ReqData) of
        {ok, _}    -> true;
        {error, _} -> false
    end.

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_nodes).

-export([
    node_name_from_req/1,
    node_exists/1,
    parse_node_name/1,
    require_node_name/1,
    safe_atom/2
]).

%%
%% API
%%

-spec node_name_from_req(cowboy_req:req()) ->
    {ok, node()} | {error, not_a_cluster_member}.
node_name_from_req(ReqData) ->
    parse_node_name(rabbit_mgmt_util:id(node, ReqData)).

%% Validates a node name given as a binary, list (string), or atom.
%% Returns {ok, Node} if the atom exists and the node is a cluster member.
-spec parse_node_name(binary() | atom() | string()) ->
    {ok, node()} | {error, not_a_cluster_member}.
parse_node_name(Node) when is_atom(Node) ->
    case rabbit_nodes:is_member(Node) of
        true  -> {ok, Node};
        false -> {error, not_a_cluster_member}
    end;
parse_node_name(NodeBin) when is_binary(NodeBin) ->
    try binary_to_existing_atom(NodeBin, utf8) of
        Node -> parse_node_name(Node)
    catch
        error:badarg -> {error, not_a_cluster_member}
    end;
parse_node_name(NodeList) when is_list(NodeList) ->
    try list_to_existing_atom(NodeList) of
        Node -> parse_node_name(Node)
    catch
        error:badarg -> {error, not_a_cluster_member}
    end.

-spec node_exists(cowboy_req:req()) -> boolean().
node_exists(ReqData) ->
    case node_name_from_req(ReqData) of
        {ok, _}    -> true;
        {error, _} -> false
    end.

-spec require_node_name(binary() | atom() | string()) -> node().
require_node_name(Val) ->
    case parse_node_name(Val) of
        {ok, Node} -> Node;
        {error, _} -> throw({error, <<"Node is not a cluster member">>})
    end.

-spec safe_atom(atom() | binary() | string(), binary()) -> atom().
safe_atom(Val, _Label) when is_atom(Val) ->
    Val;
safe_atom(Val, Label) ->
    try rabbit_data_coercion:to_existing_atom(Val)
    catch error:badarg ->
            throw({error, <<"Invalid ", Label/binary, " value">>})
    end.

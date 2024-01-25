%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_nodes).

-export([
    node_name_from_req/1,
    node_exists/1
]).

%%
%% API
%%

node_name_from_req(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).

%% To be used in resource_exists/2
node_exists(ReqData) ->
    Node = node_name_from_req(ReqData),
    AllNodes = rabbit_nodes:list_members(),
    lists:member(Node, AllNodes).
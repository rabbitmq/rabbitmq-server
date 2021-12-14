%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This health check module is deprecated. DO NOT USE.
%% See https://www.rabbitmq.com/monitoring.html#health-checks for modern alternatives.
-module(rabbit_health_check).

-export([node/1, node/2]).
-export([local/0]).

%%
%% API
%%

node(Node) ->
    %% same default as in CLI
    node(Node, 70000).

-spec node(node(), timeout()) -> ok | {badrpc, term()} | {error_string, string()}.

node(Node, Timeout) ->
    rabbit_misc:rpc_call(Node, rabbit_health_check, local, [], Timeout).

-spec local() -> ok | {error_string, string()}.

local() ->
    rabbit_log:warning("rabbitmqctl node_health_check and its HTTP API counterpart are DEPRECATED and no longer perform any checks. "
                       "See https://www.rabbitmq.com/monitoring.html#health-checks for modern alternatives"),
    ok.

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(rabbit_health_check).

%% External API
-export([node/1, node/2]).

%% Internal API
-export([local/0]).

%%----------------------------------------------------------------------------
%% External functions
%%----------------------------------------------------------------------------

node(Node) ->
    %% same default as in CLI
    node(Node, 70000).

-spec node(node(), timeout()) -> ok | {badrpc, term()} | {error_string, string()}.

node(Node, Timeout) ->
    rabbit_misc:rpc_call(Node, rabbit_health_check, local, [], Timeout).

-spec local() -> ok | {error_string, string()}.

local() ->
    rabbit_log:warning("rabbitmqctl node_health_check and its HTTP API counterpart are DEPRECATED. "
                       "See https://www.rabbitmq.com/monitoring.html#health-checks for replacement options."),
    run_checks([list_channels, list_queues, alarms, rabbit_node_monitor]).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
run_checks([]) ->
    ok;
run_checks([C|Cs]) ->
    case node_health_check(C) of
        ok ->
            run_checks(Cs);
        Error ->
            Error
    end.

node_health_check(list_channels) ->
    case rabbit_channel:info_local([pid]) of
        L when is_list(L) ->
            ok
    end;

node_health_check(list_queues) ->
    health_check_queues(rabbit_vhost:list_names());

node_health_check(rabbit_node_monitor) ->
    case rabbit_node_monitor:partitions() of
        [] ->
            ok;
        L when is_list(L), length(L) > 0 ->
            ErrorMsg = io_lib:format("cluster partition in effect: ~p", [L]),
            {error_string, ErrorMsg}
    end;

node_health_check(alarms) ->
    % Note:
    % Removed call to rabbit:status/0 here due to a memory leak on win32,
    % plus it uses an excessive amount of resources
    % Alternative to https://github.com/rabbitmq/rabbitmq-server/pull/3893
    case rabbit:alarms() of
        [] ->
            ok;
        Alarms ->
            ErrorMsg = io_lib:format("resource alarm(s) in effect:~p", [Alarms]),
            {error_string, ErrorMsg}
    end.

health_check_queues([]) ->
    ok;
health_check_queues([VHost|RestVHosts]) ->
    case rabbit_amqqueue:info_local(VHost) of
        L when is_list(L) ->
            health_check_queues(RestVHosts)
    end.

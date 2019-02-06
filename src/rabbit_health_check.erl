%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_health_check).

%% External API
-export([node/1, node/2]).

%% Internal API
-export([local/0]).

%%----------------------------------------------------------------------------
%% External functions
%%----------------------------------------------------------------------------

-spec node(node(), timeout()) -> ok | {badrpc, term()} | {error_string, string()}.

node(Node) ->
    %% same default as in CLI
    node(Node, 70000).
node(Node, Timeout) ->
    rabbit_misc:rpc_call(Node, rabbit_health_check, local, [], Timeout).

-spec local() -> ok | {error_string, string()}.

local() ->
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
    health_check_queues(rabbit_vhost:list());

node_health_check(rabbit_node_monitor) ->
    case rabbit_node_monitor:partitions() of
        L when is_list(L) ->
            ok
    end;

node_health_check(alarms) ->
    case proplists:get_value(alarms, rabbit:status()) of
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

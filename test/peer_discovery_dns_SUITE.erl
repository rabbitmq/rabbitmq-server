%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(peer_discovery_dns_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
     {group, non_parallel}
    ].

groups() ->
    [
      {non_parallel, [], [
                          hostname_discovery_with_long_node_names,
                          hostname_discovery_with_short_node_names,
                          node_discovery_with_long_node_names,
                          node_discovery_with_short_node_names
                         ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 1}}
    ].


%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

%% peer_discovery.tests.rabbitmq.net used in the tests below
%% returns three A records two of which fail our resolution process:
%%
%% * One does not resolve to a [typically] non-reachable IP
%% * One does not support reverse lookup queries

-define(DISCOVERY_ENDPOINT, "peer_discovery.tests.rabbitmq.net").

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_testcase(_Testcase, Config) ->
    %% TODO: support IPv6-only environments
    case inet_res:lookup(?DISCOVERY_ENDPOINT, in, a) of
        []      ->
            {skip, "pre-configured *.rabbitmq.net record does not resolve, skipping"};
        [_ | _] ->
            Config
    end.

end_per_testcase(_Testcase, Config) ->
    case inet_res:lookup(?DISCOVERY_ENDPOINT, in, a) of
        []      ->
            {skip, "pre-configured *.rabbitmq.net record does not resolve, skipping"};
        [_ | _] ->
            Config
    end.


%% -------------------------------------------------------------------
%% Test cases
%% -------------------------------------------------------------------

hostname_discovery_with_long_node_names(_) ->
    Result = rabbit_peer_discovery_dns:discover_hostnames(?DISCOVERY_ENDPOINT, true),
    ?assertEqual(["www.rabbitmq.com"], Result).

hostname_discovery_with_short_node_names(_) ->
    Result = rabbit_peer_discovery_dns:discover_hostnames(?DISCOVERY_ENDPOINT, false),
    ?assertEqual(["www"], Result).

node_discovery_with_long_node_names(_) ->
    Result = rabbit_peer_discovery_dns:discover_nodes(?DISCOVERY_ENDPOINT, true),
    ?assertEqual(['ct_rabbit@www.rabbitmq.com'], Result).

node_discovery_with_short_node_names(_) ->
    Result = rabbit_peer_discovery_dns:discover_nodes(?DISCOVERY_ENDPOINT, false),
    ?assertEqual([ct_rabbit@www], Result).

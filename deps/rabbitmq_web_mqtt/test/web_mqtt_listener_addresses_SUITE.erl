%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(web_mqtt_listener_addresses_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
      every_configured_address_is_bound,
      every_configured_address_is_reported
    ].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_nodes_count, 1}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

%% On a dual stack host a listener configured with a port and no interface
%% expands to both wildcard addresses, and the plugin has to bind each one.
every_configured_address_is_bound(Config) ->
    Port = listener_port(Config),
    Addresses = configured_addresses(Config, Port),
    ?assertNotEqual([], Addresses),
    Bound = [IPAddress || IPAddress <- Addresses,
                          address_is_bound(Config, IPAddress, Port)],
    ?assertEqual(Addresses, Bound).

every_configured_address_is_reported(Config) ->
    Port = listener_port(Config),
    ?assertEqual(configured_addresses(Config, Port),
                 reported_addresses(Config, Port)).

%% -------------------------------------------------------------------
%% Helpers running on the broker node.
%% -------------------------------------------------------------------

listener_addresses_on_port(Port) ->
    lists:sort(rabbit_networking:listener_ip_addresses([{port, Port}])).

reported_addresses_on_port(Port) ->
    lists:sort([IPAddress
                || #listener{ip_address = IPAddress, port = ListenerPort}
                       <- rabbit_networking:node_listeners(node()),
                   ListenerPort =:= Port]).

%% A second bind of an address that already has a listening socket fails with
%% `eaddrinuse`, which works for wildcard addresses where connecting does not.
address_is_bound_on_port(IPAddress, Port) ->
    Family = case tuple_size(IPAddress) of
                 4 -> inet;
                 8 -> inet6
             end,
    case gen_tcp:listen(Port, [Family, {ip, IPAddress}]) of
        {error, eaddrinuse} ->
            true;
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            false
    end.

%% -------------------------------------------------------------------
%% Helpers running on the CT node.
%% -------------------------------------------------------------------

listener_port(Config) ->
    rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt).

configured_addresses(Config, Port) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ?MODULE, listener_addresses_on_port, [Port]).

reported_addresses(Config, Port) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ?MODULE, reported_addresses_on_port, [Port]).

address_is_bound(Config, IPAddress, Port) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, ?MODULE, address_is_bound_on_port, [IPAddress, Port]).

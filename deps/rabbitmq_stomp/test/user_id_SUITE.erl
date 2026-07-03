%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

%% Integration tests for the user_id header on the STOMP publish path.
-module(user_id_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").

-define(DESTINATION, <<"/topic/user-id-test">>).

all() ->
    [{group, list_to_atom("version_" ++ V)} || V <- ?SUPPORTED_VERSIONS].

groups() ->
    Tests = [
        scenario_a,
        scenario_b,
        scenario_c,
        scenario_d,
        scenario_e
    ],
    [{list_to_atom("version_" ++ V), [sequence], Tests} || V <- ?SUPPORTED_VERSIONS].

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    Version = string:sub_string(atom_to_list(Group), 9),
    rabbit_ct_helpers:set_config(Config, [{version, Version}]).

end_per_group(_Group, Config) -> Config.

init_per_testcase(_TestCase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user,
                                 [<<"user">>, <<"pass">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_permissions, [
        <<"user">>, <<"/">>, <<".*">>, <<".*">>, <<".*">>, <<"acting-user">>]),
    Config.

end_per_testcase(_TestCase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"user">>, <<"acting-user">>]),
    Config.

scenario_a(Config) ->
    with_client(Config, fun(Client) ->
        subscribe(Client),
        send(Client, [{<<"user-id">>, <<"user">>}]),
        {ok, _Client1, Hdrs, Body} = stomp_receive(Client, 'MESSAGE'),
        [<<"hello">>] = Body,
        <<"user">> = maps:get(<<"user-id">>, Hdrs)
    end).

scenario_b(Config) ->
    with_client(Config, fun(Client) ->
        subscribe(Client),
        send(Client, []),
        {ok, _Client1, _Hdrs, Body} = stomp_receive(Client, 'MESSAGE'),
        [<<"hello">>] = Body
    end).

scenario_c(Config) ->
    with_client(Config, fun(Client) ->
        subscribe(Client),
        send(Client, [{<<"user-id">>, <<"someone-else">>}]),
        {ok, _Client1, Hdrs, _Body} = stomp_receive(Client, 'ERROR'),
        <<"access_refused">> = maps:get(<<"message">>, Hdrs)
    end).

scenario_d(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_tags,
                                 [<<"user">>, [impersonator], <<"acting-user">>]),
    with_client(Config, fun(Client) ->
        subscribe(Client),
        send(Client, [{<<"user-id">>, <<"someone-else">>}]),
        {ok, _Client1, Hdrs, Body} = stomp_receive(Client, 'MESSAGE'),
        [<<"hello">>] = Body,
        <<"someone-else">> = maps:get(<<"user-id">>, Hdrs)
    end).

%% A transactional SEND defers the publish to COMMIT, so the check must still
%% reject a forged user-id then.
scenario_e(Config) ->
    with_client(Config, fun(Client) ->
        Tx = <<"tx1">>,
        rabbit_stomp_client:send(Client, 'BEGIN', [{<<"transaction">>, Tx}]),
        rabbit_stomp_client:send(
          Client, 'SEND', [{<<"destination">>, ?DESTINATION},
                           {<<"transaction">>, Tx},
                           {<<"user-id">>, <<"someone-else">>}], ["hello"]),
        rabbit_stomp_client:send(Client, 'COMMIT', [{<<"transaction">>, Tx}]),
        {ok, _Client1, Hdrs, _Body} = stomp_receive(Client, 'ERROR'),
        <<"access_refused">> = maps:get(<<"message">>, Hdrs)
    end).

with_client(Config, Fun) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Client} = rabbit_stomp_client:connect(Version, "user", "pass", StompPort),
    try
        Fun(Client)
    after
        rabbit_stomp_client:disconnect(Client)
    end.

subscribe(Client) ->
    rabbit_stomp_client:send(
      Client, 'SUBSCRIBE', [{<<"destination">>, ?DESTINATION},
                            {<<"id">>, <<"s0">>},
                            {<<"durable">>, <<"true">>}]).

send(Client, ExtraHeaders) ->
    rabbit_stomp_client:send(
      Client, 'SEND', [{<<"destination">>, ?DESTINATION} | ExtraHeaders], ["hello"]).

stomp_receive(Client, Command) ->
    {#stomp_frame{command         = Command,
                  headers         = Hdrs,
                  body_iolist_rev = Body}, Client1} =
        rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.

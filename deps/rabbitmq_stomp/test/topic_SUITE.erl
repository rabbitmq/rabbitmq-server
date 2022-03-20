%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(topic_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

all() ->
    [{group, list_to_atom("version_" ++ V)} || V <- ?SUPPORTED_VERSIONS].

groups() ->
    Tests = [
        publish_topic_authorisation,
        subscribe_topic_authorisation,
        change_default_topic_exchange
    ],

    [{list_to_atom("version_" ++ V), [sequence], Tests}
     || V <- ?SUPPORTED_VERSIONS].

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
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{
        node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)
    }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {amqp_connection, Connection},
        {amqp_channel, Channel},
        {stomp_client, Client}
      ]),
    init_per_testcase0(Config1).

end_per_testcase(_TestCase, Config) ->
    Connection = ?config(amqp_connection, Config),
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:disconnect(Client),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    end_per_testcase0(Config).

init_per_testcase0(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user,
                                 [<<"user">>, <<"pass">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_permissions, [
        <<"user">>, <<"/">>, <<".*">>, <<".*">>, <<".*">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_topic_permissions, [
        <<"user">>, <<"/">>, <<"amq.topic">>, <<"^{username}.Authorised">>, <<"^{username}.Authorised">>, <<"acting-user">>]),
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, ClientFoo} = rabbit_stomp_client:connect(Version, "user", "pass", StompPort),
    rabbit_ct_helpers:set_config(Config, [{client_foo, ClientFoo}]).

end_per_testcase0(Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:disconnect(ClientFoo),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"user">>, <<"acting-user">>]),
    Config.

publish_topic_authorisation(Config) ->
    ClientFoo = ?config(client_foo, Config),

    AuthorisedTopic = "/topic/user.AuthorisedTopic",
    RestrictedTopic = "/topic/user.RestrictedTopic",

    %% send on authorised topic
    rabbit_stomp_client:send(
        ClientFoo, "SUBSCRIBE", [{"destination", AuthorisedTopic}]),

    rabbit_stomp_client:send(
        ClientFoo, "SEND", [{"destination", AuthorisedTopic}], ["authorised hello"]),

    {ok, _Client1, _, Body} = stomp_receive(ClientFoo, "MESSAGE"),
    [<<"authorised hello">>] = Body,

    %% send on restricted topic
    rabbit_stomp_client:send(
      ClientFoo, "SEND", [{"destination", RestrictedTopic}], ["hello"]),
    {ok, _Client2, Hdrs2, _} = stomp_receive(ClientFoo, "ERROR"),
    "access_refused" = proplists:get_value("message", Hdrs2),
    ok.

subscribe_topic_authorisation(Config) ->
    ClientFoo = ?config(client_foo, Config),

    AuthorisedTopic = "/topic/user.AuthorisedTopic",
    RestrictedTopic = "/topic/user.RestrictedTopic",

    %% subscribe to authorised topic
    rabbit_stomp_client:send(
        ClientFoo, "SUBSCRIBE", [{"destination", AuthorisedTopic}]),

    rabbit_stomp_client:send(
        ClientFoo, "SEND", [{"destination", AuthorisedTopic}], ["authorised hello"]),

    {ok, _Client1, _, Body} = stomp_receive(ClientFoo, "MESSAGE"),
    [<<"authorised hello">>] = Body,

    %% subscribe to restricted topic
    rabbit_stomp_client:send(
        ClientFoo, "SUBSCRIBE", [{"destination", RestrictedTopic}]),
    {ok, _Client2, Hdrs2, _} = stomp_receive(ClientFoo, "ERROR"),
    "access_refused" = proplists:get_value("message", Hdrs2),
    ok.

change_default_topic_exchange(Config) ->
    Channel = ?config(amqp_channel, Config),
    ClientFoo = ?config(client_foo, Config),
    Ex = <<"my-topic-exchange">>,
    AuthorisedTopic = "/topic/user.AuthorisedTopic",

    Declare = #'exchange.declare'{exchange = Ex, type = <<"topic">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Declare),

    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env, [rabbitmq_stomp, default_topic_exchange, Ex]),

    rabbit_stomp_client:send(
        ClientFoo, "SUBSCRIBE", [{"destination", AuthorisedTopic}]),

    rabbit_stomp_client:send(
        ClientFoo, "SEND", [{"destination", AuthorisedTopic}], ["ohai there"]),

    {ok, _Client1, _, Body} = stomp_receive(ClientFoo, "MESSAGE"),
    [<<"ohai there">>] = Body,

    Delete = #'exchange.delete'{exchange = Ex},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, Delete),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, application, unset_env, [rabbitmq_stomp, default_topic_exchange]),
    ok.


stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
        headers     = Hdrs,
        body_iolist = Body},   Client1} =
        rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.


%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(unix_domain_socket_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

all() ->
    [test_unix_domain_socket,
     test_uds_publish_consume,
     test_uds_connection_failure,
     test_uds_is_loopback_on_raw_socket,
     test_uds_guest_with_loopback_users,
     test_uds_internal_loopback_backend,
     test_uds_auth_attempt_source_tracking,
     test_uds_ntoa_local_atom,
     test_uds_tcp_listener_addresses].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(
      Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Name = atom_to_list(Testcase) ++ ".sock",
    SocketPath = uds_socket_path(Config, Name),
    [{uds_socket_path, SocketPath} | Config].

end_per_testcase(Testcase, Config) ->
    case ?config(uds_socket_path, Config) of
        undefined -> ok;
        SocketPath ->
            _ = rabbit_ct_broker_helpers:rpc(
                  Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
            _ = file:delete(SocketPath)
    end,
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

uds_socket_path(Config, Name) ->
    PrivDir = ?config(priv_dir, Config),
    Path = filename:join(PrivDir, Name),
    case length(Path) > 104 of
        true ->
            UniqueId = integer_to_list(erlang:unique_integer([positive])),
            filename:join("/tmp", "rmq-uds-" ++ UniqueId ++ "-" ++ Name);
        false ->
            Path
    end.

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

test_unix_domain_socket(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),

    Queue = <<"test_uds_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Channel, #'queue.declare'{queue = Queue, durable = true}),

    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
    file:delete(SocketPath),
    ok.

test_uds_publish_consume(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),

    Queue = <<"test_uds_pubsub_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Channel, #'queue.declare'{queue = Queue, durable = true}),

    Payload = <<"Hello over Unix Sockets">>,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Queue},
    amqp_channel:cast(Channel, Publish, #amqp_msg{payload = Payload}),

    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Queue, no_ack = true}),
    receive
        {#'basic.deliver'{consumer_tag = CTag}, #amqp_msg{payload = MsgPayload}} ->
            Payload = MsgPayload
    after 5000 ->
        ct:fail(message_not_received)
    end,

    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, stop_tcp_listener, [SocketPath]),
    file:delete(SocketPath),
    ok.

test_uds_connection_failure(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    ?assertMatch({error, _}, amqp_connection:start(ConnParams)).

%% Verifies finding #1 at the unit level: is_loopback must return true for the
%% {local, _} tuple form that peername returns on an accepted UDS socket.
test_uds_is_loopback_on_raw_socket(Config) ->
    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(
                         Config, rabbit_net, is_loopback, [{local, <<>>}])),
    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(
                         Config, rabbit_net, is_loopback, [{local, <<"/tmp/x.sock">>}])).

%% Verifies finding #1 end-to-end: guest must be able to connect over UDS when
%% loopback_users includes guest (the production default).
test_uds_guest_with_loopback_users(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, loopback_users, [<<"guest">>]]),
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),
        ConnParams = #amqp_params_network{
            host = {local, SocketPath}, port = 0,
            username = <<"guest">>, password = <<"guest">>},
        {ok, Conn} = amqp_connection:start(ConnParams),
        {ok, Channel} = amqp_connection:open_channel(Conn),
        ok = amqp_channel:close(Channel),
        ok = amqp_connection:close(Conn)
    after
        rabbit_ct_broker_helpers:rpc(
          Config, application, set_env, [rabbit, loopback_users, []])
    end.

%% Verifies finding #1 with the internal_loopback backend: guest must be
%% accepted when connecting over UDS, since UDS is local.
test_uds_internal_loopback_backend(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0,
           "rabbitmq_auth_backend_internal_loopback"),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, loopback_users, [<<"guest">>]]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, auth_backends, [rabbit_auth_backend_internal_loopback]]),
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),
        ConnParams = #amqp_params_network{
            host = {local, SocketPath}, port = 0,
            username = <<"guest">>, password = <<"guest">>},
        {ok, Conn} = amqp_connection:start(ConnParams),
        {ok, Channel} = amqp_connection:open_channel(Conn),
        ok = amqp_channel:close(Channel),
        ok = amqp_connection:close(Conn)
    after
        rabbit_ct_broker_helpers:rpc(
          Config, application, set_env,
          [rabbit, auth_backends, [rabbit_auth_backend_internal]]),
        rabbit_ct_broker_helpers:rpc(
          Config, application, set_env, [rabbit, loopback_users, []])
    end.

%% Verifies finding #2: a UDS connection must not crash when
%% track_auth_attempt_source is enabled.
test_uds_auth_attempt_source_tracking(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, track_auth_attempt_source, true]),
    try
        ok = rabbit_ct_broker_helpers:rpc(
               Config, rabbit_networking, start_tcp_listener, [SocketPath, 10]),
        ConnParams = #amqp_params_network{
            host = {local, SocketPath}, port = 0,
            username = <<"guest">>, password = <<"guest">>},
        {ok, Conn} = amqp_connection:start(ConnParams),
        {ok, Channel} = amqp_connection:open_channel(Conn),
        ok = amqp_channel:close(Channel),
        ok = amqp_connection:close(Conn)
    after
        rabbit_ct_broker_helpers:rpc(
          Config, application, set_env,
          [rabbit, track_auth_attempt_source, false])
    end.

%% Verifies finding #3: rabbit_misc:ntoa must handle the bare atom 'local'
%% (which arrives as PeerAddr via get_authz_data_from({socket, _}) on a UDS
%% connection) without crashing. The HTTP auth backend passes the result to
%% quote_plus, so it must be a string.
test_uds_ntoa_local_atom(Config) ->
    Result = rabbit_ct_broker_helpers:rpc(
               Config, rabbit_misc, ntoa, [local]),
    ?assert(is_list(Result)).

%% Verifies finding #4: tcp_listener_addresses must handle string paths that do
%% not start with / or . (relative paths accepted by the schema's string
%% datatype) without crashing.
test_uds_tcp_listener_addresses(Config) ->
    Result = rabbit_ct_broker_helpers:rpc(
               Config, rabbit_networking, tcp_listener_addresses, ["myapp.sock"]),
    ?assertMatch([{{local, "myapp.sock"}, 0, local}], Result).

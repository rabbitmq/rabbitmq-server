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
    [{group, with_listener},
     {group, without_listener}].

groups() ->
    [{with_listener, [],
      [test_unix_domain_socket,
       test_uds_publish_consume,
       test_uds_guest_with_loopback_users,
       test_uds_internal_loopback_backend,
       test_uds_auth_attempt_source_tracking]},
     {without_listener, [],
      [test_uds_connection_failure,
       test_uds_is_loopback_on_raw_socket,
       test_uds_ntoa_local_path,
       test_uds_tcp_listener_addresses]}].

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

%% The with_listener group owns the lifecycle of a single UDS listener: it is
%% started once here and stopped in end_per_group, so no test body or teardown
%% has to reason about whether a listener exists.
init_per_group(with_listener, Config) ->
    SocketPath = uds_socket_path(Config, "with_listener.sock"),
    _ = file:delete(SocketPath),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, start_tcp_listener,
           [{local, SocketPath, 0}, 10]),
    [{uds_socket_path, SocketPath} | Config];
init_per_group(without_listener, Config) ->
    Config.

end_per_group(with_listener, Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, rabbit_networking, stop_tcp_listener,
           [{local, SocketPath, 0}]),
    _ = file:delete(SocketPath),
    Config;
end_per_group(without_listener, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = ensure_uds_socket_path(Testcase, Config),
    ok = set_test_env(Testcase, Config1),
    Config1.

end_per_testcase(Testcase, Config) ->
    ok = unset_test_env(Testcase, Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase),
    Config.

%% The with_listener group already provides a socket path; the without_listener
%% tests that need one get a per-testcase path here.
ensure_uds_socket_path(Testcase, Config) ->
    case ?config(uds_socket_path, Config) of
        undefined ->
            Name = atom_to_list(Testcase) ++ ".sock",
            [{uds_socket_path, uds_socket_path(Config, Name)} | Config];
        _ ->
            Config
    end.

%% Per-testcase application environment, applied before the test body and
%% reverted in unset_test_env/2.
set_test_env(test_uds_guest_with_loopback_users, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, loopback_users, [<<"guest">>]]);
set_test_env(test_uds_internal_loopback_backend, Config) ->
    ok = rabbit_ct_broker_helpers:enable_plugin(
           Config, 0, "rabbitmq_auth_backend_internal_loopback"),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, loopback_users, [<<"guest">>]]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, auth_backends, [rabbit_auth_backend_internal_loopback]]);
set_test_env(test_uds_auth_attempt_source_tracking, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, track_auth_attempt_source, true]);
set_test_env(_Testcase, _Config) ->
    ok.

unset_test_env(test_uds_guest_with_loopback_users, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env, [rabbit, loopback_users, []]);
unset_test_env(test_uds_internal_loopback_backend, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, auth_backends, [rabbit_auth_backend_internal]]),
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env, [rabbit, loopback_users, []]);
unset_test_env(test_uds_auth_attempt_source_tracking, Config) ->
    ok = rabbit_ct_broker_helpers:rpc(
           Config, application, set_env,
           [rabbit, track_auth_attempt_source, false]);
unset_test_env(_Testcase, _Config) ->
    ok.

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
    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),

    Queue = <<"test_uds_queue">>,
    #'queue.declare_ok'{} = amqp_channel:call(
                              Channel, #'queue.declare'{queue = Queue, durable = true}),

    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn).

test_uds_publish_consume(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
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
    ok = amqp_connection:close(Conn).

test_uds_connection_failure(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),

    ConnParams = #amqp_params_network{host = {local, SocketPath}, port = 0},
    ?assertMatch({error, enoent}, amqp_connection:start(ConnParams)).

%% Verifies finding #1 at the unit level. is_loopback must return true both for
%% the {local, _} tuple and for a real accepted UDS socket, whose peername is
%% {ok, {local, <<>>}}. The socket case is the one that was broken: the
%% socket-level clause must preserve the {local, _} tuple rather than
%% destructure it into the bare atom 'local'.
test_uds_is_loopback_on_raw_socket(Config) ->
    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(
                         Config, rabbit_net, is_loopback, [{local, <<>>}])),
    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(
                         Config, rabbit_net, is_loopback, [{local, <<"/tmp/x.sock">>}])),
    SocketPath = ?config(uds_socket_path, Config),
    file:delete(SocketPath),
    ?assertEqual(true, rabbit_ct_broker_helpers:rpc(
                         Config, ?MODULE, is_loopback_on_accepted_uds, [SocketPath])).

%% Verifies finding #1 end-to-end: guest must be able to connect over UDS when
%% loopback_users includes guest (the production default).
test_uds_guest_with_loopback_users(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    ConnParams = #amqp_params_network{
        host = {local, SocketPath}, port = 0,
        username = <<"guest">>, password = <<"guest">>},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn).

%% Verifies finding #1 with the internal_loopback backend: guest must be
%% accepted when connecting over UDS, since UDS is local.
test_uds_internal_loopback_backend(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    ConnParams = #amqp_params_network{
        host = {local, SocketPath}, port = 0,
        username = <<"guest">>, password = <<"guest">>},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn).

%% Verifies finding #2: a UDS connection must not crash when
%% track_auth_attempt_source is enabled.
test_uds_auth_attempt_source_tracking(Config) ->
    SocketPath = ?config(uds_socket_path, Config),
    ConnParams = #amqp_params_network{
        host = {local, SocketPath}, port = 0,
        username = <<"guest">>, password = <<"guest">>},
    {ok, Conn} = amqp_connection:start(ConnParams),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Conn).

%% Verifies finding #3: rabbit_misc:ntoa and ntoab must format a {local, Path}
%% address (the peeraddr shape a UDS connection now yields) as the path, since
%% the HTTP auth backend passes ntoa's result to quote_plus.
test_uds_ntoa_local_path(Config) ->
    ?assertEqual("/tmp/x.sock", rabbit_ct_broker_helpers:rpc(
                                  Config, rabbit_misc, ntoa, [{local, "/tmp/x.sock"}])),
    ?assertEqual(<<"/tmp/x.sock">>, rabbit_ct_broker_helpers:rpc(
                                     Config, rabbit_misc, ntoab, [{local, <<"/tmp/x.sock">>}])).

%% Verifies finding #4: tcp_listener_addresses must map the {local, Path, Port}
%% tuple produced by the domain_socket schema datatype to a UDS address, while a
%% bare string (which the schema never produces) must not be silently treated as
%% a socket path.
test_uds_tcp_listener_addresses(Config) ->
    ?assertMatch([{{local, "/var/run/rmq.sock"}, 0, local}],
                 rabbit_ct_broker_helpers:rpc(
                   Config, rabbit_networking, tcp_listener_addresses,
                   [{local, "/var/run/rmq.sock", 0}])),
    ?assertError({exception, function_clause, _},
                 rabbit_ct_broker_helpers:rpc(
                   Config, rabbit_networking, tcp_listener_addresses, ["localhost"])).

%% Runs on the broker node: listens on a UDS, connects to it, accepts the
%% connection, and reports is_loopback/1 for the accepted socket.
is_loopback_on_accepted_uds(SocketPath) ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false},
                                 {ifaddr, {local, SocketPath}}, local]),
    Self = self(),
    _ = spawn(fun() ->
                      {ok, C} = gen_tcp:connect({local, SocketPath}, 0,
                                                [binary, {active, false}, local]),
                      Self ! connected,
                      receive stop -> ok after 5000 -> ok end,
                      gen_tcp:close(C)
              end),
    {ok, A} = gen_tcp:accept(L, 5000),
    receive connected -> ok after 5000 -> ok end,
    Result = rabbit_net:is_loopback(A),
    gen_tcp:close(A),
    gen_tcp:close(L),
    file:delete(SocketPath),
    Result.

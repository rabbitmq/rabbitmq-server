%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(auth_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(CONNECT_TIMEOUT, 10000).

all() ->
    [{group, anonymous_no_ssl_user},
     {group, anonymous_ssl_user},
     {group, no_ssl_user},
     {group, ssl_user},
     {group, client_id_propagation}].

groups() ->
    [{anonymous_ssl_user, [],
      [anonymous_auth_success,
       user_credentials_auth,
       ssl_user_auth_success,
       ssl_user_vhost_not_allowed,
       ssl_user_vhost_parameter_mapping_success,
       ssl_user_vhost_parameter_mapping_not_allowed,
       ssl_user_vhost_parameter_mapping_vhost_does_not_exist,
       ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping
      ]},
     {anonymous_no_ssl_user, [],
      [anonymous_auth_success,
       user_credentials_auth,
       port_vhost_mapping_success,
       port_vhost_mapping_success_no_mapping,
       port_vhost_mapping_not_allowed,
       port_vhost_mapping_vhost_does_not_exist
       %% SSL auth will succeed, because we cannot ignore anonymous
       ]},
     {ssl_user, [],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_success,
       ssl_user_vhost_not_allowed,
       ssl_user_vhost_parameter_mapping_success,
       ssl_user_vhost_parameter_mapping_not_allowed,
       ssl_user_vhost_parameter_mapping_vhost_does_not_exist,
       ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping
      ]},
     {no_ssl_user, [],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_failure,
       port_vhost_mapping_success,
       port_vhost_mapping_success_no_mapping,
       port_vhost_mapping_not_allowed,
       port_vhost_mapping_vhost_does_not_exist
     ]},
     {client_id_propagation, [],
      [client_id_propagation]
     }
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"}
    ]),
    MqttConfig = mqtt_config(Group),
    AuthConfig = auth_config(Group),
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> merge_app_env(MqttConfig, Conf) end ] ++
        [ fun(Conf) -> case AuthConfig of
                            undefined -> Conf;
                            _         -> merge_app_env(AuthConfig, Conf)
                       end
          end ] ++
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

merge_app_env(MqttConfig, Config) ->
    rabbit_ct_helpers:merge_app_env(Config, MqttConfig).

mqtt_config(anonymous_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, true}]};
mqtt_config(anonymous_no_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  false},
                     {allow_anonymous, true}]};
mqtt_config(ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false}]};
mqtt_config(no_ssl_user) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  false},
                     {allow_anonymous, false}]};
mqtt_config(client_id_propagation) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, true}]}.

auth_config(client_id_propagation) ->
    {rabbit, [
            {auth_backends, [rabbit_auth_backend_mqtt_mock]}
          ]
    };
auth_config(_) ->
    undefined.

init_per_testcase(Testcase, Config) when Testcase == ssl_user_auth_success;
                                         Testcase == ssl_user_auth_failure ->
    Config1 = set_cert_user_on_default_vhost(Config),
    rabbit_ct_helpers:testcase_started(Config1, Testcase);
init_per_testcase(ssl_user_vhost_parameter_mapping_success, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    Config2 = set_vhost_for_cert_user(Config1, User),
    rabbit_ct_helpers:testcase_started(Config2, ssl_user_vhost_parameter_mapping_success);
init_per_testcase(ssl_user_vhost_parameter_mapping_not_allowed, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config2),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config2, User, VhostForCertUser),
    rabbit_ct_helpers:testcase_started(Config2, ssl_user_vhost_parameter_mapping_not_allowed);
init_per_testcase(user_credentials_auth, Config) ->
    User = <<"new-user">>,
    Pass = <<"new-user-pass">>,
    ok = rabbit_ct_broker_helpers:add_user(Config, 0, User, Pass),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, <<"/">>),
    Config1 = rabbit_ct_helpers:set_config(Config, [{new_user, User},
                                                    {new_user_pass, Pass}]),
    rabbit_ct_helpers:testcase_started(Config1, user_credentials_auth);
init_per_testcase(ssl_user_vhost_not_allowed, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    rabbit_ct_helpers:testcase_started(Config1, ssl_user_vhost_not_allowed);
init_per_testcase(ssl_user_vhost_parameter_mapping_vhost_does_not_exist, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    rabbit_ct_helpers:testcase_started(Config1, ssl_user_vhost_parameter_mapping_vhost_does_not_exist);
init_per_testcase(port_vhost_mapping_success, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    rabbit_ct_helpers:testcase_started(Config1, port_vhost_mapping_success);
init_per_testcase(port_vhost_mapping_success_no_mapping, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    PortToVHostMappingParameter = [
        {<<"1">>,   <<"unlikely to exist">>},
        {<<"2">>,   <<"unlikely to exist">>}],
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_port_to_vhost_mapping, PortToVHostMappingParameter),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, VHost),
    rabbit_ct_helpers:testcase_started(Config1, port_vhost_mapping_success_no_mapping);
init_per_testcase(port_vhost_mapping_not_allowed, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, VHost),
    rabbit_ct_helpers:testcase_started(Config1, port_vhost_mapping_not_allowed);
init_per_testcase(port_vhost_mapping_vhost_does_not_exist, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:delete_vhost(Config1, VHost),
    rabbit_ct_helpers:testcase_started(Config1, port_vhost_mapping_vhost_does_not_exist);
init_per_testcase(ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),

    Config3 = set_vhost_for_port_vhost_mapping_user(Config2, User),
    VhostForPortMapping = ?config(mqtt_port_to_vhost_mapping, Config2),
    rabbit_ct_broker_helpers:clear_permissions(Config3, User, VhostForPortMapping),

    rabbit_ct_broker_helpers:clear_permissions(Config3, User, <<"/">>),
    rabbit_ct_helpers:testcase_started(Config3, ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

set_cert_user_on_default_vhost(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    CertFile = filename:join([CertsDir, "client", "cert.pem"]),
    {ok, CertBin} = file:read_file(CertFile),
    [{'Certificate', Cert, not_encrypted}] = public_key:pem_decode(CertBin),
    UserBin = rabbit_ct_broker_helpers:rpc(Config, 0,
                                           rabbit_ssl,
                                           peer_cert_auth_name,
                                           [Cert]),
    User = binary_to_list(UserBin),
    ok = rabbit_ct_broker_helpers:add_user(Config, 0, User, ""),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, <<"/">>),
    rabbit_ct_helpers:set_config(Config, [{temp_ssl_user, User}]).

set_vhost_for_cert_user(Config, User) ->
    VhostForCertUser = <<"vhost_for_cert_user">>,
    UserToVHostMappingParameter = [
        {rabbit_data_coercion:to_binary(User), VhostForCertUser},
        {<<"O=client,CN=unlikelytoexistuser">>, <<"vhost2">>}
    ],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_default_vhosts, UserToVHostMappingParameter),
    rabbit_ct_helpers:set_config(Config, [{temp_vhost_for_ssl_user, VhostForCertUser}]).

set_vhost_for_port_vhost_mapping_user(Config, User) ->
    VhostForPortMapping = <<"vhost_for_port_vhost_mapping">>,
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    TlsPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    PortToVHostMappingParameter = [
        {integer_to_binary(Port),    VhostForPortMapping},
        {<<"1884">>,                 <<"vhost2">>},
        {integer_to_binary(TlsPort), VhostForPortMapping},
        {<<"8884">>,                 <<"vhost2">>}

    ],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VhostForPortMapping),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, VhostForPortMapping),
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_port_to_vhost_mapping, PortToVHostMappingParameter),
    rabbit_ct_helpers:set_config(Config, [{temp_vhost_for_port_mapping, VhostForPortMapping}]).

end_per_testcase(Testcase, Config) when Testcase == ssl_user_auth_success;
                                        Testcase == ssl_user_auth_failure;
                                        Testcase == ssl_user_vhost_not_allowed ->
    delete_cert_user(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(TestCase, Config) when TestCase == ssl_user_vhost_parameter_mapping_success;
                                        TestCase == ssl_user_vhost_parameter_mapping_not_allowed ->
    delete_cert_user(Config),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(user_credentials_auth, Config) ->
    User = ?config(new_user, Config),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["delete_user", User]),
    rabbit_ct_helpers:testcase_finished(Config, user_credentials_auth);
end_per_testcase(ssl_user_vhost_parameter_mapping_vhost_does_not_exist, Config) ->
    delete_cert_user(Config),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),
    rabbit_ct_helpers:testcase_finished(Config, ssl_user_vhost_parameter_mapping_vhost_does_not_exist);
end_per_testcase(Testcase, Config) when Testcase == port_vhost_mapping_success;
                                        Testcase == port_vhost_mapping_not_allowed;
                                        Testcase == port_vhost_mapping_success_no_mapping ->
    User = <<"guest">>,
    rabbit_ct_broker_helpers:set_full_permissions(Config, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(port_vhost_mapping_vhost_does_not_exist, Config) ->
    User = <<"guest">>,
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, User, <<"/">>),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    rabbit_ct_helpers:testcase_finished(Config, port_vhost_mapping_vhost_does_not_exist);
end_per_testcase(ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping, Config) ->
    delete_cert_user(Config),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),

    VHostForPortVHostMapping = ?config(temp_vhost_for_port_mapping, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHostForPortVHostMapping),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    rabbit_ct_helpers:testcase_finished(Config, ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

delete_cert_user(Config) ->
    User = ?config(temp_ssl_user, Config),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["delete_user", User]).

anonymous_auth_success(Config) ->
    expect_successful_connection(fun connect_anonymous/1, Config).

anonymous_auth_failure(Config) ->
    expect_authentication_failure(fun connect_anonymous/1, Config).


ssl_user_auth_success(Config) ->
    expect_successful_connection(fun connect_ssl/1, Config).

ssl_user_auth_failure(Config) ->
    expect_authentication_failure(fun connect_ssl/1, Config).

user_credentials_auth(Config) ->
    NewUser = ?config(new_user, Config),
    NewUserPass = ?config(new_user_pass, Config),

    expect_successful_connection(
        fun(Conf) -> connect_user(NewUser, NewUserPass, Conf) end,
        Config),

    expect_successful_connection(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config),

    expect_successful_connection(
        fun(Conf) -> connect_user(<<"/:guest">>, <<"guest">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(NewUser, <<"invalid_pass">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(undefined, <<"pass">>, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(NewUser, undefined, Conf) end,
        Config),

    expect_authentication_failure(
        fun(Conf) -> connect_user(<<"non-existing-vhost:guest">>, <<"guest">>, Conf) end,
        Config).

ssl_user_vhost_parameter_mapping_success(Config) ->
    expect_successful_connection(fun connect_ssl/1, Config).

ssl_user_vhost_parameter_mapping_not_allowed(Config) ->
    expect_authentication_failure(fun connect_ssl/1, Config).

ssl_user_vhost_not_allowed(Config) ->
    expect_authentication_failure(fun connect_ssl/1, Config).

ssl_user_vhost_parameter_mapping_vhost_does_not_exist(Config) ->
    expect_authentication_failure(fun connect_ssl/1, Config).

port_vhost_mapping_success(Config) ->
    expect_successful_connection(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config).

port_vhost_mapping_success_no_mapping(Config) ->
    %% no vhost mapping for the port, falling back to default vhost
    %% where the user can connect
    expect_successful_connection(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config
    ).

port_vhost_mapping_not_allowed(Config) ->
    expect_authentication_failure(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config
    ).

port_vhost_mapping_vhost_does_not_exist(Config) ->
    expect_authentication_failure(
        fun(Conf) -> connect_user(<<"guest">>, <<"guest">>, Conf) end,
        Config
    ).

ssl_user_port_vhost_mapping_takes_precedence_over_cert_vhost_mapping(Config) ->
    expect_successful_connection(fun connect_ssl/1, Config).

connect_anonymous(Config) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, <<"simpleClient">>},
                       {proto_ver, 3},
                       {logger, info}]).

connect_ssl(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    SSLConfig = [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                 {certfile, filename:join([CertsDir, "client", "cert.pem"])},
                 {keyfile, filename:join([CertsDir, "client", "key.pem"])}],
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, <<"simpleClient">>},
                       {proto_ver, 3},
                       {logger, info},
                       {ssl, SSLConfig}]).

client_id_propagation(Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config,
                                                             rabbit_auth_backend_mqtt_mock),
    %% setup creates the ETS table required for the mqtt auth mock
    %% it blocks indefinitely so we need to spawn
    Self = self(),
    _ = spawn(fun () ->
                      rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_mqtt_mock,
                                                   setup,
                                                   [Self])
              end),
    %% the setup process will notify us
    receive
        ok         -> ok
        after 3000 -> ok
    end,
    ClientId = <<"client-id-propagation">>,
    {ok, C} = connect_user(<<"client-id-propagation">>, <<"client-id-propagation">>,
                           Config, ClientId),
    receive {mqttc, C, connected} -> ok
    after ?CONNECT_TIMEOUT -> exit(emqttc_connection_timeout)
    end,
    emqttc:subscribe(C, <<"TopicA">>, qos0),
    [{authentication, AuthProps}] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                                 rabbit_auth_backend_mqtt_mock,
                                                                 get,
                                                                 [authentication]),
    ?assertEqual(ClientId, proplists:get_value(client_id, AuthProps)),

    [{vhost_access, AuthzData}] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                               rabbit_auth_backend_mqtt_mock,
                                                               get,
                                                               [vhost_access]),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, AuthzData)),

    [{resource_access, AuthzContext}] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                                     rabbit_auth_backend_mqtt_mock,
                                                                     get,
                                                                     [resource_access]),
    ?assertEqual(true, maps:size(AuthzContext) > 0),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, AuthzContext)),

    [{topic_access, TopicContext}] = rabbit_ct_broker_helpers:rpc(Config, 0,
                                                                  rabbit_auth_backend_mqtt_mock,
                                                                  get,
                                                                  [topic_access]),
    VariableMap = maps:get(variable_map, TopicContext),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, VariableMap)),

    emqttc:disconnect(C).

connect_user(User, Pass, Config) ->
    connect_user(User, Pass, Config, User).
connect_user(User, Pass, Config, ClientID) ->
    Creds = case User of
        undefined -> [];
        _         -> [{username, User}]
    end ++ case Pass of
        undefined -> [];
        _         -> [{password, Pass}]
    end,
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqttc:start_link([{host, "localhost"},
                       {port, P},
                       {client_id, ClientID},
                       {proto_ver, 3},
                       {logger, info}] ++ Creds).

expect_successful_connection(ConnectFun, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    {ok, C} = ConnectFun(Config),
    receive {mqttc, C, connected} -> emqttc:disconnect(C)
    after ?CONNECT_TIMEOUT -> exit(emqttc_connection_timeout)
    end,
    [Attempt] =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt)),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"mqtt">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 1).

expect_authentication_failure(ConnectFun, Config) ->
    process_flag(trap_exit, true),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    {ok, C} = ConnectFun(Config),
    Result = receive
        {mqttc, C, connected} -> {error, unexpected_anonymous_connection};
        {'EXIT', C, {shutdown,{connack_error,'CONNACK_AUTH'}}} -> ok;
        {'EXIT', C, {shutdown,{connack_error,'CONNACK_CREDENTIALS'}}} -> ok
    after
        ?CONNECT_TIMEOUT -> {error, emqttc_connection_timeout}
    end,
    [Attempt] =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt), <<>>),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"mqtt">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 0),
    process_flag(trap_exit, false),
    case Result of
        ok -> ok;
        {error, Err} -> exit(Err)
    end.

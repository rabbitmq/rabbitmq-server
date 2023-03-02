%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(auth_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% defined in MQTT v4 and v5 (not in v3)
-define(SUBACK_FAILURE, 16#80).

-define(FAIL_IF_CRASH_LOG, {["Generic server.*terminating"],
                            fun () -> ct:fail(crash_detected) end}).
-import(rabbit_ct_broker_helpers, [rpc/5]).

all() ->
    [{group, anonymous_no_ssl_user},
     {group, anonymous_ssl_user},
     {group, no_ssl_user},
     {group, ssl_user},
     {group, client_id_propagation},
     {group, authz},
     {group, limit}].

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
     },
     {authz, [],
      [no_queue_bind_permission,
       no_queue_unbind_permission,
       no_queue_consume_permission,
       no_queue_consume_permission_on_connect,
       no_queue_delete_permission,
       no_queue_declare_permission,
       no_publish_permission,
       no_publish_permission_will_message,
       no_topic_read_permission,
       no_topic_write_permission,
       topic_write_permission_variable_expansion,
       loopback_user_connects_from_remote_host
      ]
     },
     {limit, [],
      [vhost_connection_limit,
       vhost_queue_limit,
       user_connection_limit
      ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(authz, Config0) ->
    User = <<"mqtt-user">>,
    Password = <<"mqtt-password">>,
    VHost = <<"mqtt-vhost">>,
    MqttConfig = {rabbitmq_mqtt, [{default_user, User}
                                 ,{default_pass, Password}
                                 ,{allow_anonymous, true}
                                 ,{vhost, VHost}
                                 ,{exchange, <<"amq.topic">>}
                                 ]},
    Config1 = rabbit_ct_helpers:run_setup_steps(rabbit_ct_helpers:merge_app_env(Config0, MqttConfig),
                                                rabbit_ct_broker_helpers:setup_steps() ++
                                                    rabbit_ct_client_helpers:setup_steps()),
    rabbit_ct_broker_helpers:add_user(Config1, User, Password),
    rabbit_ct_broker_helpers:add_vhost(Config1, VHost),
    [Log|_] = rpc(Config1, 0, rabbit, log_locations, []),
    [{mqtt_user, User}, {mqtt_vhost, VHost}, {mqtt_password, Password}, {log_location, Log}|Config1];
init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"}
    ]),
    MqttConfig = mqtt_config(Group),
    AuthConfig = auth_config(Group),
    rabbit_ct_helpers:run_setup_steps(Config1,
        [ fun(Conf) -> case MqttConfig of
                           undefined  -> Conf;
                           _          -> merge_app_env(MqttConfig, Conf)
                       end
          end] ++
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
                     {allow_anonymous, true}]};
mqtt_config(_) ->
    undefined.

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
    UserBin = rpc(Config, 0, rabbit_ssl, peer_cert_auth_name, [Cert]),
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
end_per_testcase(Testcase, Config) when Testcase == no_queue_bind_permission;
                                        Testcase == no_queue_unbind_permission;
                                        Testcase == no_queue_consume_permission;
                                        Testcase == no_queue_consume_permission_on_connect;
                                        Testcase == no_queue_delete_permission;
                                        Testcase == no_queue_declare_permission;
                                        Testcase == no_publish_permission;
                                        Testcase == no_publish_permission_will_message;
                                        Testcase == no_topic_read_permission;
                                        Testcase == no_topic_write_permission;
                                        Testcase == topic_write_permission_variable_expansion;
                                        Testcase == loopback_user_connects_from_remote_host ->
    %% So let's wait before logs are surely flushed
    Marker = "MQTT_AUTH_SUITE_MARKER",
    rpc(Config, 0, rabbit_log, error, [Marker]),
    wait_log(Config, [{[Marker], fun () -> stop end}]),

    %% Preserve file contents in case some investigation is needed, before truncating.
    file:copy(?config(log_location, Config), iolist_to_binary([?config(log_location, Config), ".", atom_to_binary(Testcase)])),

    %% And provide an empty log file for the next test in this group
    file:write_file(?config(log_location, Config), <<>>),

    rabbit_ct_helpers:testcase_finished(Config, Testcase);
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
    connect_anonymous(Config, <<"simpleClient">>).

connect_anonymous(Config, ClientId) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqtt:start_link([{host, "localhost"},
                      {port, P},
                      {clientid, ClientId},
                      {proto_ver, v4}]).

connect_ssl(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    SSLConfig = [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                 {certfile, filename:join([CertsDir, "client", "cert.pem"])},
                 {keyfile, filename:join([CertsDir, "client", "key.pem"])}],
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    emqtt:start_link([{host, "localhost"},
                      {port, P},
                      {clientid, <<"simpleClient">>},
                      {proto_ver, v4},
                      {ssl, true},
                      {ssl_opts, SSLConfig}]).

client_id_propagation(Config) ->
    ok = rabbit_ct_broker_helpers:add_code_path_to_all_nodes(Config,
                                                             rabbit_auth_backend_mqtt_mock),
    %% setup creates the ETS table required for the mqtt auth mock
    %% it blocks indefinitely so we need to spawn
    Self = self(),
    _ = spawn(
          fun () ->
                  rpc(Config, 0, rabbit_auth_backend_mqtt_mock, setup, [Self])
          end),
    %% the setup process will notify us
    receive
        ok -> ok
    after
        3000 -> ct:fail("timeout waiting for rabbit_auth_backend_mqtt_mock:setup/1")
    end,
    ClientId = <<"client-id-propagation">>,
    {ok, C} = connect_user(<<"fake-user">>, <<"fake-password">>,
                           Config, ClientId),
    {ok, _} = emqtt:connect(C),
    {ok, _, _} = emqtt:subscribe(C, <<"TopicA">>),
    [{authentication, AuthProps}] = rpc(Config, 0,
                                        rabbit_auth_backend_mqtt_mock,
                                        get,
                                        [authentication]),
    ?assertEqual(ClientId, proplists:get_value(client_id, AuthProps)),

    [{vhost_access, AuthzData}] = rpc(Config, 0,
                                      rabbit_auth_backend_mqtt_mock,
                                      get,
                                      [vhost_access]),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, AuthzData)),

    [{resource_access, AuthzContext}] = rpc(Config, 0,
                                            rabbit_auth_backend_mqtt_mock,
                                            get,
                                            [resource_access]),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, AuthzContext)),

    [{topic_access, TopicContext}] = rpc(Config, 0,
                                         rabbit_auth_backend_mqtt_mock,
                                         get,
                                         [topic_access]),
    VariableMap = maps:get(variable_map, TopicContext),
    ?assertEqual(ClientId, maps:get(<<"client_id">>, VariableMap)),

    ok = emqtt:disconnect(C).

%% These tests try to cover all operations that are listed in the
%% table in https://www.rabbitmq.com/access-control.html#authorisation
%% and which MQTT plugin tries to perform.
%%
%% MQTT v4 has a single SUBACK error code but does not allow to differentiate
%% between different kind of errors why a subscription request failed.
%% The only non-intrusive way to check for `access_refused`
%% codepath is by checking logs. Every testcase from this group
%% truncates log file beforehand, so it'd be easier to analyze. There
%% is an additional wait in the corresponding end_per_testcase that
%% ensures that logs for the current testcase were completely
%% flushed, and won't contaminate following tests from this group.
no_queue_bind_permission(Config) ->
    ExpectedLogs =
    ["MQTT resource access refused: write access to queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' "
     "refused for user 'mqtt-user'",
     "Failed to add binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' for topic test/topic: access_refused"
    ],
    test_subscribe_permissions_combination(<<".*">>, <<"">>, <<".*">>, Config, ExpectedLogs).

no_queue_unbind_permission(Config) ->
    User = ?config(mqtt_user, Config),
    Vhost = ?config(mqtt_vhost, Config),
    rabbit_ct_broker_helpers:set_permissions(Config, User, Vhost, <<".*">>, <<".*">>, <<".*">>),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Opts = [{host, "localhost"},
            {port, P},
            {proto_ver, v4},
            {clientid, User},
            {username, User},
            {password, ?config(mqtt_password, Config)}],
    {ok, C1} = emqtt:start_link([{clean_start, false} | Opts]),
    {ok, _} = emqtt:connect(C1),
    Topic = <<"my/topic">>,
    ?assertMatch({ok, _Properties, [1]},
                 emqtt:subscribe(C1, Topic, qos1)),
    ok = emqtt:disconnect(C1),

    %% Revoke read access to amq.topic exchange.
    rabbit_ct_broker_helpers:set_permissions(Config, User, Vhost, <<".*">>, <<".*">>, <<"^(?!amq\.topic$)">>),
    {ok, C2} = emqtt:start_link([{clean_start, false} | Opts]),
    {ok, _} = emqtt:connect(C2),
    process_flag(trap_exit, true),
    %% We subscribe with the same client ID to the same topic again, but this time with QoS 0.
    %% Therefore we trigger the qos1 queue to be unbound (and the qos0 queue to be bound).
    %% However, unbinding requires read access to the exchange, which we don't have anymore.
    ?assertMatch({ok, _Properties, [?SUBACK_FAILURE]},
                 emqtt:subscribe(C2, Topic, qos0)),
    ok = assert_connection_closed(C2),
    ExpectedLogs =
    ["MQTT resource access refused: read access to exchange 'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
     "Failed to remove binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
     "'mqtt-subscription-mqtt-userqos1' in vhost 'mqtt-vhost' for topic my/topic: access_refused"
    ],
    wait_log(Config, [?FAIL_IF_CRASH_LOG, {ExpectedLogs, fun () -> stop end}]),

    %% Clean up the qos1 queue by connecting with clean session.
    rabbit_ct_broker_helpers:set_permissions(Config, User, Vhost, <<".*">>, <<".*">>, <<".*">>),
    {ok, C3} = emqtt:start_link([{clean_start, true} | Opts]),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:disconnect(C3).

no_queue_consume_permission(Config) ->
    ExpectedLogs =
    ["MQTT resource access refused: read access to queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' "
     "refused for user 'mqtt-user'"],
    test_subscribe_permissions_combination(<<".*">>, <<".*">>, <<"^amq\\.topic">>, Config, ExpectedLogs).

no_queue_delete_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    ClientId = <<"no_queue_delete_permission">>,
    {ok, C1} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 [{clean_start, false}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, {<<"test/topic">>, qos1}),
    ok = emqtt:disconnect(C1),

    set_permissions(<<>>, ".*", ".*", Config),
    %% Now we have a durable queue that user doesn't have permission to delete.
    %% Attempt to establish clean session should fail.
    {ok, C2} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 [{clean_start, true}]),
    unlink(C2),
    ?assertMatch({error, _},
                 emqtt:connect(C2)),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: configure access to queue "
                        "'mqtt-subscription-~sqos1' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "Rejected MQTT connection .* with CONNACK return code 5"],
         fun() -> stop end}
      ]),
    ok.

no_queue_consume_permission_on_connect(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    ClientId = <<"no_queue_consume_permission_on_connect">>,
    {ok, C1} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 [{clean_start, false}]),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, {<<"test/topic">>, qos1}),
    ok = emqtt:disconnect(C1),

    set_permissions(".*", ".*", "^amq\\.topic", Config),
    {ok, C2} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 [{clean_start, false}]),
    unlink(C2),
    ?assertMatch({error, _},
                 emqtt:connect(C2)),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: read access to queue "
                        "'mqtt-subscription-~sqos1' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "Rejected MQTT connection .* with CONNACK return code 5"],
         fun () -> stop end}
      ]),
    ok.

no_queue_declare_permission(Config) ->
    set_permissions("", ".*", ".*", Config),
    ClientId = <<"no_queue_declare_permission">>,
    {ok, C} = connect_user(
                ?config(mqtt_user, Config),
                ?config(mqtt_password, Config),
                Config,
                ClientId,
                [{clean_start, true}]),
    {ok, _} = emqtt:connect(C),

    process_flag(trap_exit, true),
    {ok, _, [?SUBACK_FAILURE]} = emqtt:subscribe(C, <<"test/topic">>, qos0),
    ok = assert_connection_closed(C),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: configure access to queue "
                        "'mqtt-subscription-~sqos0' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "MQTT protocol error on connection .*: subscribe_error"],
         fun () -> stop end}
      ]),
    ok.

no_publish_permission(Config) ->
    set_permissions(".*", "", ".*", Config),
    C = open_mqtt_connection(Config),
    process_flag(trap_exit, true),
    ok = emqtt:publish(C, <<"some/topic">>, <<"payload">>),
    assert_connection_closed(C),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG
              ,{["MQTT resource access refused: write access to exchange "
                 "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                 "MQTT connection .* is closing due to an authorization failure"],
                fun () -> stop end}
             ]),
    ok.

%% Test that publish permission checks are performed for the will message.
no_publish_permission_will_message(Config) ->
    %% Allow write access to queue.
    %% Disallow write access to exchange.
    set_permissions(".*", "^mqtt-subscription.*qos1$", ".*", Config),
    Topic = <<"will/topic">>,
    Opts = [{will_topic, Topic},
            {will_payload, <<"will payload">>},
            {will_qos, 0}],
    {ok, C} = connect_user(?config(mqtt_user, Config),
                           ?config(mqtt_password, Config),
                           Config,
                           <<"client-with-will">>,
                           Opts),
    {ok, _} = emqtt:connect(C),
    timer:sleep(100),
    [ServerPublisherPid] = util:all_connection_pids(Config),

    Sub = open_mqtt_connection(Config),
    {ok, _, [1]} = emqtt:subscribe(Sub, Topic, qos1),

    unlink(C),
    %% Trigger sending of will message.
    erlang:exit(ServerPublisherPid, test_will),

    %% We expect to not receive a will message because of missing publish permission.
    receive Unexpected -> ct:fail("Received unexpectedly: ~p", [Unexpected])
    after 300 -> ok
    end,

    wait_log(Config,
             [{["MQTT resource access refused: write access to exchange "
                "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'"],
               fun () -> stop end}
             ]),
    ok = emqtt:disconnect(Sub).

no_topic_read_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    set_topic_permissions("^allow-write\\..*", "^allow-read\\..*", Config),
    C = open_mqtt_connection(Config),

    %% Check topic permission setup is working.
    {ok, _, [0]} = emqtt:subscribe(C, <<"allow-read/some/topic">>),

    process_flag(trap_exit, true),
    {ok, _, [?SUBACK_FAILURE]} = emqtt:subscribe(C, <<"test/topic">>),
    ok = assert_connection_closed(C),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG,
              {["MQTT topic access refused: read access to topic 'test.topic' in exchange "
                "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                "Failed to add binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
                "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' for topic test/topic: access_refused"
               ],
               fun () -> stop end}
             ]),
    ok.

no_topic_write_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    set_topic_permissions("^allow-write\\..*", "^allow-read\\..*", Config),
    C = open_mqtt_connection(Config),

    %% Check topic permission setup is working.
    {ok, _} = emqtt:publish(C, <<"allow-write/some/topic">>, <<"payload">>, qos1),

    process_flag(trap_exit, true),
    ?assertMatch({error, _},
                 emqtt:publish(C, <<"some/other/topic">>, <<"payload">>, qos1)),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG
              ,{["MQTT topic access refused: write access to topic 'some.other.topic' in "
                 "exchange 'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                 "MQTT connection .* is closing due to an authorization failure"],
                fun () -> stop end}
             ]),
    ok.

%% "Internal (default) authorisation backend supports variable expansion in permission patterns.
%% Three variables are supported: username, vhost, and client_id.
%% Note that client_id only applies to MQTT."
topic_write_permission_variable_expansion(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    set_topic_permissions("^{username}.{vhost}.{client_id}$", ".*", Config),
    User = ?config(mqtt_user, Config),
    VHost = ?config(mqtt_vhost, Config),
    ClientId = <<"my_client">>,
    {ok, C} = connect_user(User, ?config(mqtt_password, Config), Config, ClientId),
    {ok, _} = emqtt:connect(C),
    Prefix = <<User/binary, "/", VHost/binary, "/">>,
    AllowedTopic =  <<Prefix/binary, ClientId/binary>>,
    DeniedTopic = <<Prefix/binary, <<"other_client">>/binary>>,
    ?assertMatch({ok, _}, emqtt:publish(C, AllowedTopic, <<"payload">>, qos1)),
    unlink(C),
    ?assertMatch({error, _}, emqtt:publish(C, DeniedTopic, <<"payload">>, qos1)),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG
              ,{["MQTT topic access refused: write access to topic "
                 "'mqtt-user.mqtt-vhost.other_client' in exchange 'amq.topic' in vhost "
                 "'mqtt-vhost' refused for user 'mqtt-user'",
                 "MQTT connection .* is closing due to an authorization failure"],
                fun () -> stop end}
             ]),
    ok.

loopback_user_connects_from_remote_host(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    {ok, C} = connect_anonymous(Config),

    %% CT node runs on the same host as the RabbitMQ node.
    %% Instead of changing the IP address of CT node to a non-loopback IP address,
    %% we mock rabbit_access_control:check_user_loopback/2.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_access_control,
    ok = rpc(Config, 0, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, 0, meck, expect, [Mod, check_user_loopback, 2, not_allowed]),

    process_flag(trap_exit, true),
    ?assertMatch({error, _}, emqtt:connect(C)),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG,
              {["MQTT login failed: user 'mqtt-user' can only connect via localhost",
                "Rejected MQTT connection .* with CONNACK return code 5"],
               fun () -> stop end}
             ]),

    true = rpc(Config, 0, meck, validate, [Mod]),
    ok = rpc(Config, 0, meck, unload, [Mod]).

set_topic_permissions(WritePat, ReadPat, Config) ->
    rpc(Config, 0,
        rabbit_auth_backend_internal, set_topic_permissions,
        [?config(mqtt_user, Config), ?config(mqtt_vhost, Config),
         <<"amq.topic">>, WritePat, ReadPat, <<"acting-user">>]).

set_permissions(PermConf, PermWrite, PermRead, Config) ->
    rabbit_ct_broker_helpers:set_permissions(Config, ?config(mqtt_user, Config), ?config(mqtt_vhost, Config),
                                             iolist_to_binary(PermConf),
                                             iolist_to_binary(PermWrite),
                                             iolist_to_binary(PermRead)).

open_mqtt_connection(Config) ->
    open_mqtt_connection(Config, []).
open_mqtt_connection(Config, Opts) ->
    {ok, C} = connect_user(?config(mqtt_user, Config), ?config(mqtt_password, Config), Config, ?config(mqtt_user, Config), Opts),
    {ok, _} = emqtt:connect(C),
    C.

test_subscribe_permissions_combination(PermConf, PermWrite, PermRead, Config, ExtraLogChecks) ->
    rabbit_ct_broker_helpers:set_permissions(Config,
                                             ?config(mqtt_user, Config),
                                             ?config(mqtt_vhost, Config),
                                             PermConf, PermWrite, PermRead),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    User = ?config(mqtt_user, Config),
    Opts = [{host, "localhost"},
            {port, P},
            {clientid, User},
            {username, User},
            {password, ?config(mqtt_password, Config)}],
    {ok, C1} = emqtt:start_link([{proto_ver, v4} | Opts]),
    {ok, _} = emqtt:connect(C1),
    process_flag(trap_exit, true),
    %% In v4, we expect to receive a failure return code for our subscription in the SUBACK packet.
    ?assertMatch({ok, _Properties, [?SUBACK_FAILURE]},
                 emqtt:subscribe(C1, <<"test/topic">>)),
    ok = assert_connection_closed(C1),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG
              ,{["MQTT protocol error on connection.*: subscribe_error"|ExtraLogChecks], fun () -> stop end}
             ]),

    {ok, C2} = emqtt:start_link([{proto_ver, v3} | Opts]),
    {ok, _} = emqtt:connect(C2),

    %% In v3, there is no failure return code in the SUBACK packet.
    ?assertMatch({ok, _Properties, [0]},
                 emqtt:subscribe(C2, <<"test/topic">>)),
    ok = assert_connection_closed(C2).

connect_user(User, Pass, Config) ->
    connect_user(User, Pass, Config, User, []).
connect_user(User, Pass, Config, ClientID) ->
    connect_user(User, Pass, Config, ClientID, []).
connect_user(User, Pass, Config, ClientID0, Opts) ->
    Creds = case User of
        undefined -> [];
        _         -> [{username, User}]
    end ++ case Pass of
        undefined -> [];
        _         -> [{password, Pass}]
    end,
    ClientID = case ClientID0 of
                   undefined -> [];
                   ID -> [{clientid, ID}]
               end,
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqtt:start_link(Opts ++ Creds ++ ClientID ++
                     [{host, "localhost"}, {port, P}, {proto_ver, v4}]).

expect_successful_connection(ConnectFun, Config) ->
    rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    {ok, C} = ConnectFun(Config),
    {ok, _} = emqtt:connect(C),
    ok = emqtt:disconnect(C),
    [Attempt] = rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt)),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"mqtt">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 1).

expect_authentication_failure(ConnectFun, Config) ->
    rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    {ok, C} = ConnectFun(Config),
    unlink(C),
    ?assertMatch({error, _}, emqtt:connect(C)),
    [Attempt] = rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt), <<>>),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"mqtt">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 0),
    ok.

vhost_connection_limit(Config) ->
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, <<"/">>, max_connections, 2),
    {ok, C1} = connect_anonymous(Config, <<"client1">>),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = connect_anonymous(Config, <<"client2">>),
    {ok, _} = emqtt:connect(C2),
    {ok, C3} = connect_anonymous(Config, <<"client3">>),
    unlink(C3),
    ?assertMatch({error, {unauthorized_client, _}}, emqtt:connect(C3)),
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

vhost_queue_limit(Config) ->
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, <<"/">>, max_queues, 1),
    {ok, C} = connect_anonymous(Config),
    {ok, _} = emqtt:connect(C),
    process_flag(trap_exit, true),
    %% qos0 queue can be created, qos1 queue fails to be created.
    %% (RabbitMQ creates subscriptions in the reverse order of the SUBSCRIBE packet.)
    ?assertMatch({ok, _Properties, [?SUBACK_FAILURE, ?SUBACK_FAILURE, 0]},
                 emqtt:subscribe(C, [{<<"topic1">>, qos1},
                                     {<<"topic2">>, qos1},
                                     {<<"topic3">>, qos0}])),
    ok = assert_connection_closed(C).

user_connection_limit(Config) ->
    DefaultUser = <<"guest">>,
    ok = rabbit_ct_broker_helpers:set_user_limits(Config, DefaultUser, #{max_connections => 1}),
    {ok, C1} = connect_anonymous(Config, <<"client1">>),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = connect_anonymous(Config, <<"client2">>),
    unlink(C2),
    ?assertMatch({error, {unauthorized_client, _}}, emqtt:connect(C2)),
    ok = emqtt:disconnect(C1),
    ok = rabbit_ct_broker_helpers:clear_user_limits(Config, DefaultUser, max_connections).

wait_log(Config, Clauses) ->
    wait_log(Config, Clauses, erlang:monotonic_time(millisecond) + 1000).

wait_log(Config, Clauses, Deadline) ->
    {ok, Content} = file:read_file(?config(log_location, Config)),
    case erlang:monotonic_time(millisecond) of
        T when T =< Deadline ->
            case wait_log_check_clauses(Content, Clauses) of
                stop -> ok;
                continue ->
                    timer:sleep(50),
                    wait_log(Config, Clauses, Deadline)
            end;
        _ ->
            lists:foreach(
              fun
                  ({REs, _}) ->
                      Matches = [ io_lib:format("~p - ~s~n", [RE, re:run(Content, RE, [{capture, none}])]) || RE <- REs ],
                      ct:pal("Wait log clause status: ~s", [Matches])
              end, Clauses),
            ct:fail(expected_logs_not_found)
    end,
    ok.

wait_log_check_clauses(_, []) ->
    continue;
wait_log_check_clauses(Content, [{REs, Fun}|Rest]) ->
    case multiple_re_match(Content, REs) of
        true -> Fun();
        _ ->
            wait_log_check_clauses(Content, Rest)
    end.

multiple_re_match(Content, REs) ->
    lists:all(
      fun (RE) ->
              match == re:run(Content, RE, [{capture, none}])
      end, REs).

assert_connection_closed(ClientPid) ->
    receive
        {'EXIT', ClientPid, {shutdown, tcp_closed}} ->
            ok
    after
        2000 ->
            ct:fail("timed out waiting for exit message")
    end.

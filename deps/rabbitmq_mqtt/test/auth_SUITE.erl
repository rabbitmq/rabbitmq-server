%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(auth_SUITE).
-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% not defined in v3
-define(SUBACK_FAILURE, 16#80).

-define(RC_DISCONNECT_WITH_WILL, 16#04).
-define(RC_NOT_AUTHORIZED, 16#87).
-define(RC_QUOTA_EXCEEDED, 16#97).

-define(FAIL_IF_CRASH_LOG, {["Generic server.*terminating"],
                            fun () -> ct:fail(crash_detected) end}).
-import(rabbit_ct_broker_helpers,
        [rpc/5,
         set_full_permissions/3]).
-import(rabbit_ct_helpers, [testcase_started/2]).
-import(util, [non_clean_sess_opts/0]).

all() ->
    [
     {group, v4},
     {group, v5}
    ].

groups() ->
    [
     {v4, [], sub_groups()},
     {v5, [], sub_groups()}
    ].

sub_groups() ->
    [{anonymous_ssl_user, [shuffle],
      [anonymous_auth_success,
       user_credentials_auth,
       ssl_user_auth_success,
       ssl_user_vhost_not_allowed,
       ssl_user_vhost_parameter_mapping_success,
       ssl_user_vhost_parameter_mapping_not_allowed,
       ssl_user_vhost_parameter_mapping_vhost_does_not_exist,
       ssl_user_cert_vhost_mapping_takes_precedence_over_port_vhost_mapping
      ]},
     {anonymous_no_ssl_user, [shuffle],
      [anonymous_auth_success,
       user_credentials_auth,
       port_vhost_mapping_success,
       port_vhost_mapping_success_no_mapping,
       port_vhost_mapping_not_allowed,
       port_vhost_mapping_vhost_does_not_exist
       %% SSL auth will succeed, because we cannot ignore anonymous
       ]},
     {ssl_user, [shuffle],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_success,
       ssl_user_vhost_not_allowed,
       ssl_user_vhost_parameter_mapping_success,
       ssl_user_vhost_parameter_mapping_not_allowed,
       ssl_user_vhost_parameter_mapping_vhost_does_not_exist,
       ssl_user_cert_vhost_mapping_takes_precedence_over_port_vhost_mapping
      ]},
     {ssl_user_with_invalid_client_id_in_cert_san_dns, [],
       [invalid_client_id_from_cert_san_dns
       ]},
     {ssl_user_with_client_id_in_cert_san_dns, [],
       [client_id_from_cert_san_dns
       ]},
     {ssl_user_with_client_id_in_cert_san_dns_1, [],
       [client_id_from_cert_san_dns_1
       ]},
     {ssl_user_with_client_id_in_cert_san_email, [],
       [client_id_from_cert_san_email
       ]},
     {ssl_user_with_client_id_in_cert_dn, [],
       [client_id_from_cert_dn
       ]},
     {no_ssl_user, [shuffle],
      [anonymous_auth_failure,
       user_credentials_auth,
       ssl_user_auth_failure,
       port_vhost_mapping_success,
       port_vhost_mapping_success_no_mapping,
       port_vhost_mapping_not_allowed,
       port_vhost_mapping_vhost_does_not_exist
     ]},
     {client_id_propagation, [shuffle],
      [client_id_propagation]
     },
     {authz, [],
      [queue_bind_permission,
       queue_unbind_permission,
       queue_consume_permission,
       queue_consume_permission_on_connect,
       subscription_queue_delete_permission,
       will_queue_create_permission_queue_read,
       will_queue_create_permission_exchange_write,
       will_queue_publish_permission_exchange_write,
       will_queue_publish_permission_topic_write,
       will_queue_delete_permission,
       queue_declare_permission,
       publish_permission,
       publish_permission_will_message,
       topic_read_permission,
       topic_write_permission,
       topic_write_permission_variable_expansion,
       loopback_user_connects_from_remote_host,
       connect_permission
      ]
     },
     {limit, [shuffle],
      [vhost_connection_limit,
       vhost_queue_limit,
       user_connection_limit
      ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:set_config(
      Config,
      [{start_rmq_with_plugins_disabled, true}]).

end_per_suite(Config) ->
    Config.

init_per_group(G, Config)
  when G =:= v4;
       G =:= v5 ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, G});
init_per_group(authz, Config0) ->
    User = <<"mqtt-user">>,
    Password = <<"mqtt-password">>,
    VHost = <<"mqtt-vhost">>,
    Env = [{rabbitmq_mqtt,
            [{allow_anonymous, true},
             {vhost, VHost},
             {exchange, <<"amq.topic">>}
            ]},
           {rabbit,
            [{anonymous_login_user, User},
             {anonymous_login_pass, Password}
            ]}],
    Config1 = rabbit_ct_helpers:merge_app_env(Config0, Env),
    Config = rabbit_ct_helpers:run_setup_steps(
               Config1,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config, rabbitmq_mqtt),
    rabbit_ct_broker_helpers:add_user(Config, User, Password),
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    [Log|_] = rpc(Config, 0, rabbit, log_locations, []),
    [{mqtt_user, User},
     {mqtt_vhost, VHost},
     {mqtt_password, Password},
     {log_location, Log} | Config];
init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Suffix},
        {rmq_certspwd, "bunnychow"}
    ]),
    MqttConfig = mqtt_config(Group),
    AuthConfig = auth_config(Group),
    Config2 = rabbit_ct_helpers:run_setup_steps(
      Config1,
      [fun(Conf) -> case MqttConfig of
                        undefined  -> Conf;
                        _          -> merge_app_env(MqttConfig, Conf)
                    end
       end] ++
      [fun(Conf) -> case AuthConfig of
                        undefined -> Conf;
                        _         -> merge_app_env(AuthConfig, Conf)
                    end
       end] ++
          rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()),
    util:enable_plugin(Config2, rabbitmq_mqtt),
    Config2.

end_per_group(G, Config)
  when G =:= v4;
       G =:= v5 ->
    Config;
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
mqtt_config(T) when T == ssl_user_with_client_id_in_cert_san_dns;
                    T == ssl_user_with_invalid_client_id_in_cert_san_dns ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false},
                     {ssl_cert_client_id_from, subject_alternative_name},
                     {ssl_cert_login_san_type, dns}]};
mqtt_config(ssl_user_with_client_id_in_cert_san_dns_1) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false},
                     {ssl_cert_client_id_from, subject_alternative_name},
                     {ssl_cert_login_san_type, dns},
                     {ssl_cert_login_san_index, 1}]};
mqtt_config(ssl_user_with_client_id_in_cert_san_email) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false},
                     {ssl_cert_client_id_from, subject_alternative_name},
                     {ssl_cert_login_san_type, email}]};
mqtt_config(ssl_user_with_client_id_in_cert_dn) ->
    {rabbitmq_mqtt, [{ssl_cert_login,  true},
                     {allow_anonymous, false},
                     {ssl_cert_client_id_from, distinguished_name}
                     ]};
mqtt_config(_) ->
    undefined.

auth_config(T) when T == client_id_propagation;
                    T == ssl_user_with_client_id_in_cert_san_dns;
                    T == ssl_user_with_client_id_in_cert_san_dns_1;
                    T == ssl_user_with_client_id_in_cert_san_email;
                    T == ssl_user_with_client_id_in_cert_dn ->
    {rabbit, [
            {auth_backends, [rabbit_auth_backend_mqtt_mock]}
          ]
    };

auth_config(_) ->
    undefined.

init_per_testcase(T, Config) when T == ssl_user_auth_success;
                                  T == ssl_user_auth_failure ->
    Config1 = set_cert_user_on_default_vhost(Config),
    testcase_started(Config1, T);
init_per_testcase(T = ssl_user_vhost_parameter_mapping_success, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    Config2 = set_vhost_for_cert_user(Config1, User),
    testcase_started(Config2, T);
init_per_testcase(T = ssl_user_vhost_parameter_mapping_not_allowed, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config2),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config2, User, VhostForCertUser),
    testcase_started(Config2, T);
init_per_testcase(T = user_credentials_auth, Config) ->
    User = <<"new-user">>,
    Pass = <<"new-user-pass">>,
    ok = rabbit_ct_broker_helpers:add_user(Config, 0, User, Pass),
    ok = set_full_permissions(Config, User, <<"/">>),
    Config1 = rabbit_ct_helpers:set_config(Config, [{new_user, User},
                                                    {new_user_pass, Pass}]),
    testcase_started(Config1, T);
init_per_testcase(T = ssl_user_vhost_not_allowed, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    testcase_started(Config1, T);
init_per_testcase(T = ssl_user_vhost_parameter_mapping_vhost_does_not_exist, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config2),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    testcase_started(Config1, T);
init_per_testcase(T = port_vhost_mapping_success, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    testcase_started(Config1, T);
init_per_testcase(T = port_vhost_mapping_success_no_mapping, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    PortToVHostMappingParameter = [
        {<<"1">>,   <<"unlikely to exist">>},
        {<<"2">>,   <<"unlikely to exist">>}],
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_port_to_vhost_mapping, PortToVHostMappingParameter),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, VHost),
    testcase_started(Config1, T);
init_per_testcase(T = port_vhost_mapping_not_allowed, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, VHost),
    testcase_started(Config1, T);
init_per_testcase(T = port_vhost_mapping_vhost_does_not_exist, Config) ->
    User = <<"guest">>,
    Config1 = set_vhost_for_port_vhost_mapping_user(Config, User),
    rabbit_ct_broker_helpers:clear_permissions(Config1, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config1),
    rabbit_ct_broker_helpers:delete_vhost(Config1, VHost),
    testcase_started(Config1, T);
init_per_testcase(T = ssl_user_cert_vhost_mapping_takes_precedence_over_port_vhost_mapping, Config) ->
    Config1 = set_cert_user_on_default_vhost(Config),
    User = ?config(temp_ssl_user, Config1),
    Config2 = set_vhost_for_cert_user(Config1, User),
    Config3 = set_vhost_for_port_vhost_mapping_user(Config2, User),
    %% Given we revoke the vhost permissions that were set by set_vhost_for_port_vhost_mapping_user/2,
    %% we know that if connecting succeeds, the cert vhost mapping must have taken precedence.
    VhostForPortMapping = ?config(temp_vhost_for_port_mapping, Config3),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config3, User, VhostForPortMapping),
    ok = rabbit_ct_broker_helpers:clear_permissions(Config3, User, <<"/">>),
    testcase_started(Config3, T);
init_per_testcase(T, Config)
  when T =:= will_queue_create_permission_queue_read;
       T =:= will_queue_create_permission_exchange_write;
       T =:= will_queue_publish_permission_exchange_write;
       T =:= will_queue_publish_permission_topic_write;
       T =:= will_queue_delete_permission ->
    case ?config(mqtt_version, Config) of
        v4 -> {skip, "Will Delay Interval is an MQTT 5.0 feature"};
        v5 -> testcase_started(Config, T)
    end;
init_per_testcase(T, Config)
  when T =:= client_id_propagation;
       T =:= invalid_client_id_from_cert_san_dns;
       T =:= client_id_from_cert_san_dns;
       T =:= client_id_from_cert_san_dns_1;
       T =:= client_id_from_cert_san_email;
       T =:= client_id_from_cert_dn ->
    SetupProcess = setup_rabbit_auth_backend_mqtt_mock(Config),
    rabbit_ct_helpers:set_config(Config, {mock_setup_process, SetupProcess});

init_per_testcase(Testcase, Config) ->
    testcase_started(Config, Testcase).

get_client_cert_subject(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    CertFile = filename:join([CertsDir, "client", "cert.pem"]),
    {ok, CertBin} = file:read_file(CertFile),
    [{'Certificate', Cert, not_encrypted}] = public_key:pem_decode(CertBin),
    iolist_to_binary(rpc(Config, 0, rabbit_ssl, peer_cert_subject, [Cert])).

set_cert_user_on_default_vhost(Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    CertFile = filename:join([CertsDir, "client", "cert.pem"]),
    {ok, CertBin} = file:read_file(CertFile),
    [{'Certificate', Cert, not_encrypted}] = public_key:pem_decode(CertBin),
    UserBin = rpc(Config, 0, rabbit_ssl, peer_cert_auth_name, [Cert]),
    User = binary_to_list(UserBin),
    ok = rabbit_ct_broker_helpers:add_user(Config, 0, User, ""),
    ok = set_full_permissions(Config, User, <<"/">>),
    rabbit_ct_helpers:set_config(Config, {temp_ssl_user, User}).

set_vhost_for_cert_user(Config, User) ->
    VhostForCertUser = <<"vhost_for_cert_user">>,
    UserToVHostMappingParameter = [
        {rabbit_data_coercion:to_binary(User), VhostForCertUser},
        {<<"O=client,CN=unlikelytoexistuser">>, <<"vhost2">>}
    ],
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VhostForCertUser),
    ok = set_full_permissions(Config, User, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_default_vhosts, UserToVHostMappingParameter),
    rabbit_ct_helpers:set_config(Config, {temp_vhost_for_ssl_user, VhostForCertUser}).

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
    ok = set_full_permissions(Config, User, VhostForPortMapping),
    ok = rabbit_ct_broker_helpers:set_global_parameter(Config, mqtt_port_to_vhost_mapping, PortToVHostMappingParameter),
    rabbit_ct_helpers:set_config(Config, {temp_vhost_for_port_mapping, VhostForPortMapping}).

end_per_testcase(Testcase, Config) when Testcase == ssl_user_auth_success;
                                        Testcase == ssl_user_auth_failure;
                                        Testcase == ssl_user_vhost_not_allowed ->
    delete_cert_user(Config),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(TestCase, Config) when TestCase == ssl_user_vhost_parameter_mapping_success;
                                        TestCase == ssl_user_vhost_parameter_mapping_not_allowed ->
    delete_cert_user(Config),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, TestCase);
end_per_testcase(user_credentials_auth, Config) ->
    User = ?config(new_user, Config),
    {ok,_} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, ["delete_user", User]),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, user_credentials_auth);
end_per_testcase(ssl_user_vhost_parameter_mapping_vhost_does_not_exist, Config) ->
    delete_cert_user(Config),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, ssl_user_vhost_parameter_mapping_vhost_does_not_exist);
end_per_testcase(Testcase, Config) when Testcase == port_vhost_mapping_success;
                                        Testcase == port_vhost_mapping_not_allowed;
                                        Testcase == port_vhost_mapping_success_no_mapping ->
    User = <<"guest">>,
    ok = set_full_permissions(Config, User, <<"/">>),
    VHost = ?config(temp_vhost_for_port_mapping, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHost),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(T = port_vhost_mapping_vhost_does_not_exist, Config) ->
    User = <<"guest">>,
    ok = set_full_permissions(Config, User, <<"/">>),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, T);
end_per_testcase(T = ssl_user_cert_vhost_mapping_takes_precedence_over_port_vhost_mapping, Config) ->
    delete_cert_user(Config),
    VhostForCertUser = ?config(temp_vhost_for_ssl_user, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VhostForCertUser),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_default_vhosts),

    VHostForPortVHostMapping = ?config(temp_vhost_for_port_mapping, Config),
    ok = rabbit_ct_broker_helpers:delete_vhost(Config, VHostForPortVHostMapping),
    ok = rabbit_ct_broker_helpers:clear_global_parameter(Config, mqtt_port_to_vhost_mapping),
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, T);
end_per_testcase(T, Config) when T == queue_bind_permission;
                                 T == queue_unbind_permission;
                                 T == queue_consume_permission;
                                 T == queue_consume_permission_on_connect;
                                 T == subscription_queue_delete_permission;
                                 T == will_queue_create_permission_queue_read,
                                 T == will_queue_create_permission_exchange_write,
                                 T == will_queue_delete_permission;
                                 T == queue_declare_permission;
                                 T == publish_permission;
                                 T == publish_permission_will_message;
                                 T == topic_read_permission;
                                 T == topic_write_permission;
                                 T == topic_write_permission_variable_expansion;
                                 T == loopback_user_connects_from_remote_host ->
    %% So let's wait before logs are surely flushed
    Marker = "MQTT_AUTH_SUITE_MARKER",
    rpc(Config, 0, rabbit_log, error, [Marker]),
    wait_log(Config, [{[Marker], fun () -> stop end}]),

    %% Preserve file contents in case some investigation is needed, before truncating.
    file:copy(?config(log_location, Config), iolist_to_binary([?config(log_location, Config), ".", atom_to_binary(T)])),

    %% And provide an empty log file for the next test in this group
    file:write_file(?config(log_location, Config), <<>>),

    close_all_connections(Config),

    rabbit_ct_helpers:testcase_finished(Config, T);

end_per_testcase(T, Config)
   when T =:= client_id_propagation;
       T =:= invalid_client_id_from_cert_san_dns;
       T =:= client_id_from_cert_san_dns;
       T =:= client_id_from_cert_san_dns_1;
       T =:= client_id_from_cert_san_email;
       T =:= client_id_from_cert_dn ->
    SetupProcess = ?config(mock_setup_process, Config),
    SetupProcess ! stop,
    close_all_connections(Config);

end_per_testcase(Testcase, Config) ->
    close_all_connections(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

close_all_connections(Config) ->
    rpc(Config, 0, rabbit_mqtt, close_local_client_connections,
        [end_per_testcase]).

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

client_id_from_cert_san_dns(Config) ->
    ExpectedClientId = <<"rabbit_client_id">>, % Found in the client's certificate as SAN type DNS
    MqttClientId = ExpectedClientId,
    {ok, C} = connect_ssl(MqttClientId, Config),
    {ok, _} = emqtt:connect(C),
    [{authentication, AuthProps}] = rpc(Config, 0,
                                        rabbit_auth_backend_mqtt_mock,
                                        get,
                                        [authentication]),
    ?assertEqual(ExpectedClientId, proplists:get_value(client_id, AuthProps)),
    ok = emqtt:disconnect(C).

client_id_from_cert_san_dns_1(Config) ->
    ExpectedClientId = <<"rabbit_client_id_ext">>, % Found in the client's certificate as SAN type DNS
    MqttClientId = ExpectedClientId,
    {ok, C} = connect_ssl(MqttClientId, Config),
    {ok, _} = emqtt:connect(C),
    [{authentication, AuthProps}] = rpc(Config, 0,
                                        rabbit_auth_backend_mqtt_mock,
                                        get,
                                        [authentication]),
    ?assertEqual(ExpectedClientId, proplists:get_value(client_id, AuthProps)),
    ok = emqtt:disconnect(C).

client_id_from_cert_san_email(Config) ->
    ExpectedClientId = <<"rabbit_client@localhost">>, % Found in the client's certificate as SAN type email
    MqttClientId = ExpectedClientId,
    {ok, C} = connect_ssl(MqttClientId, Config),
    {ok, _} = emqtt:connect(C),
    [{authentication, AuthProps}] = rpc(Config, 0,
                                        rabbit_auth_backend_mqtt_mock,
                                        get,
                                        [authentication]),
    ?assertEqual(ExpectedClientId, proplists:get_value(client_id, AuthProps)),
    ok = emqtt:disconnect(C).

client_id_from_cert_dn(Config) ->
    ExpectedClientId = get_client_cert_subject(Config), % subject = distinguished_name
    MqttClientId = ExpectedClientId,
    {ok, C} = connect_ssl(MqttClientId, Config),
    {ok, _} = emqtt:connect(C),
    [{authentication, AuthProps}] = rpc(Config, 0,
                                        rabbit_auth_backend_mqtt_mock,
                                        get,
                                        [authentication]),
    ?assertEqual(ExpectedClientId, proplists:get_value(client_id, AuthProps)),
    ok = emqtt:disconnect(C).

invalid_client_id_from_cert_san_dns(Config) ->
    MqttClientId = <<"other_client_id">>,
    {ok, C} = connect_ssl(MqttClientId, Config),
    unlink(C),
    {error, {client_identifier_not_valid, _}} = emqtt:connect(C).

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

ssl_user_cert_vhost_mapping_takes_precedence_over_port_vhost_mapping(Config) ->
    expect_successful_connection(fun connect_ssl/1, Config).

connect_anonymous(Config) ->
    connect_anonymous(Config, <<"simpleClient">>).

connect_anonymous(Config, ClientId) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    emqtt:start_link([{host, "localhost"},
                      {port, P},
                      {clientid, ClientId},
                      {proto_ver, ?config(mqtt_version, Config)}]).

connect_ssl(Config) ->
    connect_ssl(<<"simpleClient">>, Config).

connect_ssl(ClientId, Config) ->
    CertsDir = ?config(rmq_certsdir, Config),
    SSLConfig = [{cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                 {certfile, filename:join([CertsDir, "client", "cert.pem"])},
                 {keyfile, filename:join([CertsDir, "client", "key.pem"])},
                 {server_name_indication, "localhost"}],
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt_tls),
    emqtt:start_link([{host, "localhost"},
                      {port, P},
                      {clientid, ClientId},
                      {proto_ver, ?config(mqtt_version, Config)},
                      {ssl, true},
                      {ssl_opts, SSLConfig}]).

setup_rabbit_auth_backend_mqtt_mock(Config) ->
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
        {ok, SP} -> SP
    after
        30_000 -> ct:fail("timeout waiting for rabbit_auth_backend_mqtt_mock:setup/1")
    end.

client_id_propagation(Config) ->
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

    emqtt:disconnect(C).


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
queue_bind_permission(Config) ->
    ExpectedLogs =
    ["MQTT resource access refused: write access to queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' "
     "refused for user 'mqtt-user'",
     "Failed to add binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' for topic filter test/topic: access_refused"
    ],
    test_subscribe_permissions_combination(<<".*">>, <<"">>, <<".*">>, Config, ExpectedLogs).

queue_unbind_permission(Config) ->
    User = ?config(mqtt_user, Config),
    Vhost = ?config(mqtt_vhost, Config),
    set_full_permissions(Config, User, Vhost),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    Opts = [{host, "localhost"},
            {port, P},
            {proto_ver, ?config(mqtt_version, Config)},
            {clientid, User},
            {username, User},
            {password, ?config(mqtt_password, Config)}],
    {ok, C1} = emqtt:start_link(non_clean_sess_opts() ++ Opts),
    {ok, _} = emqtt:connect(C1),
    Topic = <<"my/topic">>,
    ?assertMatch({ok, _Properties, [1]},
                 emqtt:subscribe(C1, Topic, qos1)),
    ok = emqtt:disconnect(C1),

    %% Revoke write access to qos1 queue.
    rabbit_ct_broker_helpers:set_permissions(Config, User, Vhost, <<".*">>, <<"mqtt-subscription-mqtt-userqos0">>, <<".*">>),
    {ok, C2} = emqtt:start_link(non_clean_sess_opts() ++ Opts),
    {ok, _} = emqtt:connect(C2),
    process_flag(trap_exit, true),
    %% We subscribe with the same client ID to the same topic again, but this time with QoS 0.
    %% Therefore we trigger the qos1 queue to be unbound (and the qos0 queue to be bound).
    %% However, unbinding requires write access to the qos1 queue, which we don't have anymore.
    ExpectedReasonCode = suback_error_code(?RC_NOT_AUTHORIZED, Config),
    ?assertMatch({ok, _Properties, [ExpectedReasonCode]},
                 emqtt:subscribe(C2, Topic, qos0)),
    ok = assert_connection_closed(C2),
    ExpectedLogs =
    ["MQTT resource access refused: write access to queue 'mqtt-subscription-mqtt-userqos1' "
     "in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
     "Failed to remove binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
     "'mqtt-subscription-mqtt-userqos1' in vhost 'mqtt-vhost' for topic filter my/topic: access_refused"
    ],
    wait_log(Config, [?FAIL_IF_CRASH_LOG, {ExpectedLogs, fun () -> stop end}]),

    %% Clean up the qos1 queue by connecting with clean session.
    set_full_permissions(Config, User, Vhost),
    {ok, C3} = emqtt:start_link([{clean_start, true} | Opts]),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:disconnect(C3).

queue_consume_permission(Config) ->
    ExpectedLogs =
    ["MQTT resource access refused: read access to queue "
     "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' "
     "refused for user 'mqtt-user'"],
    test_subscribe_permissions_combination(<<".*">>, <<".*">>, <<"^amq\\.topic">>, Config, ExpectedLogs).

subscription_queue_delete_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C1} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 non_clean_sess_opts()),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, {<<"test/topic">>, qos1}),
    ok = emqtt:disconnect(C1),

    set_permissions(<<"^mqtt-will-">>, ".*", ".*", Config),
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
          "Rejected MQTT connection .* with Connect Reason Code 135"],
         fun() -> stop end}
      ]),
    ok.

%% queue.declare with DLX requires permission to read from queue
will_queue_create_permission_queue_read(Config) ->
    set_permissions(<<".*">>, ".*", <<>>, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    disconnect_with_delayed_will(ClientId, Config),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: read access to queue "
                        "'mqtt-will-~s' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "failed to schedule delayed Will Message"],
         fun() -> stop end}
      ]),
    ok.

%% queue.declare with DLX requires permission to write to DLX exchange
will_queue_create_permission_exchange_write(Config) ->
    set_permissions(<<".*">>, <<>>, <<".*">>, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    disconnect_with_delayed_will(ClientId, Config),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{["MQTT resource access refused: write access to exchange "
          "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
          "failed to schedule delayed Will Message"],
         fun() -> stop end}
      ]),
    ok.

will_queue_publish_permission_exchange_write(Config) ->
    set_permissions(<<".*">>, <<"amq.topic">>, <<".*">>, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    disconnect_with_delayed_will(ClientId, Config),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{["MQTT resource access refused: write access to exchange "
          "'amq.default' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
          "failed to schedule delayed Will Message"],
         fun() -> stop end}
      ]),
    ok.

%% Dead lettering to a topic exchange requires writing to the topic.
will_queue_publish_permission_topic_write(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    set_topic_permissions("", ".*", Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    disconnect_with_delayed_will(ClientId, Config),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{["MQTT topic access refused: write access to topic 'my.topic' in exchange "
          "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
          "failed to schedule delayed Will Message"],
         fun() -> stop end}
      ]),
    ok.

will_queue_delete_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    disconnect_with_delayed_will(ClientId, Config),
    set_permissions(<<>>, ".*", ".*", Config),
    %% Now we have a Will queue that user doesn't have permission to delete.
    %% Resuming the session should fail.
    {ok, C2} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 [{clean_start, false}]),
    unlink(C2),
    ?assertMatch({error, _}, emqtt:connect(C2)),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: configure access to queue "
                        "'mqtt-will-~s' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "Rejected MQTT connection .* with Connect Reason Code 135"],
         fun() -> stop end}
      ]),
    ok.

disconnect_with_delayed_will(ClientId, Config) ->
    {ok, C} = connect_user(
                ?config(mqtt_user, Config),
                ?config(mqtt_password, Config),
                Config,
                ClientId,
                non_clean_sess_opts() ++
                [{properties, #{'Session-Expiry-Interval' => 3}},
                 {will_props, #{'Will-Delay-Interval' => 3}},
                 {will_topic, <<"my/topic">>},
                 {will_payload, <<"msg">>}]),
    {ok, _} = emqtt:connect(C),
    ok = emqtt:disconnect(C, ?RC_DISCONNECT_WITH_WILL).

queue_consume_permission_on_connect(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    ClientId = <<"queue_consume_permission_on_connect">>,
    {ok, C1} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 non_clean_sess_opts()),
    {ok, _} = emqtt:connect(C1),
    {ok, _, _} = emqtt:subscribe(C1, {<<"test/topic">>, qos1}),
    ok = emqtt:disconnect(C1),

    set_permissions(".*", ".*", "^amq\\.topic", Config),
    {ok, C2} = connect_user(
                 ?config(mqtt_user, Config),
                 ?config(mqtt_password, Config),
                 Config,
                 ClientId,
                 non_clean_sess_opts()),
    unlink(C2),
    ?assertMatch({error, _},
                 emqtt:connect(C2)),
    wait_log(
      Config,
      [?FAIL_IF_CRASH_LOG
       ,{[io_lib:format("MQTT resource access refused: read access to queue "
                        "'mqtt-subscription-~sqos1' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                        [ClientId]),
          "Rejected MQTT connection .* with Connect Reason Code 135"],
         fun () -> stop end}
      ]),
    ok.

queue_declare_permission(Config) ->
    set_permissions("^mqtt-will-", ".*", ".*", Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    {ok, C} = connect_user(
                ?config(mqtt_user, Config),
                ?config(mqtt_password, Config),
                Config,
                ClientId,
                [{clean_start, true}]),
    {ok, _} = emqtt:connect(C),

    process_flag(trap_exit, true),

    ExpectedReasonCode = suback_error_code(?RC_NOT_AUTHORIZED, Config),
    {ok, _, [ExpectedReasonCode]} = emqtt:subscribe(C, <<"test/topic">>, qos0),
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

publish_permission(Config) ->
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
publish_permission_will_message(Config) ->
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

topic_read_permission(Config) ->
    set_permissions(".*", ".*", ".*", Config),
    set_topic_permissions("^allow-write\\..*", "^allow-read\\..*", Config),
    C = open_mqtt_connection(Config),

    %% Check topic permission setup is working.
    {ok, _, [0]} = emqtt:subscribe(C, <<"allow-read/some/topic">>),

    process_flag(trap_exit, true),

    ExpectedReasonCode = suback_error_code(?RC_NOT_AUTHORIZED, Config),
    {ok, _, [ExpectedReasonCode]} = emqtt:subscribe(C, <<"test/topic">>),
    ok = assert_connection_closed(C),
    wait_log(Config,
             [?FAIL_IF_CRASH_LOG,
              {["MQTT topic access refused: read access to topic 'test.topic' in exchange "
                "'amq.topic' in vhost 'mqtt-vhost' refused for user 'mqtt-user'",
                "Failed to add binding between exchange 'amq.topic' in vhost 'mqtt-vhost' and queue "
                "'mqtt-subscription-mqtt-userqos0' in vhost 'mqtt-vhost' for topic filter test/topic: access_refused"
               ],
               fun () -> stop end}
             ]),
    ok.

topic_write_permission(Config) ->
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
                "Rejected MQTT connection .* with Connect Reason Code 135"],
               fun () -> stop end}
             ]),

    true = rpc(Config, 0, meck, validate, [Mod]),
    ok = rpc(Config, 0, meck, unload, [Mod]).

%% No specific configure, write, or read permissions should be required for only connecting.
connect_permission(Config) ->
    set_permissions("", "", "", Config),
    C = open_mqtt_connection(Config),
    ok = emqtt:disconnect(C).

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
    {ok, C1} = emqtt:start_link([{proto_ver, ?config(mqtt_version, Config)} | Opts]),
    {ok, _} = emqtt:connect(C1),
    process_flag(trap_exit, true),
    %% In v4 and v5, we expect to receive a failure return code for our subscription in the SUBACK packet.
    ExpectedReasonCode = suback_error_code(?RC_NOT_AUTHORIZED, Config),
    ?assertMatch({ok, _Properties, [ExpectedReasonCode]},
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
                     [{host, "localhost"}, {port, P}, {proto_ver, ?config(mqtt_version, Config)}]).

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
    ExpectedError = expected_connection_limit_error(Config),
    unlink(C3),
    ?assertMatch({error, {ExpectedError, _}}, emqtt:connect(C3)),
    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2),
    ok = rabbit_ct_broker_helpers:clear_vhost_limit(Config, 0, <<"/">>).

vhost_queue_limit(Config) ->
    ok = rabbit_ct_broker_helpers:set_vhost_limit(Config, 0, <<"/">>, max_queues, 1),
    {ok, C} = connect_anonymous(Config),
    {ok, _} = emqtt:connect(C),
    process_flag(trap_exit, true),
    %% qos0 queue can be created, qos1 queue fails to be created.
    ExpectedRc = suback_error_code(?RC_QUOTA_EXCEEDED, Config),
    ?assertMatch({ok, _Properties, [0, ExpectedRc, ExpectedRc]},
                 emqtt:subscribe(C, [{<<"topic1">>, qos0},
                                     {<<"topic2">>, qos1},
                                     {<<"topic3">>, qos1}])),
    ok = assert_connection_closed(C),
    ok = rabbit_ct_broker_helpers:clear_vhost_limit(Config, 0, <<"/">>).

user_connection_limit(Config) ->
    DefaultUser = <<"guest">>,
    ok = rabbit_ct_broker_helpers:set_user_limits(Config, DefaultUser, #{max_connections => 1}),
    {ok, C1} = connect_anonymous(Config, <<"client1">>),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = connect_anonymous(Config, <<"client2">>),
    ExpectedError = expected_connection_limit_error(Config),
    unlink(C2),
    ?assertMatch({error, {ExpectedError, _}}, emqtt:connect(C2)),
    ok = emqtt:disconnect(C1),
    ok = rabbit_ct_broker_helpers:clear_user_limits(Config, DefaultUser, max_connections).

expected_connection_limit_error(Config) ->
    case ?config(mqtt_version, Config) of
        v4 ->
            unauthorized_client;
        v5 ->
            %% MQTT 5.0 has more specific error codes.
            quota_exceeded
    end.

suback_error_code(ReasonCode, Config) ->
    case ?config(mqtt_version, Config) of
        v4 ->
            ?SUBACK_FAILURE;
        v5 ->
            ReasonCode
    end.

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
        30_000 ->
            ct:fail("timed out waiting for exit message")
    end.

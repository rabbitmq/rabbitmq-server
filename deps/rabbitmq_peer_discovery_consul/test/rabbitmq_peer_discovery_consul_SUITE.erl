%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2022 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbitmq_peer_discovery_consul_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    [
     {group, registration_body_tests}
     , {group, registration_tests}
     , {group, unregistration_tests}
     , {group, list_node_tests}
     , {group, other_tests}
     , {group, lock_tests}
    ].

groups() ->
    [
     {registration_body_tests, [], [
                 registration_body_simple_case,
                 registration_body_svc_addr_set_via_env_var,
                 registration_body_svc_ttl_set_via_env_var,
                 registration_body_svc_tags_set_via_env_var,
                 registration_body_deregister_after_set_via_env_var,
                 registration_body_ttl_and_deregister_after_both_unset_via_env_var,
                 registration_body_ttl_unset_and_deregister_after_set_via_env_var,
                 service_id_all_defaults_test,
                 service_id_with_unset_address_test,
                 service_ttl_default_test
                ]}
     , {registration_tests, [], [
                 registration_with_all_default_values_test,
                 registration_with_cluster_name_test,
                 registration_without_acl_token_test,
                 registration_with_acl_token_test,
                 registration_with_auto_addr_test,
                 registration_with_auto_addr_from_nodename_test,
                 registration_with_auto_addr_nic_test,
                 registration_with_auto_addr_nic_issue_12_test,
                 registration_generic_error_test
                ]}
     , {unregistration_tests, [], [
                 unregistration_with_all_defaults_test,
                 unregistration_without_acl_token_test,
                 unregistration_with_acl_token_test,
                 unregistration_with_generic_error_test
                ]}
     , {list_node_tests, [], [
                 list_nodes_default_values_test,
                 list_nodes_without_acl_token_test,
                 list_nodes_with_acl_token_test,
                 list_nodes_with_cluster_name_token_test,
                 list_nodes_with_cluster_name_and_acl_token_test,
                 list_nodes_return_value_basic_test,
                 list_nodes_return_value_basic_long_node_name_test,
                 list_nodes_return_value_long_node_name_and_custom_domain_test,
                 list_nodes_return_value_srv_address_test,
                 list_nodes_return_value_nodes_in_warning_state_included_test,
                 list_nodes_return_value_nodes_in_warning_state_filtered_out_test
                ]}
     , {other_tests, [], [
                 health_check_with_all_defaults_test,
                 health_check_without_acl_token_test,
                 health_check_with_acl_token_test,
                 health_check_error_handling_test
                ]}
    , {lock_tests, [], [
                        startup_lock_path_with_prefix_test,
                        startup_lock_path_default_value_test,
                        startup_lock_path_with_cluster_name_test,
                        create_session_with_token_test,
                        create_session_without_token_test,
                        get_lock_status_with_token_test,
                        get_lock_status_with_session_test,
                        get_lock_status_without_session_test,
                        wait_for_lock_release_with_session_with_token_test,
                        wait_for_lock_release_with_session_without_token_test,
                        wait_for_lock_release_without_session_test,
                        acquire_lock_with_token_test,
                        acquire_lock_not_acquired_test,
                        acquire_lock_successfully_acquired_test,
                        release_lock_with_token_test,
                        release_lock_not_released_test,
                        release_lock_successfully_released_test,
                        consul_kv_read_custom_values_test,
                        consul_kv_read_default_values_test,
                        consul_kv_write_custom_values_test,
                        consul_kv_write_default_values_test,
                        consul_session_renew_custom_values_test,
                        consul_session_renew_default_values_test,
                        consul_session_create_custom_values_test,
                        consul_session_create_default_values_test
                       ]}
    ].

init_per_group(_, Config) -> Config.
end_per_group(_, Config)  -> Config.

reset() ->
    meck:unload(),
    application:unset_env(rabbit, cluster_formation),
    [os:unsetenv(Var) || Var <- [
                                 "CLUSTER_NAME",
                                 "CONSUL_SCHEME",
                                 "CONSUL_HOST",
                                 "CONSUL_PORT",
                                 "CONSUL_ACL_TOKEN",
                                 "CONSUL_SVC",
                                 "CONSUL_SVC_ADDR",
                                 "CONSUL_SVC_ADDR_AUTO",
                                 "CONSUL_SVC_ADDR_NIC",
                                 "CONSUL_SVC_ADDR_NODENAME",
                                 "CONSUL_SVC_PORT",
                                 "CONSUL_SVC_TTL",
                                 "CONSUL_SVC_TAGS",
                                 "CONSUL_DOMAIN",
                                 "CONSUL_DEREGISTER_AFTER",
                                 "CONSUL_INCLUDE_NODES_WITH_WARNINGS",
                                 "CONSUL_USE_LONGNAME",
                                 "CONSUL_LOCK_PREFIX"
                                ]].

init_per_testcase(_TC, Config) ->
    reset(),
    meck:new(rabbit_log, []),
    meck:new(rabbit_peer_discovery_httpc, [passthrough]),
    Config.

end_per_testcase(_TC, Config) ->
    reset(),
    Config.


%%%
%%% Testcases
%%%

-define(CONSUL_CHECK_NOTES, 'RabbitMQ Consul-based peer discovery plugin TTL check').

registration_body_simple_case(_Config) ->
    Expectation = [{'ID',rabbitmq},
                       {'Name', rabbitmq},
                       {'Port', 5672},
                       {'Check',
                         [{'Notes', ?CONSUL_CHECK_NOTES},
                          {'TTL',   '30s'},
                          {'Status', 'passing'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_svc_addr_set_via_env_var(_Config) ->
    os:putenv("CONSUL_SVC_ADDR", "mercurio"),
    Expectation = [{'ID', 'rabbitmq:mercurio'},
                   {'Name', rabbitmq},
                   {'Address', mercurio},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL',   '30s'},
                     {'Status', 'passing'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_svc_ttl_set_via_env_var(_Config) ->
    os:putenv("CONSUL_SVC_TTL", "257"),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL',   '257s'},
                     {'Status', 'passing'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_svc_tags_set_via_env_var(_Config) ->
    os:putenv("CONSUL_SVC_TAGS", "urlprefix-:5672 proto=tcp, mq, mq server"),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL',   '30s'},
                     {'Status', 'passing'}]},
                   {'Tags',['urlprefix-:5672 proto=tcp',mq,'mq server']}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_deregister_after_set_via_env_var(_Config) ->
    os:putenv("CONSUL_DEREGISTER_AFTER", "520"),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL','30s'},
                     {'Status', 'passing'},
                     {'DeregisterCriticalServiceAfter','520s'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_ttl_and_deregister_after_both_unset_via_env_var(_Config) ->
    os:putenv("CONSUL_DEREGISTER_AFTER", ""),
    os:putenv("CONSUL_SVC_TTL", ""),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

%% "deregister after" won't be enabled if TTL isn't set
registration_body_ttl_unset_and_deregister_after_set_via_env_var(_Config) ->
    os:putenv("CONSUL_DEREGISTER_AFTER", "120"),
    os:putenv("CONSUL_SVC_TTL", ""),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

service_id_all_defaults_test(_Config) ->
    ?assertEqual("rabbitmq", rabbit_peer_discovery_consul:service_id()).

service_id_with_unset_address_test(_Config) ->
    os:putenv("CONSUL_SVC", "rmq"),
    os:putenv("CONSUL_SVC_ADDR", "mercurio.local"),
    os:putenv("CONSUL_SVC_ADDR_AUTO", "false"),
    os:putenv("CONSUL_SVC_ADDR_NODENAME", ""),
    ?assertEqual("rmq:mercurio.local", rabbit_peer_discovery_consul:service_id()).

service_ttl_default_test(_Config) ->
   ?assertEqual("30s", rabbit_peer_discovery_consul:service_ttl(30)).

list_nodes_default_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
               ?assertEqual("http", Scheme),
               ?assertEqual("localhost", Host),
               ?assertEqual(8500, Port),
               ?assertEqual("v1/health/service/rabbitmq", Path),
               ?assertEqual([passing], Args),
               ?assertEqual([], Headers),
               ?assertEqual([], HttpOpts),
               {error, "testing"}
             end),
           ?assertEqual({ok, {[], disc}}, rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_without_acl_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
               ?assertEqual("https", Scheme),
               ?assertEqual("consul.service.consul", Host),
               ?assertEqual(8501, Port),
               ?assertEqual("v1/health/service/rabbit", Path),
               ?assertEqual([passing], Args),
               ?assertEqual([], Headers),
               ?assertEqual([], HttpOpts),
               {error, "testing"}
             end),
           os:putenv("CONSUL_SCHEME", "https"),
           os:putenv("CONSUL_HOST", "consul.service.consul"),
           os:putenv("CONSUL_PORT", "8501"),
           os:putenv("CONSUL_SVC", "rabbit"),
           ?assertEqual({ok, {[], disc}}, rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_with_acl_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
               ?assertEqual("http", Scheme),
               ?assertEqual("consul.service.consul", Host),
               ?assertEqual(8500, Port),
               ?assertEqual("v1/health/service/rabbitmq", Path),
               ?assertEqual([passing], Args),
               ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
               ?assertEqual([], HttpOpts),
               {error, "testing"}
             end),
           os:putenv("CONSUL_HOST", "consul.service.consul"),
           os:putenv("CONSUL_PORT", "8500"),
           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           ?assertEqual({ok, {[], disc}}, rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_with_cluster_name_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
               ?assertEqual("http", Scheme),
               ?assertEqual("consul.service.consul", Host),
               ?assertEqual(8500, Port),
               ?assertEqual("v1/health/service/rabbitmq", Path),
               ?assertEqual([passing], Args),
               ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
               ?assertEqual([], HttpOpts),
               {error, "testing"}
             end),
           os:putenv("CONSUL_HOST", "consul.service.consul"),
           os:putenv("CONSUL_PORT", "8500"),
           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           ?assertEqual({ok, {[], disc}}, rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_with_cluster_name_and_acl_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
               ?assertEqual("http", Scheme),
               ?assertEqual("localhost", Host),
               ?assertEqual(8500, Port),
               ?assertEqual("v1/health/service/rabbitmq", Path),
               ?assertEqual([passing, {tag, "qa"}], Args),
               ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
               ?assertEqual([], HttpOpts),
               {error, "testing"}
             end),
           os:putenv("CLUSTER_NAME", "qa"),
           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           ?assertEqual({ok, {[], disc}}, rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_basic_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host, "localhost"},
                                                           {consul_port, 8500}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(_, _, _, _, _, _, _) ->
               Body = "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
               rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body))
             end),
           ?assertEqual({ok, {['rabbit@rabbit1', 'rabbit@rabbit2'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_basic_long_node_name_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host,         "localhost"},
                                                           {consul_port,         8500},
                                                           {consul_use_longname, true}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(_, _, _, _, _, _, _) ->
               Body = "[{\"Node\": {\"Node\": \"rabbit2\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
               rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body))
             end),
           ?assertEqual({ok, {['rabbit@rabbit1.node.consul', 'rabbit@rabbit2.node.consul'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_long_node_name_and_custom_domain_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host,         "localhost"},
                                                           {consul_port,         8500},
                                                           {consul_use_longname, true},
                                                           {consul_domain,       "internal"}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(_, _, _, _, _, _, _) ->
               Body = "[{\"Node\": {\"Node\": \"rabbit2\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
               rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body))
             end),
           ?assertEqual({ok, {['rabbit@rabbit1.node.internal', 'rabbit@rabbit2.node.internal'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_srv_address_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host,         "localhost"},
                                                           {consul_port,         8500}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(_, _, _, _, _, _, _) ->
               Body = "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
               rabbit_json:try_decode(rabbit_data_coercion:to_binary(Body))
             end),
           ?assertEqual({ok, {['rabbit@172.16.4.51', 'rabbit@172.172.16.51'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_nodes_in_warning_state_included_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host,         "localhost"},
                                                           {consul_port,         8500}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(_, _, _, _, [], _, _) ->
                        rabbit_json:try_decode(list_of_nodes_with_warnings());
                   (_, _, _, _, [passing], _, _) ->
                        rabbit_json:try_decode(list_of_nodes_without_warnings())
             end),
           os:putenv("CONSUL_INCLUDE_NODES_WITH_WARNINGS", "true"),
           ?assertEqual({ok, {['rabbit@172.16.4.51'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

list_nodes_return_value_nodes_in_warning_state_filtered_out_test(_Config) ->
    application:set_env(rabbit, cluster_formation,
                        [
                         {peer_discovery_backend,         rabbit_peer_discovery_consul},
                         {peer_discovery_consul,          [
                                                           {consul_host,         "localhost"},
                                                           {consul_port,         8500}
                                                          ]}
                        ]),
    meck:expect(rabbit_peer_discovery_httpc, get,
             fun(_, _, _, _, [], _, _) ->
                     rabbit_json:try_decode(list_of_nodes_with_warnings());
                (_, _, _, _, [passing], _, _) ->
                     rabbit_json:try_decode(list_of_nodes_without_warnings())
             end),
           os:putenv("CONSUL_INCLUDE_NODES_WITH_WARNINGS", "false"),
           ?assertEqual({ok, {['rabbit@172.16.4.51', 'rabbit@172.172.16.51'], disc}},
                        rabbit_peer_discovery_consul:list_nodes()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)).

registration_with_all_default_values_test(_Config) ->
          meck:expect(rabbit_log, debug, fun(_Message) -> ok end),
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("http", Scheme),
              ?assertEqual("localhost", Host),
              ?assertEqual(8500, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([], Headers),
              Expect = <<"{\"ID\":\"rabbitmq\",\"Name\":\"rabbitmq\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_log)),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)).

registration_with_cluster_name_test(_Config) ->
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("http", Scheme),
              ?assertEqual("localhost", Host),
              ?assertEqual(8500, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([], Headers),
              Expect = <<"{\"ID\":\"rabbitmq\",\"Name\":\"rabbitmq\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"},\"Tags\":[\"test-rabbit\"]}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CLUSTER_NAME", "test-rabbit"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)).

registration_without_acl_token_test(_Config) ->
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("https", Scheme),
              ?assertEqual("consul.service.consul", Host),
              ?assertEqual(8501, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([], Headers),
              Expect = <<"{\"ID\":\"rabbit:10.0.0.1\",\"Name\":\"rabbit\",\"Address\":\"10.0.0.1\",\"Port\":5671,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CONSUL_SCHEME", "https"),
          os:putenv("CONSUL_HOST", "consul.service.consul"),
          os:putenv("CONSUL_PORT", "8501"),
          os:putenv("CONSUL_SVC", "rabbit"),
          os:putenv("CONSUL_SVC_ADDR", "10.0.0.1"),
          os:putenv("CONSUL_SVC_PORT", "5671"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)).

registration_with_acl_token_test(_Config) ->
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("https", Scheme),
              ?assertEqual("consul.service.consul", Host),
              ?assertEqual(8501, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([], Headers),
              Expect = <<"{\"ID\":\"rabbit:10.0.0.1\",\"Name\":\"rabbit\",\"Address\":\"10.0.0.1\",\"Port\":5671,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CONSUL_SCHEME", "https"),
          os:putenv("CONSUL_HOST", "consul.service.consul"),
          os:putenv("CONSUL_PORT", "8501"),
          os:putenv("CONSUL_SVC", "rabbit"),
          os:putenv("CONSUL_SVC_ADDR", "10.0.0.1"),
          os:putenv("CONSUL_SVC_PORT", "5671"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)).

registration_with_auto_addr_test(_Config) ->
           meck:new(rabbit_peer_discovery_util, [passthrough]),
           meck:expect(rabbit_peer_discovery_util, node_hostname, fun(true)  -> "bob.consul.node";
                                                                     (false) -> "bob" end),
           meck:expect(rabbit_peer_discovery_httpc, put,
             fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
               ?assertEqual("http", Scheme),
               ?assertEqual("consul.service.consul", Host),
               ?assertEqual(8500, Port),
               ?assertEqual("v1/agent/service/register", Path),
               ?assertEqual([], Args),
               ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
               Expect = <<"{\"ID\":\"rabbitmq:bob\",\"Name\":\"rabbitmq\",\"Address\":\"bob\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
               ?assertEqual(Expect, Body),
               {ok, []}
             end),
           os:putenv("CONSUL_HOST", "consul.service.consul"),
           os:putenv("CONSUL_PORT", "8500"),
           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           os:putenv("CONSUL_SVC_ADDR_AUTO", "true"),
           ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
           ?assert(meck:validate(rabbit_peer_discovery_httpc)),
           ?assert(meck:validate(rabbit_peer_discovery_util)).

registration_with_auto_addr_from_nodename_test(_Config) ->
          meck:new(rabbit_peer_discovery_util, [passthrough]),
          meck:expect(rabbit_peer_discovery_util, node_hostname, fun(true)  -> "bob.consul.node";
                                                                    (false) -> "bob" end),
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("http", Scheme),
              ?assertEqual("consul.service.consul", Host),
              ?assertEqual(8500, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
              Expect = <<"{\"ID\":\"rabbitmq:bob.consul.node\",\"Name\":\"rabbitmq\",\"Address\":\"bob.consul.node\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CONSUL_HOST", "consul.service.consul"),
          os:putenv("CONSUL_PORT", "8500"),
          os:putenv("CONSUL_ACL_TOKEN", "token-value"),
          os:putenv("CONSUL_SVC_ADDR_AUTO", "true"),
          os:putenv("CONSUL_SVC_ADDR_NODENAME", "true"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)),
          ?assert(meck:validate(rabbit_peer_discovery_util)).

registration_with_auto_addr_nic_test(_Config) ->
          meck:new(rabbit_peer_discovery_util, [passthrough]),
          meck:expect(rabbit_peer_discovery_util, nic_ipv4,
            fun(NIC) ->
              ?assertEqual("en0", NIC),
              {ok, "172.16.4.50"}
            end),
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("http", Scheme),
              ?assertEqual("consul.service.consul", Host),
              ?assertEqual(8500, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
              Expect = <<"{\"ID\":\"rabbitmq:172.16.4.50\",\"Name\":\"rabbitmq\",\"Address\":\"172.16.4.50\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CONSUL_HOST", "consul.service.consul"),
          os:putenv("CONSUL_PORT", "8500"),
          os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           os:putenv("CONSUL_SVC_ADDR_AUTO", "true"),
          os:putenv("CONSUL_SVC_ADDR_NIC", "en0"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)),
          ?assert(meck:validate(rabbit_peer_discovery_util)).

registration_with_auto_addr_nic_issue_12_test(_Config) ->
          meck:new(rabbit_peer_discovery_util, [passthrough]),
          meck:expect(rabbit_peer_discovery_util, nic_ipv4,
            fun(NIC) ->
              ?assertEqual("en0", NIC),
              {ok, "172.16.4.50"}
            end),
          meck:expect(rabbit_peer_discovery_httpc, put,
            fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
              ?assertEqual("http", Scheme),
              ?assertEqual("consul.service.consul", Host),
              ?assertEqual(8500, Port),
              ?assertEqual("v1/agent/service/register", Path),
              ?assertEqual([], Args),
              ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
              Expect = <<"{\"ID\":\"rabbitmq:172.16.4.50\",\"Name\":\"rabbitmq\",\"Address\":\"172.16.4.50\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\",\"Status\":\"passing\"}}">>,
              ?assertEqual(Expect, Body),
              {ok, []}
            end),
          os:putenv("CONSUL_HOST", "consul.service.consul"),
          os:putenv("CONSUL_PORT", "8500"),
          os:putenv("CONSUL_ACL_TOKEN", "token-value"),
           os:putenv("CONSUL_SVC_ADDR_AUTO", "false"),
          os:putenv("CONSUL_SVC_ADDR_NIC", "en0"),
          ?assertEqual(ok, rabbit_peer_discovery_consul:register()),
          ?assert(meck:validate(rabbit_peer_discovery_httpc)),
          ?assert(meck:validate(rabbit_peer_discovery_util)).

registration_generic_error_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(_Scheme, _Host, _Port, _Path, _Args, _Headers, _Body) ->
             {error, "testing"}
           end),
         ?assertEqual({error, "testing"}, rabbit_peer_discovery_consul:register()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

health_check_with_all_defaults_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("http", Scheme),
             ?assertEqual("localhost", Host),
             ?assertEqual(8500, Port),
             ?assertEqual("v1/agent/check/pass/service:rabbitmq", Path),
             ?assertEqual([], Args),
             ?assertEqual([], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         ?assertEqual(ok, rabbit_peer_discovery_consul:send_health_check_pass()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

health_check_without_acl_token_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("https", Scheme),
             ?assertEqual("consul.service.consul", Host),
             ?assertEqual(8501, Port),
             ?assertEqual("v1/agent/check/pass/service:rabbit", Path),
             ?assertEqual([], Args),
             ?assertEqual([], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         os:putenv("CONSUL_SCHEME", "https"),
         os:putenv("CONSUL_HOST", "consul.service.consul"),
         os:putenv("CONSUL_PORT", "8501"),
         os:putenv("CONSUL_SVC", "rabbit"),
         ?assertEqual(ok, rabbit_peer_discovery_consul:send_health_check_pass()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

health_check_with_acl_token_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("http", Scheme),
             ?assertEqual("consul.service.consul", Host),
             ?assertEqual(8500, Port),
             ?assertEqual("v1/agent/check/pass/service:rabbitmq", Path),
             ?assertEqual([], Args),
             ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         os:putenv("CONSUL_HOST", "consul.service.consul"),
         os:putenv("CONSUL_PORT", "8500"),
         os:putenv("CONSUL_ACL_TOKEN", "token-value"),
         ?assertEqual(ok, rabbit_peer_discovery_consul:send_health_check_pass()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

health_check_error_handling_test(_Config) ->
         meck:expect(rabbit_log, error, fun(_Message, _Args) ->
           ok
         end),
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(_Scheme, _Host, _Port, _Path, _Args, _Headers, _HttpOpts) ->
             {error, "testing"}
           end),
         ?assertEqual(ok, rabbit_peer_discovery_consul:send_health_check_pass()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)),
         ?assert(meck:validate(rabbit_log)).


unregistration_with_all_defaults_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("http", Scheme),
             ?assertEqual("localhost", Host),
             ?assertEqual(8500, Port),
             ?assertEqual("v1/agent/service/deregister/rabbitmq", Path),
             ?assertEqual([], Args),
             ?assertEqual([], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         ?assertEqual(ok, rabbit_peer_discovery_consul:unregister()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).


unregistration_without_acl_token_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("https", Scheme),
             ?assertEqual("consul.service.consul", Host),
             ?assertEqual(8501, Port),
             ?assertEqual("v1/agent/service/deregister/rabbit:10.0.0.1", Path),
             ?assertEqual([], Args),
             ?assertEqual([], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         os:putenv("CONSUL_SCHEME", "https"),
         os:putenv("CONSUL_HOST", "consul.service.consul"),
         os:putenv("CONSUL_PORT", "8501"),
         os:putenv("CONSUL_SVC", "rabbit"),
         os:putenv("CONSUL_SVC_ADDR", "10.0.0.1"),
         os:putenv("CONSUL_SVC_PORT", "5671"),
         ?assertEqual(ok, rabbit_peer_discovery_consul:unregister()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

unregistration_with_acl_token_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
             ?assertEqual("http", Scheme),
             ?assertEqual("consul.service.consul", Host),
             ?assertEqual(8500, Port),
             ?assertEqual("v1/agent/service/deregister/rabbitmq", Path),
             ?assertEqual([], Args),
             ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
             ?assertEqual([], HttpOpts),
             {ok, []}
           end),
         os:putenv("CONSUL_HOST", "consul.service.consul"),
         os:putenv("CONSUL_PORT", "8500"),
         os:putenv("CONSUL_ACL_TOKEN", "token-value"),
         ?assertEqual(ok, rabbit_peer_discovery_consul:unregister()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

unregistration_with_generic_error_test(_Config) ->
         meck:expect(rabbit_peer_discovery_httpc, put,
           fun(_Scheme, _Host, _Port, _Path, _Args, _Headers, _HttpOpts) ->
             {error, "testing"}
           end),
         ?assertEqual({error, "testing"}, rabbit_peer_discovery_consul:unregister()),
         ?assert(meck:validate(rabbit_peer_discovery_httpc)).

startup_lock_path_default_value_test(_Config) ->
    Expectation = "rabbitmq/default/startup_lock",
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:startup_lock_path()).

startup_lock_path_with_prefix_test(_Config) ->
    Expectation = "myprefix/default/startup_lock",
    os:putenv("CONSUL_LOCK_PREFIX", "myprefix"),
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:startup_lock_path()).

startup_lock_path_with_cluster_name_test(_Config) ->
    os:putenv("CLUSTER_NAME", "mycluster"),
    Expectation = "rabbitmq/mycluster/startup_lock",
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:startup_lock_path()).

create_session_without_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/session/create", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        Expect = <<"{\"Name\":\"node-name\",\"TTL\":\"30s\"}">>,
                        ?assertEqual(Expect, Body),
                        {ok, #{<<"ID">> => <<"session-id">>}}
                end),
    ?assertEqual({ok, "session-id"}, rabbit_peer_discovery_consul:create_session('node-name', 30)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

create_session_with_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/session/create", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
                        Expect = <<"{\"Name\":\"node-name\",\"TTL\":\"30s\"}">>,
                        ?assertEqual(Expect, Body),
                        {ok, #{<<"ID">> => <<"session-id">>}}
                end),
    os:putenv("CONSUL_ACL_TOKEN", "token-value"),
    ?assertEqual({ok, "session-id"}, rabbit_peer_discovery_consul:create_session('node-name', 30)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

get_lock_status_without_session_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, [#{<<"LockIndex">> => 3,
                                <<"Key">> => <<"rabbitmq/default/startup_lock">>,
                                <<"Flags">> => 0,
                                <<"Value">> => <<"W3t9XQ==">>,
                                <<"Session">> => <<"session-id">>,
                                <<"CreateIndex">> => 8,
                                <<"ModifyIndex">> => 21}]}
                end),
    ?assertEqual({ok, {true, 21}}, rabbit_peer_discovery_consul:get_lock_status()),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

get_lock_status_with_session_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, [#{<<"LockIndex">> => 3,
                                <<"Key">> => <<"rabbitmq/default/startup_lock">>,
                                <<"Flags">> => 0,
                                <<"Value">> => <<"W3t9XQ==">>,
                                <<"CreateIndex">> => 8,
                                <<"ModifyIndex">> => 21}]}
                end),
    ?assertEqual({ok, {false, 21}}, rabbit_peer_discovery_consul:get_lock_status()),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

get_lock_status_with_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, [#{<<"LockIndex">> => 3,
                                <<"Key">> => <<"rabbitmq/default/startup_lock">>,
                                <<"Flags">> => 0,
                                <<"Value">> => <<"W3t9XQ==">>,
                                <<"CreateIndex">> => 8,
                                <<"ModifyIndex">> => 21}]}
                end),
    os:putenv("CONSUL_ACL_TOKEN", "token-value"),
    ?assertEqual({ok, {false, 21}}, rabbit_peer_discovery_consul:get_lock_status()),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

wait_for_lock_release_with_session_without_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{index, 42}, {wait, "300s"}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, []}
                end),
    ?assertEqual(ok, rabbit_peer_discovery_consul:wait_for_lock_release(true, 42, 300)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

wait_for_lock_release_with_session_with_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{index, 42}, {wait, "300s"}], Args),
                        ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, []}
                end),
    os:putenv("CONSUL_ACL_TOKEN", "token-value"),
    ?assertEqual(ok, rabbit_peer_discovery_consul:wait_for_lock_release(true, 42, 300)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

wait_for_lock_release_without_session_test(_Config) ->
    ?assertEqual(ok, rabbit_peer_discovery_consul:wait_for_lock_release(false, 0, 0)).

acquire_lock_successfully_acquired_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, true}
                end),
    ?assertEqual({ok, true}, rabbit_peer_discovery_consul:acquire_lock(session_id)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

acquire_lock_not_acquired_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, false}
                end),
    ?assertEqual({ok, false}, rabbit_peer_discovery_consul:acquire_lock(session_id)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

acquire_lock_with_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
                        ?assertEqual([], Body),
                        {ok, true}
                end),
    os:putenv("CONSUL_ACL_TOKEN", "token-value"),
    ?assertEqual({ok, true}, rabbit_peer_discovery_consul:acquire_lock(session_id)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

release_lock_successfully_released_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{release, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, true}
                end),
    ?assertEqual({ok, true}, rabbit_peer_discovery_consul:release_lock(session_id)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

release_lock_not_released_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{release, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, false}
                end),
    ?assertEqual({ok, false}, rabbit_peer_discovery_consul:release_lock(session_id)),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

release_lock_with_token_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/rabbitmq/default/startup_lock", Path),
                        ?assertEqual([{release, session_id}], Args),
                        ?assertEqual([{"X-Consul-Token", "token-value"}], Headers),
                        ?assertEqual([], Body),
                        {ok, true}
                end),
    os:putenv("CONSUL_ACL_TOKEN", "token-value"),
    ?assertEqual({ok, true}, rabbit_peer_discovery_consul:release_lock(session_id)).

consul_kv_read_default_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/path/to/key", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, []}
                end),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_kv_read("path/to/key", [{acquire, session_id}], rabbit_peer_discovery_consul:maybe_add_acl([]))),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_kv_read_custom_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, get,
                fun(Scheme, Host, Port, Path, Args, Headers, HttpOpts) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("consul.node.consul", Host),
                        ?assertEqual(8501, Port),
                        ?assertEqual("v1/kv/path/to/key", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], HttpOpts),
                        {ok, []}
            end),
    os:putenv("CONSUL_HOST", "consul.node.consul"),
    os:putenv("CONSUL_PORT", "8501"),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_kv_read("path/to/key", [{acquire, session_id}], rabbit_peer_discovery_consul:maybe_add_acl([]))),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_kv_write_default_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/kv/path/to/key", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
                end),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_kv_write("path/to/key", [{acquire, session_id}], rabbit_peer_discovery_consul:maybe_add_acl([]), [])),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_kv_write_custom_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("consul.node.consul", Host),
                        ?assertEqual(8501, Port),
                        ?assertEqual("v1/kv/path/to/key", Path),
                        ?assertEqual([{acquire, session_id}], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
                end),
    os:putenv("CONSUL_HOST", "consul.node.consul"),
    os:putenv("CONSUL_PORT", "8501"),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_kv_write("path/to/key", [{acquire, session_id}], rabbit_peer_discovery_consul:maybe_add_acl([]), [])),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_session_create_default_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/session/create", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
                end),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_session_create([], rabbit_peer_discovery_consul:maybe_add_acl([]), [])),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_session_create_custom_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("consul.node.consul", Host),
                        ?assertEqual(8501, Port),
                        ?assertEqual("v1/session/create", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
            end),
    os:putenv("CONSUL_HOST", "consul.node.consul"),
    os:putenv("CONSUL_PORT", "8501"),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_session_create([], rabbit_peer_discovery_consul:maybe_add_acl([]), [])),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_session_renew_default_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("localhost", Host),
                        ?assertEqual(8500, Port),
                        ?assertEqual("v1/session/renew/session_id", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
                end),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_session_renew("session_id", [], rabbit_peer_discovery_consul:maybe_add_acl([]))),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

consul_session_renew_custom_values_test(_Config) ->
    meck:expect(rabbit_peer_discovery_httpc, put,
                fun(Scheme, Host, Port, Path, Args, Headers, Body) ->
                        ?assertEqual("http", Scheme),
                        ?assertEqual("consul.node.consul", Host),
                        ?assertEqual(8501, Port),
                        ?assertEqual("v1/session/renew/session_id", Path),
                        ?assertEqual([], Args),
                        ?assertEqual([], Headers),
                        ?assertEqual([], Body),
                        {ok, []}
                end),
    os:putenv("CONSUL_HOST", "consul.node.consul"),
    os:putenv("CONSUL_PORT", "8501"),
    ?assertEqual({ok, []}, rabbit_peer_discovery_consul:consul_session_renew("session_id", [], rabbit_peer_discovery_consul:maybe_add_acl([]))),
    ?assert(meck:validate(rabbit_peer_discovery_httpc)).

%%%
%%% Implementation
%%%

list_of_nodes_with_warnings() ->
    <<"[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"warning\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"critical\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]">>.

list_of_nodes_without_warnings() ->
    <<"[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]">>.

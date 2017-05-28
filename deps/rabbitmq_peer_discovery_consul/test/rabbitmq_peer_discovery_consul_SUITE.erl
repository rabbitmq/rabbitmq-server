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
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2017 Pivotal Software, Inc. All rights reserved.
%%

-module(rabbitmq_peer_discovery_consul_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    [
     {group, registration_body_tests}
     %% , {group, registration_tests}
     %% , {group, other_tests}
    ].

groups() ->
    [
     {registration_body_tests, [], [
                 registration_body_simple_case,
                 registration_body_svc_addr_set_via_env_var,
                 registration_body_svc_ttl_set_via_env_var,
                 registration_body_deregister_after_set_via_env_var,
                 registration_body_ttl_and_deregister_after_both_unset_via_env_var,
                 registration_body_ttl_unset_and_deregister_after_set_via_env_var,
                 service_id_all_defaults_test,
                 service_id_with_unset_address_test,
                 service_ttl_default_test
                ]}
     %% , {registration_tests, [], [
     %%             %% registeration_test,
     %%             %% registeration_failure_test,
     %%             %% unregisteration_test
     %%            ]},
     %% , {other_tests, [], [
     %%             %% nodelist_test,
     %%             %% send_health_check_pass_test,
     %%            ]}
    ].


init_per_group(_, Config) -> Config.
end_per_group(_, Config)  -> Config.

reset() ->
    application:unset_env(rabbit, autocluster),
    os:unsetenv("CONSUL_SVC"),
    os:unsetenv("CONSUL_SVC_ADDR"),
    os:unsetenv("CONSUL_SVC_ADDR_AUTO"),
    os:unsetenv("CONSUL_SVC_ADDR_NIC"),
    os:unsetenv("CONSUL_SVC_ADDR_NODENAME"),
    os:unsetenv("CONSUL_SVC_TTL"),
    os:unsetenv("CONSUL_DEREGISTER_AFTER").

init_per_testcase(_TC, Config) ->
    reset(),
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
                          {'TTL',   '30s'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_svc_addr_set_via_env_var(_Config) ->
    os:putenv("CONSUL_SVC_ADDR", "mercurio"),
    Expectation = [{'ID', 'rabbitmq:mercurio'},
                   {'Name', rabbitmq},
                   {'Address', mercurio},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL',   '30s'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_svc_ttl_set_via_env_var(_Config) ->
    os:putenv("CONSUL_SVC_TTL", "257"),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL',   '257s'}]}],
    ?assertEqual(Expectation, rabbit_peer_discovery_consul:build_registration_body()).

registration_body_deregister_after_set_via_env_var(_Config) ->
    os:putenv("CONSUL_DEREGISTER_AFTER", "520"),
    Expectation = [{'ID', 'rabbitmq'},
                   {'Name', rabbitmq},
                   {'Port', 5672},
                   {'Check',
                    [{'Notes', ?CONSUL_CHECK_NOTES},
                     {'TTL','30s'},
                     {'Deregister_critical_service_after','520s'}]}],
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


%% nodelist_test() ->
%%   {
%%     foreach,
%%     fun() ->
%%       autocluster_testing:reset(),
%%       meck:new(autocluster_httpc, []),
%%       [autocluster_httpc]
%%     end,
%%     fun autocluster_testing:on_finish/1,
%%     [
%%       {"default values",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, health, service, "rabbitmq"], Path),
%%               ?assertEqual([passing], Args),
%%               {error, "testing"}
%%             end),
%%           ?assertEqual({error, "testing"}, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"without token",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("https", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8501, Port),
%%               ?assertEqual([v1, health, service, "rabbit"], Path),
%%               ?assertEqual([passing], Args),
%%               {error, "testing"}
%%             end),
%%           os:putenv("CONSUL_SCHEME", "https"),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8501"),
%%           os:putenv("CONSUL_SVC", "rabbit"),
%%           ?assertEqual({error, "testing"}, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with token",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, health, service, "rabbitmq"], Path),
%%               ?assertEqual([passing, {token, "token-value"}], Args),
%%               {error, "testing"}
%%             end),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8500"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           ?assertEqual({error, "testing"}, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with cluster name",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, health, service, "rabbitmq"], Path),
%%               ?assertEqual([passing, {tag,"bob"}], Args),
%%               {error, "testing"}
%%             end),
%%           os:putenv("CLUSTER_NAME", "bob"),
%%           ?assertEqual({error, "testing"}, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with cluster name and token",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, health, service, "rabbitmq"], Path),
%%               ?assertEqual([passing, {tag,"bob"}, {token, "token-value"}], Args),
%%               {error, "testing"}
%%             end),
%%           os:putenv("CLUSTER_NAME", "bob"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           ?assertEqual({error, "testing"}, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, _) ->
%%               Body = "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
%%               rabbit_misc:json_decode(Body)
%%             end),
%%           Expectation = {ok,['rabbit@rabbit1', 'rabbit@rabbit2']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result with consul long name",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, _) ->
%%               Body = "[{\"Node\": {\"Node\": \"rabbit2\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
%%               rabbit_misc:json_decode(Body)
%%             end),
%%           os:putenv("CONSUL_USE_LONGNAME", "true"),
%%           Expectation = {ok,['rabbit@rabbit1.node.consul', 'rabbit@rabbit2.node.consul']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result with long name and custom domain",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, _) ->
%%               Body = "[{\"Node\": {\"Node\": \"rabbit2\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit2\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"\", \"Port\": 5672, \"ID\": \"rabbitmq\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
%%               rabbit_misc:json_decode(Body)
%%             end),
%%           os:putenv("CONSUL_USE_LONGNAME", "true"),
%%           os:putenv("CONSUL_DOMAIN", "mydomain"),
%%           Expectation = {ok,['rabbit@rabbit1.node.mydomain', 'rabbit@rabbit2.node.mydomain']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result with srv address",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, _) ->
%%               Body = "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]",
%%               rabbit_misc:json_decode(Body)
%%             end),
%%           Expectation = {ok,['rabbit@172.16.4.51','rabbit@172.172.16.51']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result with warnings allowed",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, []) ->
%%                     rabbit_misc:json_decode(with_warnings());
%%                (_, _, _, _, [passing]) ->
%%                     rabbit_misc:json_decode(without_warnings())
%%             end),
%%           os:putenv("CONSUL_INCLUDE_NODES_WITH_WARNINGS", "true"),
%%           Expectation = {ok,['rabbit@172.16.4.51']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"return result with no warnings allowed",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(_, _, _, _, []) ->
%%                     rabbit_misc:json_decode(with_warnings());
%%                (_, _, _, _, [passing]) ->
%%                     rabbit_misc:json_decode(without_warnings())
%%             end),
%%           Expectation = {ok,['rabbit@172.16.4.51', 'rabbit@172.172.16.51']},
%%           ?assertEqual(Expectation, autocluster_consul:nodelist()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end}
%%     ]
%%   }.


%% registeration_test() ->
%%   {
%%     foreach,
%%     fun() ->
%%       autocluster_testing:reset(),
%%       meck:new(autocluster_httpc, []),
%%       meck:new(autocluster_util, [passthrough]),
%%       meck:new(timer, [unstick, passthrough]),
%%       meck:new(autocluster_log, []),
%%       [autocluster_httpc, autocluster_util, timer, autocluster_log]
%%     end,
%%     fun autocluster_testing:on_finish/1,
%%     [
%%       {"default values",
%%         fun() ->
%%           meck:expect(autocluster_log, debug, fun(_Message) -> ok end),
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq\",\"Name\":\"rabbitmq\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           meck:expect(timer, apply_interval, fun(Interval, M, F, A) ->
%%             ?assertEqual(Interval, 15000),
%%             ?assertEqual(M, autocluster_consul),
%%             ?assertEqual(F, send_health_check_pass),
%%             ?assertEqual(A, []),
%%             {ok, "started"}
%%           end),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_log)),
%%           ?assert(meck:validate(timer)),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with cluster",
%%         fun() ->
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq\",\"Name\":\"rabbitmq\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"},\"Tags\":[\"test-rabbit\"]}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CLUSTER_NAME", "test-rabbit"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"without token",  fun() ->
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("https", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8501, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([], Args),
%%               Expect = <<"{\"ID\":\"rabbit:10.0.0.1\",\"Name\":\"rabbit\",\"Address\":\"10.0.0.1\",\"Port\":5671,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_SCHEME", "https"),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8501"),
%%           os:putenv("CONSUL_SVC", "rabbit"),
%%           os:putenv("CONSUL_SVC_ADDR", "10.0.0.1"),
%%           os:putenv("CONSUL_SVC_PORT", "5671"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with token",
%%         fun() ->
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([{token, "token-value"}], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq\",\"Name\":\"rabbitmq\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8500"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with auto addr",
%%         fun() ->
%%           meck:expect(autocluster_util, node_hostname, fun(true) -> "bob.consul.node";
%%                                                           (false) -> "bob" end),
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([{token, "token-value"}], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq:bob\",\"Name\":\"rabbitmq\",\"Address\":\"bob\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8500"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           os:putenv("CONSUL_SVC_ADDR_AUTO", "true"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc)),
%%           ?assert(meck:validate(autocluster_util))
%%         end},
%%       {"with auto addr from nodename",
%%         fun() ->
%%           meck:expect(autocluster_util, node_hostname, fun(true) -> "bob.consul.node";
%%                                                           (false) -> "bob" end),
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([{token, "token-value"}], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq:bob.consul.node\",\"Name\":\"rabbitmq\",\"Address\":\"bob.consul.node\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8500"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           os:putenv("CONSUL_SVC_ADDR_AUTO", "true"),
%%           os:putenv("CONSUL_SVC_ADDR_NODENAME", "true"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc)),
%%           ?assert(meck:validate(autocluster_util))
%%         end},
%%       {"with auto addr nic",
%%         fun() ->
%%           meck:expect(autocluster_util, nic_ipv4,
%%             fun(NIC) ->
%%               ?assertEqual("en0", NIC),
%%               {ok, "172.16.4.50"}
%%             end),
%%           meck:expect(autocluster_httpc, post,
%%             fun(Scheme, Host, Port, Path, Args, Body) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, service, register], Path),
%%               ?assertEqual([{token, "token-value"}], Args),
%%               Expect = <<"{\"ID\":\"rabbitmq:172.16.4.50\",\"Name\":\"rabbitmq\",\"Address\":\"172.16.4.50\",\"Port\":5672,\"Check\":{\"Notes\":\"RabbitMQ Consul-based peer discovery plugin TTL check\",\"TTL\":\"30s\"}}">>,
%%               ?assertEqual(Expect, Body),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8500"),
%%           os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%           os:putenv("CONSUL_SVC_ADDR_NIC", "en0"),
%%           ?assertEqual(ok, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc)),
%%           ?assert(meck:validate(autocluster_util))
%%         end
%%       },
%%       {"ttl disabled - periodic health check not started", fun() ->
%%         meck:expect(timer, apply_interval, fun(_, _, _, _) ->
%%           {error, "should not be called"}
%%          end),
%%         meck:expect(autocluster_httpc, post, fun (_, _, _, _, _, _) -> {ok, []} end),
%%         os:putenv("CONSUL_SVC_TTL", ""),
%%         ?assertEqual(ok, autocluster_consul:register()),
%%         ?assert(meck:validate(timer))
%%        end}
%%     ]
%%   }.

%% registeration_failure_test() ->
%%   {
%%     foreach,
%%     fun autocluster_testing:on_start/0,
%%     fun autocluster_testing:on_finish/1,
%%     [
%%       {"on error",
%%         fun() ->
%%           meck:new(autocluster_httpc),
%%           meck:expect(autocluster_httpc, post,
%%             fun(_Scheme, _Host, _Port, _Path, _Args, _body) ->
%%               {error, "testing"}
%%             end),
%%           ?assertEqual({error, "testing"}, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_httpc)),
%%           meck:unload(autocluster_httpc)
%%         end
%%       },
%%       {"on json encode error",
%%         fun() ->
%%           meck:new(autocluster_consul, [passthrough]),
%%           meck:new(autocluster_log, [passthrough]),
%%           meck:expect(autocluster_consul, build_registration_body, 0, {foo}),
%%           meck:expect(autocluster_log, error, 2, ok),
%%           ?assertEqual({error,{bad_term,{foo}}}, autocluster_consul:register()),
%%           ?assert(meck:validate(autocluster_consul)),
%%           meck:unload(autocluster_consul),
%%           meck:unload(autocluster_log)
%%         end
%%       }
%%     ]
%%   }.


%% send_health_check_pass_test() ->
%%   {
%%     foreach,
%%     fun() ->
%%       autocluster_testing:reset(),
%%       meck:new(autocluster_httpc, []),
%%       meck:new(autocluster_log, []),
%%       [autocluster_httpc, autocluster_log]
%%     end,
%%     fun autocluster_testing:on_finish/1,
%%     [
%%       {"default values",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("http", Scheme),
%%               ?assertEqual("localhost", Host),
%%               ?assertEqual(8500, Port),
%%               ?assertEqual([v1, agent, check, pass, "service:rabbitmq"], Path),
%%               ?assertEqual([], Args),
%%               {ok, []}
%%             end),
%%           ?assertEqual(ok, autocluster_consul:send_health_check_pass()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"without token",
%%         fun() ->
%%           meck:expect(autocluster_httpc, get,
%%             fun(Scheme, Host, Port, Path, Args) ->
%%               ?assertEqual("https", Scheme),
%%               ?assertEqual("consul.service.consul", Host),
%%               ?assertEqual(8501, Port),
%%               ?assertEqual([v1, agent, check, pass, "service:rabbit"], Path),
%%               ?assertEqual([], Args),
%%               {ok, []}
%%             end),
%%           os:putenv("CONSUL_SCHEME", "https"),
%%           os:putenv("CONSUL_HOST", "consul.service.consul"),
%%           os:putenv("CONSUL_PORT", "8501"),
%%           os:putenv("CONSUL_SVC", "rabbit"),
%%           ?assertEqual(ok, autocluster_consul:send_health_check_pass()),
%%           ?assert(meck:validate(autocluster_httpc))
%%         end},
%%       {"with token", fun() ->
%%         meck:expect(autocluster_httpc, get,
%%           fun(Scheme, Host, Port, Path, Args) ->
%%             ?assertEqual("http", Scheme),
%%             ?assertEqual("consul.service.consul", Host),
%%             ?assertEqual(8500, Port),
%%             ?assertEqual([v1, agent, check, pass, "service:rabbitmq"], Path),
%%             ?assertEqual([{token, "token-value"}], Args),
%%             {ok, []}
%%           end),
%%         os:putenv("CONSUL_HOST", "consul.service.consul"),
%%         os:putenv("CONSUL_PORT", "8500"),
%%         os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%         ?assertEqual(ok, autocluster_consul:send_health_check_pass()),
%%         ?assert(meck:validate(autocluster_httpc))
%%        end},
%%       {"on error", fun() ->
%%         meck:expect(autocluster_log, error, fun(_Message, _Args) ->
%%           ok
%%           end),
%%         meck:expect(autocluster_httpc, get,
%%           fun(_Scheme, _Host, _Port, _Path, _Args) ->
%%             {error, "testing"}
%%           end),
%%         ?assertEqual(ok, autocluster_consul:send_health_check_pass()),
%%         ?assert(meck:validate(autocluster_httpc)),
%%         ?assert(meck:validate(autocluster_log))
%%        end}
%%     ]
%%   }.


%% unregisteration_test() ->
%%   {
%%     foreach,
%%     fun() ->
%%       autocluster_testing:reset(),
%%       meck:new(autocluster_httpc, []),
%%       [autocluster_httpc]
%%     end,
%%     fun autocluster_testing:on_finish/1,
%%     [
%%       {"default values", fun() ->
%%         meck:expect(autocluster_httpc, get,
%%           fun(Scheme, Host, Port, Path, Args) ->
%%             ?assertEqual("http", Scheme),
%%             ?assertEqual("localhost", Host),
%%             ?assertEqual(8500, Port),
%%             ?assertEqual([v1, agent, service, deregister, "rabbitmq"], Path),
%%             ?assertEqual([], Args),
%%             {ok, []}
%%           end),
%%         ?assertEqual(ok, autocluster_consul:unregister()),
%%         ?assert(meck:validate(autocluster_httpc))
%%                          end},
%%       {"without token", fun() ->
%%         meck:expect(autocluster_httpc, get,
%%           fun(Scheme, Host, Port, Path, Args) ->
%%             ?assertEqual("https", Scheme),
%%             ?assertEqual("consul.service.consul", Host),
%%             ?assertEqual(8501, Port),
%%             ?assertEqual([v1, agent, service, deregister,"rabbit:10.0.0.1"], Path),
%%             ?assertEqual([], Args),
%%             {ok, []}
%%           end),
%%         os:putenv("CONSUL_SCHEME", "https"),
%%         os:putenv("CONSUL_HOST", "consul.service.consul"),
%%         os:putenv("CONSUL_PORT", "8501"),
%%         os:putenv("CONSUL_SVC", "rabbit"),
%%         os:putenv("CONSUL_SVC_ADDR", "10.0.0.1"),
%%         os:putenv("CONSUL_SVC_PORT", "5671"),
%%         ?assertEqual(ok, autocluster_consul:unregister()),
%%         ?assert(meck:validate(autocluster_httpc))
%%        end},
%%       {"with token", fun() ->
%%         meck:expect(autocluster_httpc, get,
%%           fun(Scheme, Host, Port, Path, Args) ->
%%             ?assertEqual("http", Scheme),
%%             ?assertEqual("consul.service.consul", Host),
%%             ?assertEqual(8500, Port),
%%             ?assertEqual([v1, agent, service, deregister,"rabbitmq"], Path),
%%             ?assertEqual([{token, "token-value"}], Args),
%%             {ok, []}
%%           end),
%%         os:putenv("CONSUL_HOST", "consul.service.consul"),
%%         os:putenv("CONSUL_PORT", "8500"),
%%         os:putenv("CONSUL_ACL_TOKEN", "token-value"),
%%         ?assertEqual(ok, autocluster_consul:unregister()),
%%         ?assert(meck:validate(autocluster_httpc))
%%        end},
%%       {"on error", fun() ->
%%         meck:expect(autocluster_httpc, get,
%%           fun(_Scheme, _Host, _Port, _Path, _Args) ->
%%             {error, "testing"}
%%           end),
%%         ?assertEqual({error, "testing"}, autocluster_consul:unregister()),
%%         ?assert(meck:validate(autocluster_httpc))
%%        end}
%%     ]
%%   }.

%%%
%%% Implementation
%%%

with_warnings() ->
    "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"warning\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"critical\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]".

without_warnings() ->
    "[{\"Node\": {\"Node\": \"rabbit2.internal.domain\", \"Address\": \"10.20.16.160\"}, \"Checks\": [{\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq:172.172.16.4.50\", \"Output\": \"\"}, {\"Node\": \"rabbit2.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.16.4.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.16.4.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}, {\"Node\": {\"Node\": \"rabbit1.internal.domain\", \"Address\": \"10.20.16.159\"}, \"Checks\": [{\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"service:rabbitmq\", \"Name\": \"Service \'rabbitmq\' check\", \"ServiceName\": \"rabbitmq\", \"Notes\": \"Connect to the port internally every 30 seconds\", \"Status\": \"passing\", \"ServiceID\": \"rabbitmq\", \"Output\": \"\"}, {\"Node\": \"rabbit1.internal.domain\", \"CheckID\": \"serfHealth\", \"Name\": \"Serf Health Status\", \"ServiceName\": \"\", \"Notes\": \"\", \"Status\": \"passing\", \"ServiceID\": \"\", \"Output\": \"Agent alive and reachable\"}], \"Service\": {\"Address\": \"172.172.16.51\", \"Port\": 5672, \"ID\": \"rabbitmq:172.172.16.51\", \"Service\": \"rabbitmq\", \"Tags\": [\"amqp\"]}}]".

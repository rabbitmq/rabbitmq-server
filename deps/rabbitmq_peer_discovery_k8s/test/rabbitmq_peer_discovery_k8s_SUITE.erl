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

-module(rabbitmq_peer_discovery_k8s_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    [
     {group, unit}
    ].

groups() ->
    [
     {unit, [], [
                 extract_node_list_long_test,
                 extract_node_list_short_test,
                 extract_node_list_hostname_short_test,
                 extract_node_list_real_test,
                 extract_node_list_with_not_ready_addresses_test,
                 node_name_empty_test,
                 node_name_suffix_test
                ]}].

init_per_testcase(T, Config) when T == node_name_empty_test;
                                  T == node_name_suffix_test ->
    meck:new(net_kernel, [passthrough, unstick]),
    meck:expect(net_kernel, longnames, fun() -> true end),
    Config;
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    meck:unload(),
    application:unset_env(rabbit, cluster_formation),
    [os:unsetenv(Var) || Var <- ["K8S_HOSTNAME_SUFFIX",
                                 "K8S_ADDRESS_TYPE"]].

%%%
%%% Testcases
%%%

extract_node_list_long_test(_Config) ->
    {ok, Response} =
	rabbit_json:try_decode(
          rabbit_data_coercion:to_binary(
            "{\"name\": \"mysvc\",\n\"subsets\": [\n{\n\"addresses\": [{\"ip\": \"10.10.1.1\"}, {\"ip\": \"10.10.2.2\"}],\n\"ports\": [{\"name\": \"a\", \"port\": 8675}, {\"name\": \"b\", \"port\": 309}]\n},\n{\n\"addresses\": [{\"ip\": \"10.10.3.3\"}],\n\"ports\": [{\"name\": \"a\", \"port\": 93},{\"name\": \"b\", \"port\": 76}]\n}]}")),
    Expectation = [<<"10.10.1.1">>, <<"10.10.2.2">>, <<"10.10.3.3">>],
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:extract_node_list(Response)).

extract_node_list_short_test(_Config) ->
    {ok, Response} =
	rabbit_json:try_decode(
          rabbit_data_coercion:to_binary(
            "{\"name\": \"mysvc\",\n\"subsets\": [\n{\n\"addresses\": [{\"ip\": \"10.10.1.1\"}, {\"ip\": \"10.10.2.2\"}],\n\"ports\": [{\"name\": \"a\", \"port\": 8675}, {\"name\": \"b\", \"port\": 309}]\n}]}")),
    Expectation = [<<"10.10.1.1">>, <<"10.10.2.2">>],
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:extract_node_list(Response)).

extract_node_list_hostname_short_test(_Config) ->
    os:putenv("K8S_ADDRESS_TYPE", "hostname"),
    {ok, Response} =
	rabbit_json:try_decode(
          rabbit_data_coercion:to_binary(
            "{\"name\": \"mysvc\",\n\"subsets\": [\n{\n\"addresses\": [{\"ip\": \"10.10.1.1\", \"hostname\": \"rabbitmq-1\"}, {\"ip\": \"10.10.2.2\", \"hostname\": \"rabbitmq-2\"}],\n\"ports\": [{\"name\": \"a\", \"port\": 8675}, {\"name\": \"b\", \"port\": 309}]\n}]}")),
    Expectation = [<<"rabbitmq-1">>, <<"rabbitmq-2">>],
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:extract_node_list(Response)).

extract_node_list_real_test(_Config) ->
    {ok, Response} =
	rabbit_json:try_decode(
          rabbit_data_coercion:to_binary(
            "{\"kind\":\"Endpoints\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"galera\",\"namespace\":\"default\",\"selfLink\":\"/api/v1/namespaces/default/endpoints/galera\",\"uid\":\"646f8305-3491-11e6-8c20-ecf4bbd91e6c\",\"resourceVersion\":\"17373568\",\"creationTimestamp\":\"2016-06-17T13:42:54Z\",\"labels\":{\"app\":\"mysqla\"}},\"subsets\":[{\"addresses\":[{\"ip\":\"10.1.29.8\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"default\",\"name\":\"mariadb-tco7k\",\"uid\":\"fb59cc71-558c-11e6-86e9-ecf4bbd91e6c\",\"resourceVersion\":\"13034802\"}},{\"ip\":\"10.1.47.2\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"default\",\"name\":\"mariadb-izgp8\",\"uid\":\"fb484ab3-558c-11e6-86e9-ecf4bbd91e6c\",\"resourceVersion\":\"13035747\"}},{\"ip\":\"10.1.47.3\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"default\",\"name\":\"mariadb-init-ffrsz\",\"uid\":\"fb12e1d3-558c-11e6-86e9-ecf4bbd91e6c\",\"resourceVersion\":\"13032722\"}},{\"ip\":\"10.1.94.2\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"default\",\"name\":\"mariadb-zcc0o\",\"uid\":\"fb31ce6e-558c-11e6-86e9-ecf4bbd91e6c\",\"resourceVersion\":\"13034771\"}}],\"ports\":[{\"name\":\"mysql\",\"port\":3306,\"protocol\":\"TCP\"}]}]}")),
    Expectation = [<<"10.1.94.2">>, <<"10.1.47.3">>, <<"10.1.47.2">>,
                   <<"10.1.29.8">>],
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:extract_node_list(Response)).

extract_node_list_with_not_ready_addresses_test(_Config) ->
    {ok, Response}  =
	rabbit_json:try_decode(
          rabbit_data_coercion:to_binary(
            "{\"kind\":\"Endpoints\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"rabbitmq\",\"namespace\":\"test-rabbitmq\",\"selfLink\":\"\/api\/v1\/namespaces\/test-rabbitmq\/endpoints\/rabbitmq\",\"uid\":\"4ff733b8-3ad2-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170098\",\"creationTimestamp\":\"2017-05-17T07:27:41Z\",\"labels\":{\"app\":\"rabbitmq\",\"type\":\"LoadBalancer\"}},\"subsets\":[{\"notReadyAddresses\":[{\"ip\":\"172.17.0.2\",\"hostname\":\"rabbitmq-0\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-0\",\"uid\":\"e980fe5a-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170044\"}},{\"ip\":\"172.17.0.4\",\"hostname\":\"rabbitmq-1\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-1\",\"uid\":\"f6285603-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170071\"}},{\"ip\":\"172.17.0.5\",\"hostname\":\"rabbitmq-2\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-2\",\"uid\":\"fd5a86dc-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170096\"}}],\"ports\":[{\"name\":\"amqp\",\"port\":5672,\"protocol\":\"TCP\"},{\"name\":\"http\",\"port\":15672,\"protocol\":\"TCP\"}]}]}")),
    Expectation = [],
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:extract_node_list(Response)).

node_name_empty_test(_Config) ->
    Expectation = 'rabbit@rabbitmq-0',
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:node_name(<<"rabbitmq-0">>)).

node_name_suffix_test(_Config) ->
    os:putenv("K8S_HOSTNAME_SUFFIX", ".rabbitmq.default.svc.cluster.local"),
    Expectation = 'rabbit@rabbitmq-0.rabbitmq.default.svc.cluster.local',
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:node_name(<<"rabbitmq-0">>)).

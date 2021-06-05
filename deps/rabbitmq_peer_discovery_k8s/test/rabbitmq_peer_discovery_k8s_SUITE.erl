%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(rabbitmq_peer_discovery_k8s_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


%% rabbitmq/cluster-operator contains an implicit integration test
%% for the rabbitmq_peer_discovery_k8s plugin added by
%% https://github.com/rabbitmq/cluster-operator/pull/704

all() ->
    [
     {group, unit},
     {group, lock}
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
                 node_name_suffix_test,
                 registration_support,
                 event_v1_test
                ]},
     {lock, [], [
                 lock_single_node,
                 lock_multiple_nodes,
                 lock_local_node_not_discovered,
                 lock_list_nodes_fails
                ]}
    ].

init_per_testcase(T, Config) when T == node_name_empty_test;
                                  T == node_name_suffix_test ->
    meck:new(net_kernel, [passthrough, unstick]),
    meck:expect(net_kernel, longnames, fun() -> true end),
    Config;
init_per_testcase(_, Config) -> Config.

end_per_testcase(_, _Config) ->
    meck:unload(),
    application:unset_env(rabbit, cluster_formation),
    [os:unsetenv(Var) || Var <- ["K8S_HOSTNAME_SUFFIX",
                                 "K8S_ADDRESS_TYPE"]].

%%%
%%% Testcases
%%%

registration_support(_Config) ->
    ?assertEqual(true, rabbit_peer_discovery_k8s:supports_registration()).

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
            "{\"kind\":\"Endpoints\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"rabbitmq\",\"namespace\":\"test-rabbitmq\",\"selfLink\":\"\/api\/v1\/namespaces\/test-rabbitmq\/endpoints\/rabbitmq\",\"uid\":\"4ff733b8-3ad2-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170098\",\"creationTimestamp\":\"2017-05-17T07:27:41Z\",\"labels\":{\"app\":\"rabbitmq\",\"type\":\"LoadBalancer\"}},\"subsets\":[{\"addresses\":[{\"ip\":\"10.1.29.8\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"default\",\"name\":\"mariadb-tco7k\",\"uid\":\"fb59cc71-558c-11e6-86e9-ecf4bbd91e6c\",\"resourceVersion\":\"13034802\"}}],\"ports\":[{\"name\":\"mysql\",\"port\":3306,\"protocol\":\"TCP\"}]},{\"notReadyAddresses\":[{\"ip\":\"172.17.0.2\",\"hostname\":\"rabbitmq-0\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-0\",\"uid\":\"e980fe5a-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170044\"}},{\"ip\":\"172.17.0.4\",\"hostname\":\"rabbitmq-1\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-1\",\"uid\":\"f6285603-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170071\"}},{\"ip\":\"172.17.0.5\",\"hostname\":\"rabbitmq-2\",\"nodeName\":\"minikube\",\"targetRef\":{\"kind\":\"Pod\",\"namespace\":\"test-rabbitmq\",\"name\":\"rabbitmq-2\",\"uid\":\"fd5a86dc-3afd-11e7-a40d-080027cbdcae\",\"resourceVersion\":\"170096\"}}],\"ports\":[{\"name\":\"amqp\",\"port\":5672,\"protocol\":\"TCP\"},{\"name\":\"http\",\"port\":15672,\"protocol\":\"TCP\"}]}]}")),
    Expectation = [<<"10.1.29.8">>,
                   <<"172.17.0.2">>, <<"172.17.0.4">>, <<"172.17.0.5">>],
    ?assertEqual(Expectation, lists:sort(rabbit_peer_discovery_k8s:extract_node_list(Response))).

node_name_empty_test(_Config) ->
    Expectation = 'rabbit@rabbitmq-0',
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:node_name(<<"rabbitmq-0">>)).

node_name_suffix_test(_Config) ->
    os:putenv("K8S_HOSTNAME_SUFFIX", ".rabbitmq.default.svc.cluster.local"),
    Expectation = 'rabbit@rabbitmq-0.rabbitmq.default.svc.cluster.local',
    ?assertEqual(Expectation, rabbit_peer_discovery_k8s:node_name(<<"rabbitmq-0">>)).

event_v1_test(_Config) ->
    Expectation = #{
		    count => 1,
		    type => <<"Normal">>,
		    lastTimestamp => <<"2019-12-06T15:10:23+00:00">>,
		    reason => <<"Reason">>,
		    message => <<"MyMessage">>,
		    metadata =>#{
				 name => <<"test">> ,
				 namespace => <<"namespace">>
				},
		    involvedObject =>#{
				       apiVersion => <<"v1">>,
				       kind => <<"RabbitMQ">>,
				       name => <<"pod/MyHostName">>,
				       namespace => <<"namespace">>
				      },
		    source =>#{
			       component => <<"MyHostName/rabbitmq_peer_discovery">>,
			       host => <<"MyHostName">>
			      }
		   },
    ?assertEqual(Expectation,
		 rabbit_peer_discovery_k8s:generate_v1_event(<<"namespace">>, "test",
							     "Normal", "Reason", "MyMessage", "2019-12-06T15:10:23+00:00", "MyHostName")).

lock_single_node(_Config) ->
  LocalNode = node(),
  meck:expect(rabbit_peer_discovery_k8s, list_nodes, 0, {ok, {[LocalNode], disc}}),

  {ok, LockId} = rabbit_peer_discovery_k8s:lock(LocalNode),
  ?assertEqual(ok, rabbit_peer_discovery_k8s:unlock(LockId)).

lock_multiple_nodes(_Config) ->
  application:set_env(rabbit, cluster_formation, [{internal_lock_retries, 2}]),
  LocalNode = node(),
  OtherNode = other@host,
  meck:expect(rabbit_peer_discovery_k8s, list_nodes, 0, {ok, {[OtherNode, LocalNode], disc}}),

  {ok, {LockResourceId, OtherNode}} = rabbit_peer_discovery_k8s:lock(OtherNode),
  ?assertEqual({error, "Acquiring lock taking too long, bailing out after 2 retries"}, rabbit_peer_discovery_k8s:lock(LocalNode)),
  ?assertEqual(ok, rabbitmq_peer_discovery_k8s:unlock({LockResourceId, OtherNode})),
  ?assertEqual({ok, {LockResourceId, LocalNode}}, rabbit_peer_discovery_k8s:lock(LocalNode)),
  ?assertEqual(ok, rabbitmq_peer_discovery_k8s:unlock({LockResourceId, LocalNode})).

lock_local_node_not_discovered(_Config) ->
  meck:expect(rabbit_peer_discovery_k8s, list_nodes, 0, {ok, {[n1@host, n2@host], disc}} ),
  Expectation = {error, "Local node me@host is not part of discovered nodes [n1@host,n2@host]"},
  ?assertEqual(Expectation, rabbit_peer_discovery_k8s:lock(me@host)).

lock_list_nodes_fails(_Config) ->
  meck:expect(rabbit_peer_discovery_k8s, list_nodes, 0, {error, "K8s API unavailable"}),
  ?assertEqual({error, "K8s API unavailable"}, rabbit_peer_discovery_k8s:lock(me@host)).

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_SUITE).

-compile(export_all).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").


all() ->
    [
     {group, unit},
     {group, lock}
    ].

groups() ->
    [
     {unit, [], [
                 maybe_add_tag_filters,
                 get_hostname_name_from_reservation_set,
                 registration_support
                ]},
     {lock, [], [
                 lock_single_node,
                 lock_multiple_nodes,
                 lock_local_node_not_discovered
                ]}
    ].

%%%
%%% Testcases
%%%

maybe_add_tag_filters(_Config) ->
    Tags = maps:from_list([{"region", "us-west-2"}, {"service", "rabbitmq"}]),
    Expectation = lists:sort(
                    [{"Filter.2.Name", "tag:service"},
                     {"Filter.2.Value.1", "rabbitmq"},
                     {"Filter.1.Name", "tag:region"},
                     {"Filter.1.Value.1", "us-west-2"}]),
    Result = lists:sort(rabbit_peer_discovery_aws:maybe_add_tag_filters(Tags, [], 1)),
    ?assertEqual(Expectation, Result).

get_hostname_name_from_reservation_set(_Config) ->
    ok = eunit:test({
      foreach,
      fun on_start/0,
      fun on_finish/1,
      [{"from private DNS",
        fun() ->
                Expectation = ["ip-10-0-16-29.eu-west-1.compute.internal",
                               "ip-10-0-16-31.eu-west-1.compute.internal"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end},
       {"from arbitrary path",
        fun() ->
                os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,1,association,publicDnsName"),
                Expectation = ["ec2-203-0-113-11.eu-west-1.compute.amazonaws.com",
                               "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end},
       {"from private IP",
        fun() ->
                os:putenv("AWS_USE_PRIVATE_IP", "true"),
                Expectation = ["10.0.16.29", "10.0.16.31"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end}]
    }).

registration_support(_Config) ->
    ?assertEqual(false, rabbit_peer_discovery_aws:supports_registration()).

lock_single_node(_Config) ->
  LocalNode = node(),
  Nodes = [LocalNode],

  {ok, {LockId, Nodes}} = rabbit_peer_discovery_aws:lock([LocalNode]),
  ?assertEqual(ok, rabbit_peer_discovery_aws:unlock({LockId, Nodes})).

lock_multiple_nodes(_Config) ->
  application:set_env(rabbit, cluster_formation, [{internal_lock_retries, 2}]),
  LocalNode = node(),
  OtherNodeA = a@host,
  OtherNodeB = b@host,

  meck:expect(rabbit_nodes, lock_id, 1, {rabbit_nodes:cookie_hash(), OtherNodeA}),
  {ok, {{LockResourceId, OtherNodeA}, [LocalNode, OtherNodeA]}} = rabbit_peer_discovery_aws:lock([LocalNode, OtherNodeA]),
  meck:expect(rabbit_nodes, lock_id, 1, {rabbit_nodes:cookie_hash(), OtherNodeB}),
  ?assertEqual({error, "Acquiring lock taking too long, bailing out after 2 retries"}, rabbit_peer_discovery_aws:lock([LocalNode, OtherNodeB])),
  ?assertEqual(ok, rabbit_peer_discovery_aws:unlock({{LockResourceId, OtherNodeA}, [LocalNode, OtherNodeA]})),
  ?assertEqual({ok, {{LockResourceId, OtherNodeB}, [LocalNode, OtherNodeB]}}, rabbit_peer_discovery_aws:lock([LocalNode, OtherNodeB])),
  ?assertEqual(ok, rabbit_peer_discovery_aws:unlock({{LockResourceId, OtherNodeB}, [LocalNode, OtherNodeB]})),
  meck:unload(rabbit_nodes).

lock_local_node_not_discovered(_Config) ->
  Expectation = {error, "Local node " ++ atom_to_list(node()) ++ " is not part of discovered nodes [me@host]"},
  ?assertEqual(Expectation, rabbit_peer_discovery_aws:lock([me@host])).

%%%
%%% Implementation
%%%

on_start() ->
    reset().

on_finish(_Config) ->
    reset().

reset() ->
    application:unset_env(rabbit, cluster_formation),
    os:unsetenv("AWS_HOSTNAME_PATH"),
    os:unsetenv("AWS_USE_PRIVATE_IP").

reservation_set() ->
    [{"item", [{"reservationId","r-006cfdbf8d04c5f01"},
               {"ownerId","248536293561"},
               {"groupSet",[]},
               {"instancesSet",
                [{"item",
                  [{"instanceId","i-0c6d048641f09cad2"},
                   {"imageId","ami-ef4c7989"},
                   {"instanceState",
                    [{"code","16"},{"name","running"}]},
                   {"privateDnsName",
                    "ip-10-0-16-29.eu-west-1.compute.internal"},
                   {"dnsName",[]},
                   {"instanceType","c4.large"},
                   {"launchTime","2017-04-07T12:05:10"},
                   {"subnetId","subnet-61ff660"},
                   {"vpcId","vpc-4fe1562b"},
                   {"networkInterfaceSet", [
                    {"item", 
                    [{"association",
                     [{"publicIp","203.0.113.11"},
                      {"publicDnsName",
                       "ec2-203-0-113-11.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]},
                    {"item", 
                    [{"association",
                     [{"publicIp","203.0.113.12"},
                      {"publicDnsName",
                       "ec2-203-0-113-12.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]}]},
                   {"privateIpAddress","10.0.16.29"}]}]}]},
     {"item", [{"reservationId","r-006cfdbf8d04c5f01"},
               {"ownerId","248536293561"},
               {"groupSet",[]},
               {"instancesSet",
                [{"item",
                  [{"instanceId","i-1c6d048641f09cad2"},
                   {"imageId","ami-af4c7989"},
                   {"instanceState",
                    [{"code","16"},{"name","running"}]},
                   {"privateDnsName",
                    "ip-10-0-16-31.eu-west-1.compute.internal"},
                   {"dnsName",[]},
                   {"instanceType","c4.large"},
                   {"launchTime","2017-04-07T12:05:10"},
                   {"subnetId","subnet-61ff660"},
                   {"vpcId","vpc-4fe1562b"},
                   {"networkInterfaceSet", [
                    {"item", 
                    [{"association",
                     [{"publicIp","203.0.113.21"},
                      {"publicDnsName",
                       "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]},
                    {"item", 
                    [{"association",
                     [{"publicIp","203.0.113.22"},
                      {"publicDnsName",
                       "ec2-203-0-113-22.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]}]},
                   {"privateIpAddress","10.0.16.31"}]}]}]}].

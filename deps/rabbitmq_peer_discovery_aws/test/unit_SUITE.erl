%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
                 lock_local_node_not_discovered,
                 lock_list_nodes_fails
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
    {
      foreach,
      fun on_start/0,
      fun on_finish/1,
      [{"from private DNS",
        fun() ->
                Expectation = ["ip-10-0-16-31.eu-west-1.compute.internal",
                               "ip-10-0-16-29.eu-west-1.compute.internal"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end},
       {"from private IP",
        fun() ->
                os:putenv("AWS_USE_PRIVATE_IP", "true"),
                Expectation = ["10.0.16.31", "10.0.16.29"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end}]
    }.

registration_support(_Config) ->
    ?assertEqual(false, rabbit_peer_discovery_aws:supports_registration()).

lock_single_node(_Config) ->
  LocalNode = node(),
  Nodes = [LocalNode],
  meck:expect(rabbit_peer_discovery_aws, list_nodes, 0, {ok, {Nodes, disc}}),

  {ok, {LockId, Nodes}} = rabbit_peer_discovery_aws:lock(LocalNode),
  ?assertEqual(ok, rabbit_peer_discovery_aws:unlock({LockId, Nodes})).

lock_multiple_nodes(_Config) ->
  application:set_env(rabbit, cluster_formation, [{internal_lock_retries, 2}]),
  LocalNode = node(),
  OtherNode = other@host,
  Nodes = [OtherNode, LocalNode],
  meck:expect(rabbit_peer_discovery_aws, list_nodes, 0, {ok, {Nodes, disc}}),

  {ok, {{LockResourceId, OtherNode}, Nodes}} = rabbit_peer_discovery_aws:lock(OtherNode),
  ?assertEqual({error, "Acquiring lock taking too long, bailing out after 2 retries"},
               rabbit_peer_discovery_aws:lock(LocalNode)),
  ?assertEqual(ok, rabbitmq_peer_discovery_aws:unlock({{LockResourceId, OtherNode}, Nodes})),

  ?assertEqual({ok, {{LockResourceId, LocalNode}, Nodes}}, rabbit_peer_discovery_aws:lock(LocalNode)),
  ?assertEqual(ok, rabbitmq_peer_discovery_aws:unlock({{LockResourceId, LocalNode}, Nodes})).

lock_local_node_not_discovered(_Config) ->
  meck:expect(rabbit_peer_discovery_aws, list_nodes, 0, {ok, {[n1@host, n2@host], disc}} ),
  Expectation = {error, "Local node me@host is not part of discovered nodes [n1@host,n2@host]"},
  ?assertEqual(Expectation, rabbit_peer_discovery_aws:lock(me@host)).

lock_list_nodes_fails(_Config) ->
  meck:expect(rabbit_peer_discovery_aws, list_nodes, 0, {error, "failed for some reason"}),
  ?assertEqual({error, "failed for some reason"}, rabbit_peer_discovery_aws:lock(me@host)).

%%%
%%% Implementation
%%%

on_start() ->
    reset().

on_finish(_Config) ->
    reset().

reset() ->
    application:unset_env(rabbit, cluster_formation),
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
                   {"privateIpAddress","10.0.16.31"}]}]}]}].

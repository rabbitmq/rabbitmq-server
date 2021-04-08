%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% The Initial Developer of the Original Code is AWeber Communications.
%% Copyright (c) 2015-2016 AWeber Communications
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates. All rights reserved.
%%

-module(unit_SUITE).

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
                 maybe_add_tag_filters,
                 get_hostname_name_from_reservation_set,
                 registration_support
                ]}].

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
    ?assertEqual(rabbit_peer_discovery_aws:supports_registration(), true).

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

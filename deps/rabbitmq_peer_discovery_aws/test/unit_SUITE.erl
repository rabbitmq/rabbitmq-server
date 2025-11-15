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
                 registration_support,
                 network_interface_sorting,
                 private_ip_address_sorting
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
    ?assertMatch(Expectation, Result).

registration_support(_Config) ->
    ?assertEqual(false, rabbit_peer_discovery_aws:supports_registration()).

network_interface_sorting(_Config) ->
    %% Test ENI sorting by deviceIndex (DescribeInstances only returns attached ENIs)
    NetworkInterfaces = [
        {"item", [
            {"networkInterfaceId", "eni-secondary"},
            {"attachment", [{"deviceIndex", "1"}]}
        ]},
        {"item", [
            {"networkInterfaceId", "eni-primary"},
            {"attachment", [{"deviceIndex", "0"}]}
        ]},
        {"item", [
            {"networkInterfaceId", "eni-tertiary"},
            {"attachment", [{"deviceIndex", "2"}]}
        ]}
    ],
    
    %% Should sort ENIs by deviceIndex
    Sorted = rabbit_peer_discovery_aws:sort_ec2_hostname_path_set_members("networkInterfaceSet", NetworkInterfaces),
    
    %% Should have all 3 ENIs
    ?assertEqual(3, length(Sorted)),
    
    %% Primary ENI (deviceIndex=0) should be first
    {"item", FirstENI} = lists:nth(1, Sorted),
    ?assertEqual("eni-primary", proplists:get_value("networkInterfaceId", FirstENI)),
    
    %% Secondary ENI (deviceIndex=1) should be second
    {"item", SecondENI} = lists:nth(2, Sorted),
    ?assertEqual("eni-secondary", proplists:get_value("networkInterfaceId", SecondENI)),
    
    %% Tertiary ENI (deviceIndex=2) should be third
    {"item", ThirdENI} = lists:nth(3, Sorted),
    ?assertEqual("eni-tertiary", proplists:get_value("networkInterfaceId", ThirdENI)).

private_ip_address_sorting(_Config) ->
    %% Test private IP address sorting by primary flag
    PrivateIpAddresses = [
        {"item", [
            {"privateIpAddress", "10.0.14.176"},
            {"privateDnsName", "ip-10-0-14-176.us-west-2.compute.internal"},
            {"primary", "false"}
        ]},
        {"item", [
            {"privateIpAddress", "10.0.12.112"},
            {"privateDnsName", "ip-10-0-12-112.us-west-2.compute.internal"},
            {"primary", "true"}
        ]},
        {"item", [
            {"privateIpAddress", "10.0.15.200"},
            {"privateDnsName", "ip-10-0-15-200.us-west-2.compute.internal"},
            {"primary", "false"}
        ]}
    ],
    
    Sorted = rabbit_peer_discovery_aws:sort_ec2_hostname_path_set_members("privateIpAddressesSet", PrivateIpAddresses),
    ?assertEqual(3, length(Sorted)),
    
    %% Primary IP (primary=true) should be first
    {"item", FirstIP} = lists:nth(1, Sorted),
    ?assertEqual("10.0.12.112", proplists:get_value("privateIpAddress", FirstIP)),
    ?assertEqual("true", proplists:get_value("primary", FirstIP)),
    
    %% Non-primary IPs should maintain relative order
    {"item", SecondIP} = lists:nth(2, Sorted),
    ?assertEqual("10.0.14.176", proplists:get_value("privateIpAddress", SecondIP)),
    ?assertEqual("false", proplists:get_value("primary", SecondIP)),
    
    {"item", ThirdIP} = lists:nth(3, Sorted),
    ?assertEqual("10.0.15.200", proplists:get_value("privateIpAddress", ThirdIP)),
    ?assertEqual("false", proplists:get_value("primary", ThirdIP)).

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

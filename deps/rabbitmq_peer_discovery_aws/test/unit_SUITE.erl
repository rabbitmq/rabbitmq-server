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
                 registration_support,
                 network_interface_sorting,
                 private_ip_address_sorting,
                 multiple_hostname_paths_extraction,
                 hostname_paths_helper_functions
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
        end},
       {"from private IP DNS in network interface",
        fun() ->
                os:putenv("AWS_HOSTNAME_PATH", "networkInterfaceSet,2,privateIpAddressesSet,1,privateDnsName"),
                Expectation = ["ip-10-0-15-100.eu-west-1.compute.internal",
                               "ip-10-0-16-31.eu-west-1.compute.internal"],
                ?assertEqual(Expectation,
                             rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                               reservation_set(), []))
        end}]
    }).

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

multiple_hostname_paths_extraction(_Config) ->
    ok = eunit:test({
      foreach,
      fun on_start/0,
      fun on_finish/1,
      [{"multiple paths extraction",
        fun() ->
                %% Set up multiple hostname paths via application config
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}
                    ]}
                ]),
                
                %% Test that multiple hostnames are extracted
                Expected = ["ip-10-0-16-29.eu-west-1.compute.internal", "10.0.16.29",
                           "ip-10-0-16-31.eu-west-1.compute.internal", "10.0.16.31"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"single path fallback",
        fun() ->
                %% Test fallback to single path when no multiple paths configured
                %% This should use the default single path behavior
                Expected = ["ip-10-0-16-29.eu-west-1.compute.internal",
                           "ip-10-0-16-31.eu-west-1.compute.internal"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"use_private_ip override with multiple paths",
        fun() ->
                %% Test that use_private_ip still works with multiple paths
                os:putenv("AWS_USE_PRIVATE_IP", "true"),
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]},
                        {aws_use_private_ip, true}
                    ]}
                ]),
                
                %% Should get private IPs for privateDnsName path (overridden), plus privateIpAddress path
                %% After deduplication, should have unique IPs only
                Expected = ["10.0.16.29", "10.0.16.31"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"complex path extraction",
        fun() ->
                %% Test extraction from network interface paths
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_paths, [["networkInterfaceSet", 1, "association", "publicDnsName"],
                                             ["privateDnsName"]]}
                    ]}
                ]),
                
                %% Should extract from both paths
                Expected = ["ec2-203-0-113-11.eu-west-1.compute.amazonaws.com",
                           "ip-10-0-16-29.eu-west-1.compute.internal",
                           "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com", 
                           "ip-10-0-16-31.eu-west-1.compute.internal"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"precedence: hostname_paths takes precedence over hostname_path",
        fun() ->
                %% Test that aws_hostname_paths takes precedence over aws_hostname_path
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_path, ["privateIpAddress"]},  % This should be ignored
                        {aws_hostname_paths, [["privateDnsName"]]}  % This should be used
                    ]}
                ]),
                
                %% Should use hostname_paths (privateDnsName), not hostname_path (privateIpAddress)
                Expected = ["ip-10-0-16-29.eu-west-1.compute.internal",
                           "ip-10-0-16-31.eu-west-1.compute.internal"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"precedence: empty hostname_paths falls back to hostname_path",
        fun() ->
                %% Test that empty aws_hostname_paths falls back to aws_hostname_path
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_path, ["privateIpAddress"]},  % This should be used
                        {aws_hostname_paths, []}                    % Empty, should fall back
                    ]}
                ]),
                
                %% Should fall back to hostname_path (privateIpAddress)
                Expected = ["10.0.16.29", "10.0.16.31"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"precedence: use_private_ip with both hostname_path and hostname_paths",
        fun() ->
                %% Test use_private_ip override when both configurations exist
                os:putenv("AWS_USE_PRIVATE_IP", "true"),
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_path, ["privateDnsName"]},                    % Should be ignored
                        {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}, % Should be used
                        {aws_use_private_ip, true}
                    ]}
                ]),
                
                %% Should use hostname_paths with use_private_ip override applied
                %% privateDnsName -> privateIpAddress, privateIpAddress stays the same
                %% After deduplication: only unique IPs
                Expected = ["10.0.16.29", "10.0.16.31"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end},
       {"precedence: use_private_ip with fallback to hostname_path",
        fun() ->
                %% Test use_private_ip override when falling back to single hostname_path
                os:putenv("AWS_USE_PRIVATE_IP", "true"),
                application:set_env(rabbit, cluster_formation, [
                    {peer_discovery_aws, [
                        {aws_hostname_path, ["privateDnsName"]},    % Should be used with override
                        {aws_hostname_paths, []},                   % Empty, falls back
                        {aws_use_private_ip, true}
                    ]}
                ]),
                
                %% Should fall back to hostname_path with use_private_ip override
                %% privateDnsName -> privateIpAddress
                Expected = ["10.0.16.29", "10.0.16.31"],
                Result = rabbit_peer_discovery_aws:get_hostname_name_from_reservation_set(
                           reservation_set(), []),
                ?assertEqual(Expected, Result)
        end}]
    }).

hostname_paths_helper_functions(_Config) ->
    %% Test extract_unique_hostnames function
    Paths = [["privateDnsName"], ["privateIpAddress"]],
    %% Extract items correctly from reservation set structure - reservation_set() returns a list
    ReservationSet = reservation_set(),
    [{"item", FirstReservation}] = lists:sublist(ReservationSet, 1),
    InstancesSet = proplists:get_value("instancesSet", FirstReservation),
    Items = [Item || {"item", Item} <- InstancesSet],
    
    Hostnames = rabbit_peer_discovery_aws:extract_unique_hostnames(Paths, Items),
    ?assertEqual(2, length(Hostnames)), % Should extract 2 unique hostnames for the first instance
    
    %% Test with duplicates - should be automatically removed
    DuplicatePaths = [["privateDnsName"], ["privateDnsName"], ["privateIpAddress"]],
    UniqueHostnames = rabbit_peer_discovery_aws:extract_unique_hostnames(DuplicatePaths, Items),
    ?assertEqual(2, length(UniqueHostnames)), % Should still be 2 unique hostnames
    
    %% Test empty path handling - should return empty string gracefully
    EmptyPath = [],
    EmptyResult = rabbit_peer_discovery_aws:get_hostname(EmptyPath, [{"test", "value"}]),
    ?assertEqual("", EmptyResult),
    
    %% Test invalid path handling - should return empty string gracefully
    InvalidPath = [invalid_atom],
    InvalidResult = rabbit_peer_discovery_aws:get_hostname(InvalidPath, [{"test", "value"}]),
    ?assertEqual("", InvalidResult),
    
    %% Test get_hostname_paths function behavior
    %% Test default behavior (no configuration)
    DefaultPaths = rabbit_peer_discovery_aws:get_hostname_paths(),
    ?assertEqual([["privateDnsName"]], DefaultPaths),
    
    %% Test with multiple paths configured
    application:set_env(rabbit, cluster_formation, [
        {peer_discovery_aws, [
            {aws_hostname_paths, [["privateDnsName"], ["privateIpAddress"]]}
        ]}
    ]),
    MultiplePaths = rabbit_peer_discovery_aws:get_hostname_paths(),
    ?assertEqual([["privateDnsName"], ["privateIpAddress"]], MultiplePaths).

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
    os:unsetenv("AWS_HOSTNAME_PATHS"),
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
                    [{"attachment", [{"deviceIndex", "1"}]},
                     {"association",
                     [{"publicIp","203.0.113.12"},
                      {"publicDnsName",
                       "ec2-203-0-113-12.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]},
                     {"privateIpAddressesSet", [
                        {"item", [
                            {"privateIpAddress", "10.0.15.101"},
                            {"privateDnsName", "ip-10-0-15-101.eu-west-1.compute.internal"},
                            {"primary", "false"}
                        ]},
                        {"item", [
                            {"privateIpAddress", "10.0.15.100"},
                            {"privateDnsName", "ip-10-0-15-100.eu-west-1.compute.internal"},
                            {"primary", "true"}
                        ]}
                     ]}]},
                    {"item",
                    [{"attachment", [{"deviceIndex", "0"}]},
                     {"association",
                     [{"publicIp","203.0.113.11"},
                      {"publicDnsName",
                       "ec2-203-0-113-11.eu-west-1.compute.amazonaws.com"},
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
                    [{"attachment", [{"deviceIndex", "0"}]},
                     {"association",
                     [{"publicIp","203.0.113.21"},
                      {"publicDnsName",
                       "ec2-203-0-113-21.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]}]},
                    {"item",
                    [{"attachment", [{"deviceIndex", "1"}]},
                     {"association",
                     [{"publicIp","203.0.113.22"},
                      {"publicDnsName",
                       "ec2-203-0-113-22.eu-west-1.compute.amazonaws.com"},
                      {"ipOwnerId","amazon"}]},
                     {"privateIpAddressesSet", [
                        {"item", [
                            {"privateIpAddress", "10.0.16.31"},
                            {"privateDnsName", "ip-10-0-16-31.eu-west-1.compute.internal"},
                            {"primary", "true"}
                        ]}
                     ]}]}]},
                   {"privateIpAddress","10.0.16.31"}]}]}]}].

%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
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
                 maybe_add_instance_state_filters,
                 validate_instance_states,
                 get_hostname_name_from_reservation_set,
                 registration_support,
                 network_interface_sorting,
                 private_ip_address_sorting,
                 get_hostname_by_instance_ids_with_state_filter,
                 get_hostname_by_tags_with_state_filter
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

maybe_add_instance_state_filters(_Config) ->
    application:set_env(rabbit, cluster_formation,
                       [{peer_discovery_aws, [{aws_ec2_instance_states, ["running", "pending"]}]}]),
    QArgs = [{"Action", "DescribeInstances"}, {"Version", "2015-10-01"}],
    Result = rabbit_peer_discovery_aws:maybe_add_instance_state_filters(QArgs, 1),
    Expectation = [{"Filter.1.Name", "instance-state-name"},
                   {"Filter.1.Value.2", "pending"},
                   {"Filter.1.Value.1", "running"},
                   {"Action", "DescribeInstances"},
                   {"Version", "2015-10-01"}],
    ?assertEqual(Expectation, Result),
    application:unset_env(rabbit, cluster_formation).

validate_instance_states(_Config) ->
    ValidStates = ["pending", "running", "shutting-down", "terminated", "stopping", "stopped"],
    ?assertEqual(ValidStates, rabbit_peer_discovery_aws:validate_instance_states(ValidStates)),
    ?assertEqual(["running"], rabbit_peer_discovery_aws:validate_instance_states(["running", "invalid"])),
    ?assertEqual([], rabbit_peer_discovery_aws:validate_instance_states(["bogus", "invalid"])),
    ?assertEqual(["running", "pending"], rabbit_peer_discovery_aws:validate_instance_states([running, pending])),
    ?assertEqual(["running"], rabbit_peer_discovery_aws:validate_instance_states([running, invalid])).

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

get_hostname_by_instance_ids_with_state_filter(_Config) ->
    application:set_env(rabbit, cluster_formation,
                       [{peer_discovery_aws, [{aws_ec2_instance_states, ["running", "pending"]}]}]),
    meck:new(rabbitmq_aws, [passthrough]),
    meck:expect(rabbitmq_aws, api_get_request,
        fun("ec2", Path) ->
            ?assert(string:str(Path, "Filter") > 0),
            ?assert(string:str(Path, "instance-state-name") > 0),
            ?assert(string:str(Path, "Value.1=running") > 0),
            ?assert(string:str(Path, "Value.2=pending") > 0),
            {ok, mock_describe_instances_response()}
        end),
    Result = rabbit_peer_discovery_aws:get_hostname_by_instance_ids(
        ["i-abc123", "i-def456"], #{}),
    ?assertEqual(["ip-10-0-16-29.eu-west-1.compute.internal",
                  "ip-10-0-16-31.eu-west-1.compute.internal"], Result),
    meck:unload(rabbitmq_aws),
    application:unset_env(rabbit, cluster_formation).

get_hostname_by_tags_with_state_filter(_Config) ->
    application:set_env(rabbit, cluster_formation,
                       [{peer_discovery_aws, [{aws_ec2_instance_states, ["running"]}]}]),
    meck:new(rabbitmq_aws, [passthrough]),
    meck:expect(rabbitmq_aws, api_get_request,
        fun("ec2", Path) ->
            ?assert(string:str(Path, "Filter") > 0),
            ?assert(string:str(Path, "instance-state-name") > 0),
            ?assert(string:str(Path, "Value.1=running") > 0),
            ?assert(string:str(Path, "tag%3Aservice") > 0),
            {ok, mock_describe_instances_response()}
        end),
    Tags = maps:from_list([{"service", "rabbitmq"}]),
    Result = rabbit_peer_discovery_aws:get_hostname_by_tags(Tags),
    ?assertEqual(["ip-10-0-16-29.eu-west-1.compute.internal",
                  "ip-10-0-16-31.eu-west-1.compute.internal"], Result),
    meck:unload(rabbitmq_aws),
    application:unset_env(rabbit, cluster_formation).

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

mock_describe_instances_response() ->
    [{"DescribeInstancesResponse",
      [{"reservationSet", reservation_set()}]}].
